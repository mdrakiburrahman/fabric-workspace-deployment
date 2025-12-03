# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import json
import logging

import dacite
import requests

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.fabric.cli import FabricCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    CommonParams,
    HttpRetryHandler,
    KqlDatabaseInfo,
    MwcScopedToken,
    ShortcutManager,
    ShortcutParams,
    WorkspaceManager,
)
from fabric_workspace_deployment.static.transformers import StringTransformer


class FabricShortcutManager(ShortcutManager):
    """Concrete implementation of ShortcutManager for Microsoft Fabric."""

    def __init__(
        self,
        common_params: CommonParams,
        az_cli: AzCli,
        fabric_cli: FabricCli,
        workspace: WorkspaceManager,
        http_retry_handler: HttpRetryHandler,
    ):
        """
        Initialize the Fabric Shortcut manager.
        """
        super().__init__(common_params)
        self.az_cli = az_cli
        self.fabric_cli = fabric_cli
        self.workspace = workspace
        self.logger = logging.getLogger(__name__)
        self.http_retry = http_retry_handler

    async def execute(self) -> None:
        self.logger.info("Executing FabricShortcutManager")
        tasks = []
        for workspace_params in self.common_params.fabric.workspaces:
            if workspace_params.skip_deploy:
                self.logger.info(f"Skipping shortcuts for workspace '{workspace_params.name}' due to skipDeploy=true")
                continue
            workspace_info = await self.workspace.get(workspace_params)
            task = asyncio.create_task(self.reconcile(workspace_info.id, workspace_params.shortcut), name=f"reconcile-shortcut-{workspace_params.name}")
            tasks.append(task)

        if tasks:
            self.logger.info(f"Executing Shortcut reconciliation for {len(tasks)} workspaces in parallel")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    workspace_name = self.common_params.fabric.workspaces[i].name
                    error_msg = f"Failed to reconcile Shortcut for workspace '{workspace_name}': {result}"
                    self.logger.error(error_msg)
                    errors.append(error_msg)

            if errors:
                error = f"Failed to reconcile Shortcut for some workspaces: {'; '.join(errors)}"
                raise Exception(error)
        else:
            self.logger.info("No workspaces found to reconcile Shortcut")

        self.logger.info("Finished executing FabricShortcutManager")

    async def reconcile(self, workspace_id: str, shortcut_params: ShortcutParams) -> None:
        """
        Reconcile all shortcut for a single workspace.
        """
        existing_kql_dbs = await self.get_all_kusto_databases(workspace_id)
        database_name_to_id = {db.display_name: db.id for db in existing_kql_dbs}
        self.logger.info(f"Found {len(database_name_to_id)} existing KQL databases: {list(database_name_to_id.keys())}")
        database_shortcuts: dict[str, list] = {}
        for kql_db in shortcut_params.kql_database:
            if kql_db.database not in database_shortcuts:
                database_shortcuts[kql_db.database] = []
            database_shortcuts[kql_db.database].append(kql_db)

        for database_name, shortcuts in database_shortcuts.items():
            existing_db_id = database_name_to_id.get(database_name)
            if existing_db_id:
                self.logger.info(f"Found existing KQL database '{database_name}' with ID {existing_db_id} in workspace {workspace_id}")
                db_shortcut_params = ShortcutParams(kql_database=shortcuts)
                await self.batch_create_kusto_database_shortcut(workspace_id, existing_db_id, db_shortcut_params)
            else:
                self.logger.warning(f"KQL database '{database_name}' not found in workspace {workspace_id}")

    async def get_all_kusto_databases(self, workspace_id: str) -> list[KqlDatabaseInfo]:
        self.logger.info(f"Getting all KQL databases for workspace: {workspace_id}")

        try:
            output = self.fabric_cli.run_command(f"api -X get workspaces/{workspace_id}/KQLDatabases")
            response_data = json.loads(output.strip())
            databases_data = response_data.get("text", {}).get("value", [])
            databases_snake_case = [StringTransformer.convert_keys_to_snake_case(db) for db in databases_data]
            kql_databases = []
            for db_data in databases_snake_case:
                try:
                    kql_db = dacite.from_dict(
                        data_class=KqlDatabaseInfo,
                        data=db_data,
                        config=dacite.Config(
                            check_types=False,
                            cast=[str, int, bool, float],
                        ),
                    )
                    kql_databases.append(kql_db)
                except Exception as e:
                    self.logger.error(f"Failed to parse KQL database data with dacite: {e}")
                    self.logger.error(f"Raw database data: {db_data}")
                    raise

            self.logger.info(f"Successfully retrieved {len(kql_databases)} KQL databases for workspace {workspace_id}")
            return kql_databases

        except Exception as e:
            error_msg = f"Failed to get KQL databases for workspace '{workspace_id}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def get_kusto_database_mwc_token(self, workspace_id: str, database_id: str) -> MwcScopedToken:
        """
        Get MWC scoped token for a KQL database.

        Args:
            workspace_id: The workspace ID containing the database
            database_id: The KQL database ID

        Returns:
            MwcScopedToken: MWC scoped token information
        """
        self.logger.info(f"Getting MWC token for database {database_id} in workspace {workspace_id}")
        try:
            response = self.http_retry.execute(
                requests.post,
                f"{self.common_params.endpoint.analysis_service}/metadata/v201606/generatemwctokenv2",
                headers={
                    "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                    "Content-Type": "application/json",
                },
                json={
                    "workloadType": "Kusto",
                    "workspaceObjectId": workspace_id,
                    "artifacts": [{"artifactType": "KustoDatabase", "artifactObjectId": database_id}],
                },
                timeout=60,
            )
            token_data = response.json()
            token_data_snake_case = StringTransformer.convert_keys_to_snake_case(token_data)
            mwc_token = dacite.from_dict(
                data_class=MwcScopedToken,
                data=token_data_snake_case,
                config=dacite.Config(
                    check_types=False,
                    cast=[str, int, bool, float],
                ),
            )

            self.logger.info(f"Successfully retrieved MWC token for database {database_id}")
            return mwc_token

        except Exception as e:
            error_msg = f"Failed to get MWC token for database '{database_id}' in workspace '{workspace_id}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def batch_create_kusto_database_shortcut(self, workspace_id: str, database_id: str, shortcut_params: ShortcutParams) -> None:
        self.logger.info(f"Batch creating shortcuts for database {database_id} in workspace {workspace_id}")

        try:
            mwc_token = await self.get_kusto_database_mwc_token(workspace_id, database_id)
            shortcuts_data = []
            for kql_db in shortcut_params.kql_database:
                storage = self.common_params.fabric.storage
                full_path = f"https://{storage.account}.dfs.core.windows.net/{storage.container}{kql_db.path}"

                shortcut_item = {
                    "subFolderPath": "Shortcut",
                    "name": kql_db.table,
                    "autoRenameOnConflict": False,
                    "queryAccelerationToggle": kql_db.query_acceleration_toggle,
                    "targetProperties": {
                        "Path": full_path,
                        "TargetAccountType": "ExternalADLS",
                        "TargetType": "Folder",
                        "CustomProperties": {"shortcutDataConnectionId": storage.shortcut_data_connection_id},
                    },
                }
                shortcuts_data.append(shortcut_item)

            batch_create_url = f"https://{mwc_token.target_uri_host}/webapi/capacities/{mwc_token.capacity_object_id}/" f"workloads/Kusto/KustoService/direct/v1/workspaces/{workspace_id}/" f"artifacts/{database_id}/shortcuts/batchCreate"

            response = self.http_retry.execute(
                requests.post,
                batch_create_url,
                headers={
                    "Authorization": f"MWCToken {mwc_token.token}",
                    "Content-Type": "application/json",
                },
                json=shortcuts_data,
                timeout=60,
            )
            self.logger.info(f"Successfully created {len(shortcuts_data)} shortcuts for database {database_id}")

        except Exception as e:
            error_msg = f"Failed to batch create shortcuts for database '{database_id}' in workspace '{workspace_id}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e
