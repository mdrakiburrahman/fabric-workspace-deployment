# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import logging

import dacite
import requests

from fabric_workspace_deployment.client.fabric_folder import FabricFolderClient
from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.fabric.workspace import FabricWorkspaceManager
from fabric_workspace_deployment.operations.operation_interfaces import (
    CapacityManager,
    CommonParams,
    FabricWorkspaceParams,
    HttpRetryHandler,
    KustoDatabaseDetail,
    MONITORING_KUSTO_DATABASE,
    MONITORING_KUSTO_EVENTHOUSE,
    MonitoringManager,
    MonitoringKustoMetadata,
    MonitoringMetadata,
    MwcTokenClient,
)
from fabric_workspace_deployment.static.transformers import StringTransformer


class FabricMonitoringManager(MonitoringManager):
    """Concrete implementation of MonitoringManager for Microsoft Fabric."""

    def __init__(
        self,
        common_params: CommonParams,
        az_cli: AzCli,
        workspace_manager: FabricWorkspaceManager,
        http_retry_handler: HttpRetryHandler,
        folder_client: FabricFolderClient,
        mwc_token_client: MwcTokenClient,
        capacity_manager: CapacityManager,
    ):
        """
        Initialize the Fabric monitoring manager.

        Args:
            common_params: Common parameters containing monitoring configuration
            az_cli: Azure CLI instance for authentication
            workspace_manager: Workspace manager for getting workspace IDs
            http_retry_handler: HTTP retry handler with exponential backoff
            folder_client: Folder client for getting folder information
            mwc_token_client: MWC token client for authentication
            capacity_manager: Capacity manager for getting capacity IDs
        """
        super().__init__(common_params)
        self.az_cli = az_cli
        self.workspace_manager = workspace_manager
        self.http_retry = http_retry_handler
        self.folder_client = folder_client
        self.mwc_token_client = mwc_token_client
        self.capacity_manager = capacity_manager
        self.logger = logging.getLogger(__name__)

    async def execute(self) -> None:
        """
        Execute monitoring deployment operations for all workspaces.

        Configures monitoring resources based on the per-workspace configuration in MonitoringParams.
        """
        self.logger.info("Executing FabricMonitoringManager")

        tasks = []
        for workspace in self.common_params.fabric.workspaces:
            if workspace.skip_deploy:
                self.logger.info(f"Skipping monitoring for workspace '{workspace.name}' due to skipDeploy=true")
                continue

            self.logger.info(f"Workspace '{workspace.name}' monitoring configuration - " f"Database: {workspace.monitoring.kusto.database_enabled}, " f"Ingestion: {workspace.monitoring.kusto.ingestion_enabled}")

            task = asyncio.create_task(
                self.reconcile_workspace_monitoring(workspace),
                name=f"reconcile-monitoring-{workspace.name}",
            )
            tasks.append(task)

        if tasks:
            self.logger.info(f"Executing monitoring reconciliation for {len(tasks)} workspaces in parallel")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    workspace_name = self.common_params.fabric.workspaces[i].name
                    error_msg = f"Failed to reconcile monitoring for workspace '{workspace_name}': {result}"
                    self.logger.error(error_msg)
                    errors.append(error_msg)

            if errors:
                error = f"Failed to reconcile monitoring for some workspaces: {'; '.join(errors)}"
                raise RuntimeError(error)
        else:
            self.logger.info("No workspaces found for monitoring reconciliation")

        self.logger.info("Finished executing FabricMonitoringManager")

    async def reconcile_workspace_monitoring(self, workspace_params: FabricWorkspaceParams) -> None:
        """
        Reconcile monitoring configuration for a single workspace.

        Args:
            workspace_params: Workspace parameters (includes monitoring config)
        """
        self.logger.info(f"Reconciling monitoring for workspace: {workspace_params.name}")

        workspace_info = await self.workspace_manager.get(workspace_params)
        workspace_id = workspace_info.id

        monitoring = workspace_params.monitoring

        if monitoring.kusto.database_enabled:
            is_enabled = await self.is_monitoring_enabled(workspace_id)

            if not is_enabled:
                self.logger.info(f"Monitoring database does not exist for workspace '{workspace_params.name}'. Creating.")
                self.logger.warning(f"Note that this can take a few minutes for the first time to spin up the Kusto Cluster and database.")
                await self.enable_monitoring_database(workspace_id)
                self.logger.info(f"Successfully created monitoring database for workspace '{workspace_params.name}'")
            else:
                self.logger.info(f"Monitoring database already exists for workspace '{workspace_params.name}'")

            if monitoring.kusto.ingestion_enabled:
                self.logger.info(f"Enabling ingestion for workspace '{workspace_params.name}'")
                await self.set_ingestion_state(workspace_id, "Enabled")
                self.logger.info(f"Successfully enabled ingestion for workspace '{workspace_params.name}'")
            else:
                self.logger.info(f"Disabling ingestion for workspace '{workspace_params.name}'")
                await self.set_ingestion_state(workspace_id, "Disabled")
                self.logger.info(f"Successfully disabled ingestion for workspace '{workspace_params.name}'")
        else:
            self.logger.info(f"Database not enabled, ensuring monitoring is deleted for workspace '{workspace_params.name}'")
            await self.delete_monitoring(workspace_id)
            self.logger.info(f"Successfully ensured monitoring is deleted for workspace '{workspace_params.name}'")

        self.logger.info(f"Completed monitoring reconciliation for workspace: {workspace_params.name}")

    async def is_monitoring_enabled(self, workspace_id: str) -> bool:
        """
        Check if monitoring is enabled for a workspace.

        Args:
            workspace_id: The workspace ID

        Returns:
            bool: True if monitoring is enabled, False if response is "null" (string)
        """
        self.logger.info(f"Checking if monitoring is enabled for workspace ID: {workspace_id}")

        url = f"{self.common_params.endpoint.analysis_service}/metadata/platformMonitoring/workspace/{workspace_id}"
        token = self.az_cli.get_access_token(self.common_params.scope.analysis_service)

        response = self.http_retry.execute(
            requests.get,
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=60,
        )

        response_text = response.text.strip()

        # API returns string "null" when monitoring is not enabled
        if response_text == '"null"' or response_text == "null" or response_text == "" or not response_text:
            self.logger.info(f"Monitoring is not enabled for workspace ID: {workspace_id}")
            return False

        self.logger.info(f"Monitoring is enabled for workspace ID: {workspace_id}")
        return True

    async def enable_monitoring_database(self, workspace_id: str) -> None:
        """
        Enable monitoring database for a workspace.

        This operation takes 2-3 minutes as it provisions the Kusto database synchronously.

        Args:
            workspace_id: The workspace ID
        """
        self.logger.info(f"Enabling monitoring database for workspace ID: {workspace_id}")

        url = f"{self.common_params.endpoint.analysis_service}/metadata/platformMonitoring/workspace/{workspace_id}"
        token = self.az_cli.get_access_token(self.common_params.scope.analysis_service)

        payload = {"artifactType": "KustoDatabase", "workloadPayload": "{}"}

        # Use 10 minute timeout for this long-running operation
        response = self.http_retry.execute(
            requests.post,
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=600,  # 10 minutes
        )

        response_data = response.json()
        artifact_name = response_data.get("artifact", {}).get("displayName", "Unknown")
        ingestion_state = response_data.get("ingestionState", "Unknown")

        self.logger.info(f"Monitoring database enabled for workspace ID: {workspace_id}. " f"Artifact: {artifact_name}, Ingestion State: {ingestion_state}")

    async def set_ingestion_state(self, workspace_id: str, state: str) -> None:
        """
        Set the ingestion state for monitoring.

        Args:
            workspace_id: The workspace ID
            state: "Enabled" or "Disabled"
        """
        self.logger.info(f"Setting ingestion state to '{state}' for workspace ID: {workspace_id}")

        url = f"{self.common_params.endpoint.analysis_service}/metadata/platformMonitoring/workspace/{workspace_id}"
        token = self.az_cli.get_access_token(self.common_params.scope.analysis_service)

        payload = {"ingestionState": state}

        response = self.http_retry.execute(
            requests.patch,
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60,
        )

        response_data = response.json()
        ingestion_state = response_data.get("ingestionState", "Unknown")

        self.logger.info(f"Ingestion state set to: {ingestion_state} for workspace ID: {workspace_id}")

    async def delete_monitoring(self, workspace_id: str) -> None:
        """
        Delete monitoring for a workspace.

        Args:
            workspace_id: The workspace ID
        """
        self.logger.info(f"Deleting monitoring for workspace ID: {workspace_id}")

        # First check if monitoring exists
        is_enabled = await self.is_monitoring_enabled(workspace_id)
        if not is_enabled:
            self.logger.info(f"Monitoring does not exist for workspace ID: {workspace_id}, skipping deletion")
            return

        url = f"{self.common_params.endpoint.analysis_service}/metadata/platformMonitoring/workspace/{workspace_id}"
        token = self.az_cli.get_access_token(self.common_params.scope.analysis_service)

        self.http_retry.execute(
            requests.delete,
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=60,
        )

        self.logger.info(f"Successfully deleted monitoring for workspace ID: {workspace_id}")

    async def get_monitoring_metadata(self, workspace_id: str, workspace_params: FabricWorkspaceParams) -> MonitoringMetadata:
        """
        Get monitoring metadata for a workspace.

        Args:
            workspace_id: The workspace ID
            workspace_params: The workspace parameters containing capacity information

        Returns:
            MonitoringMetadata: Metadata containing Kusto connection details

        Raises:
            RuntimeError: If monitoring artifacts are not found or API calls fail
        """
        self.logger.info(f"Getting monitoring metadata for workspace ID: {workspace_id}")

        folder_collection = await self.folder_client.get_fabric_folder_collection(workspace_id)
        cluster_id = None
        database_id = None

        for artifact in folder_collection.artifacts:
            if artifact.display_name == MONITORING_KUSTO_EVENTHOUSE and artifact.type_name == "KustoEventHouse":
                cluster_id = artifact.object_id
                self.logger.info(f"Found Monitoring Eventhouse with cluster ID: {cluster_id}")
            elif artifact.display_name == MONITORING_KUSTO_DATABASE and artifact.type_name == "KustoDatabase":
                database_id = artifact.object_id
                self.logger.info(f"Found Monitoring KQL database with database ID: {database_id}")

        if not cluster_id:
            error_msg = f"Monitoring Eventhouse not found in workspace {workspace_id}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        if not database_id:
            error_msg = f"Monitoring KQL database not found in workspace {workspace_id}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        mwc_token = await self.mwc_token_client.get_kusto_database_mwc_token(workspace_id, database_id)
        self.logger.info(f"Retrieved MWC token for database {database_id}")

        capacity_info = await self.capacity_manager.get(workspace_params.capacity)
        capacity_id = capacity_info.fabric_id
        self.logger.info(f"Retrieved capacity ID: {capacity_id} for workspace {workspace_id}")

        normalized_capacity_id = capacity_id.lower().replace("-", "")

        url = f"https://{normalized_capacity_id}.pbidedicated.windows.net/webapi/capacities/{capacity_id}" f"/workloads/Kusto/KustoService/direct/v1/Eventhouse/{cluster_id}/Databases"

        self.logger.info(f"Calling Kusto API to get database details: {url}")

        response = self.http_retry.execute(
            requests.get,
            url,
            headers={
                "Authorization": f"MwcToken {mwc_token.token}",
                "Content-Type": "application/json",
            },
            timeout=60,
        )

        databases = response.json()

        if not isinstance(databases, list):
            error_msg = f"Unexpected response format from Kusto API: {databases}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        target_database = None
        for db in databases:
            if db.get("objectId") == database_id:
                target_database = db
                break

        if not target_database:
            error_msg = f"Database {database_id} not found in Kusto API response"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        database_data_snake_case = StringTransformer.convert_keys_to_snake_case(target_database)
        database_detail = dacite.from_dict(
            data_class=KustoDatabaseDetail,
            data=database_data_snake_case,
            config=dacite.Config(
                check_types=False,
                cast=[str, int, bool, float],
            ),
        )

        metadata = MonitoringMetadata(
            kusto=MonitoringKustoMetadata(
                query_service_uri=database_detail.extended_properties.query_service_uri,
                cluster_id=cluster_id,
                database_id=database_id,
            )
        )

        self.logger.info(f"Successfully retrieved monitoring metadata for workspace {workspace_id}: " f"Query Service URI: {metadata.kusto.query_service_uri}")

        return metadata
