# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import logging

import requests

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.fabric.workspace import FabricWorkspaceManager
from fabric_workspace_deployment.operations.operation_interfaces import (
    CommonParams,
    FabricWorkspaceParams,
    HttpRetryHandler,
    MonitoringManager,
)


class FabricMonitoringManager(MonitoringManager):
    """Concrete implementation of MonitoringManager for Microsoft Fabric."""

    def __init__(
        self,
        common_params: CommonParams,
        az_cli: AzCli,
        workspace_manager: FabricWorkspaceManager,
        http_retry_handler: HttpRetryHandler,
    ):
        """
        Initialize the Fabric monitoring manager.

        Args:
            common_params: Common parameters containing monitoring configuration
            az_cli: Azure CLI instance for authentication
            workspace_manager: Workspace manager for getting workspace IDs
            http_retry_handler: HTTP retry handler with exponential backoff
        """
        super().__init__(common_params)
        self.az_cli = az_cli
        self.workspace_manager = workspace_manager
        self.http_retry = http_retry_handler
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
