# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import json
import logging

import requests

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    CapacityManager,
    CommonParams,
    FabricSparkParams,
    HttpRetryHandler,
    MwcScopedToken,
    SparkManager,
    WorkspaceManager,
)


class FabricSparkOperations(SparkManager):
    """
    Manager for Fabric Spark operations.
    """

    def __init__(
        self,
        common_params: CommonParams,
        az_cli: AzCli,
        capacity_manager: CapacityManager,
        workspace_manager: WorkspaceManager,
        http_retry_handler: HttpRetryHandler,
    ):
        """
        Initialize the Spark manager with common parameters.

        Args:
            common_params: Common parameters containing configuration
            az_cli: Azure CLI instance
            capacity_manager: Capacity manager instance
            workspace_manager: Workspace manager instance
            http_retry_handler: HTTP retry handler instance
        """
        super().__init__(common_params)
        self.logger = logging.getLogger(__name__)
        self.az_cli = az_cli
        self.capacity_manager = capacity_manager
        self.workspace_manager = workspace_manager
        self.http_retry = http_retry_handler

    async def execute(self) -> None:
        """
        Execute reconciliation for all workspaces in parallel.
        """
        self.logger.info("Starting Spark configuration reconciliation for all workspaces")

        tasks = []
        for workspace_params in self.common_params.fabric.workspaces:
            if workspace_params.skip_deploy:
                self.logger.info(f"Skipping Spark reconciliation for workspace: {workspace_params.name}")
                continue

            self.logger.info(f"Queuing Spark reconciliation for workspace: {workspace_params.name}")
            workspace_info = await self.workspace_manager.get(workspace_params)
            capacity_info = await self.capacity_manager.get(workspace_params.capacity)

            workspace_id = workspace_info.id
            capacity_id = capacity_info.fabric_id

            if not workspace_id or not capacity_id or workspace_id == "" or capacity_id == "":
                error_msg = f"Workspace ID or capacity ID is missing for workspace: {workspace_params.name}, workspace ID: {workspace_id}, capacity ID: {capacity_id}"
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)

            tasks.append(
                self.reconcile(
                    workspace_id=workspace_id,
                    capacity_id=capacity_id,
                    spark_params=workspace_params.spark,
                )
            )

        if tasks:
            await asyncio.gather(*tasks)
            self.logger.info("Completed Spark configuration reconciliation for all workspaces")
        else:
            self.logger.info("No workspaces to reconcile for Spark configuration")

    async def reconcile(self, workspace_id: str, capacity_id: str, spark_params: FabricSparkParams) -> None:
        """
        Reconcile a single workspace to desired state Spark configuration.

        Args:
            workspace_id: The Fabric workspace id
            capacity_id: The Fabric capacity id
            spark_params: Parameters for the fabric workspace Spark configuration
        """
        self.logger.info(f"Starting Spark reconciliation for workspace: {workspace_id}")

        try:
            if not spark_params.pools or len(spark_params.pools) == 0:
                self.logger.info(f"No Spark pools configured for workspace: {workspace_id}, skipping")
                return

            mwc_token_info = await self.get_mwc_token(workspace_id, capacity_id)
            await self.create_spark_pools(
                workspace_id=workspace_id,
                capacity_id=capacity_id,
                spark_params=spark_params,
                mwc_token=mwc_token_info.token,
            )

            self.logger.info(f"Successfully reconciled Spark configuration for workspace: {workspace_id}")

        except Exception as e:
            self.logger.error(f"Failed to reconcile Spark configuration for workspace {workspace_id}: {e}")
            raise

    async def get_mwc_token(self, workspace_id: str, capacity_id: str) -> MwcScopedToken:
        """
        Get MWC scoped token for Spark operations.

        Args:
            workspace_id: The workspace ID
            capacity_id: The capacity ID

        Returns:
            MwcScopedToken: MWC scoped token information

        Raises:
            RuntimeError: If the API call fails or response cannot be parsed
        """
        self.logger.info(f"Getting MWC token for workspace: {workspace_id}, capacity: {capacity_id}")
        normalized_capacity_id = capacity_id.lower().replace("-", "")
        url = f"{self.common_params.endpoint.analysis_service}/metadata/v201606/generatemwctoken"

        headers = {
            "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
            "Content-Type": "application/json",
        }

        payload = {
            "type": "[Start] GetMWCToken",
            "workloadType": "SparkCore",
            "workspaceObjectId": workspace_id,
            "capacityObjectId": normalized_capacity_id,
        }

        try:
            response = self.http_retry.execute(
                requests.post,
                url,
                headers=headers,
                json=payload,
            )

            response_data = response.json()

            if "Token" not in response_data:
                error_msg = f"MWC token not found in response: {response_data}"
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)

            mwc_token = response_data["Token"]
            target_uri_host = f"{normalized_capacity_id}.pbidedicated.windows.net"

            self.logger.info(f"Successfully obtained MWC token for workspace: {workspace_id}")

            return MwcScopedToken(
                token=mwc_token,
                target_uri_host=target_uri_host,
                capacity_object_id=normalized_capacity_id,
            )

        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to get MWC token for workspace {workspace_id}: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def create_spark_pools(
        self,
        workspace_id: str,
        capacity_id: str,
        spark_params: FabricSparkParams,
        mwc_token: str,
    ) -> None:
        """
        Create Spark pools using MWC token.

        Args:
            workspace_id: The workspace ID
            capacity_id: The capacity ID
            spark_params: Parameters for the Spark pools to create
            mwc_token: MWC authentication token

        Raises:
            RuntimeError: If the API call fails
        """
        self.logger.info(f"Creating Spark pools for workspace: {workspace_id}")
        normalized_capacity_id = capacity_id.lower().replace("-", "")

        if not spark_params.pools or len(spark_params.pools) == 0:
            self.logger.info(f"No Spark pools configured for workspace: {workspace_id}, skipping")
            return

        url = f"https://{normalized_capacity_id}.pbidedicated.windows.net/webapi/capacities/{capacity_id}/workloads/SparkCore/SparkCoreService/automatic/v1/workspaces/{workspace_id}/sparkSettings/content"

        headers = {
            "Authorization": f"mwctoken {mwc_token}",
            "Content-Type": "application/json",
            "x-ms-workload-resource-moniker": workspace_id,
        }

        for pool in spark_params.pools:
            try:
                spark_settings = self._build_spark_settings_payload(pool)

                self.logger.info(f"Sending Spark configuration for pool to workspace: {workspace_id}")
                self.logger.debug(f"Spark settings payload: {json.dumps(spark_settings, indent=2)}")

                response = self.http_retry.execute(
                    requests.put,
                    url,
                    headers=headers,
                    json=spark_settings,
                )

                self.logger.info(f"Successfully configured Spark pool for workspace: {workspace_id}")
                self.logger.debug(f"Response status: {response.status_code}")

            except requests.exceptions.RequestException as e:
                error_msg = f"Failed to create Spark pool for workspace {workspace_id}: {e}"
                self.logger.error(error_msg)
                raise RuntimeError(error_msg) from e

    def _build_spark_settings_payload(self, pool) -> dict:
        """
        Build the Spark settings payload from a single SparkPool.

        Args:
            pool: A single SparkPool configuration

        Returns:
            dict: Spark settings payload for API

        Raises:
            ValueError: If pool details or pool ID is missing
        """
        if not pool.details:
            error_msg = "Cannot build Spark settings payload: pool details are required"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        if not pool.details.id:
            error_msg = "Cannot build Spark settings payload: pool ID is required"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        pool_conf_item = {
            "id": pool.details.id,
            "details": {
                "name": pool.details.name,
                "nodeSizeFamily": pool.details.node_size_family.value,
                "nodeSize": pool.details.node_size.value,
                "autoScale": {
                    "enabled": pool.details.auto_scale.enabled,
                    "minNodeCount": pool.details.auto_scale.min_node_count,
                    "maxNodeCount": pool.details.auto_scale.max_node_count,
                },
                "dynamicExecutorAllocation": {
                    "enabled": pool.details.dynamic_executor_allocation.enabled,
                    "minExecutors": pool.details.dynamic_executor_allocation.min_executors,
                    "maxExecutors": pool.details.dynamic_executor_allocation.max_executors,
                },
            },
        }

        payload = {
            "mountPoints": {},
            "enableCustomizedComputeConf": pool.enable_customized_compute_conf,
            "machineLearningAutoLog": {
                "enabled": pool.machine_learning_auto_log.enabled,
            },
            "jobManagement": {
                "conservativeJobAdmissionEnabled": pool.job_management.conservative_job_admission_enabled,
                "sessionTimeoutInMinutes": pool.job_management.session_timeout_in_minutes,
            },
            "highConcurrency": {
                "enabled": pool.high_concurrency.enabled,
                "notebookPipelineRunEnabled": pool.high_concurrency.notebook_pipeline_run_enabled,
            },
            "currentPoolId": pool.details.id,
            "poolConf": [pool_conf_item],
            "sparkConf": {},
            "runtimeVersion": pool.runtime_version.value,
        }

        return payload
