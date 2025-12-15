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
    MwcTokenClient,
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
        mwc_token_client: MwcTokenClient,
    ):
        """
        Initialize the Spark manager with common parameters.

        Args:
            common_params: Common parameters containing configuration
            az_cli: Azure CLI instance
            capacity_manager: Capacity manager instance
            workspace_manager: Workspace manager instance
            http_retry_handler: HTTP retry handler instance
            mwc_token_client: MWC token client instance
        """
        super().__init__(common_params)
        self.logger = logging.getLogger(__name__)
        self.az_cli = az_cli
        self.capacity_manager = capacity_manager
        self.workspace_manager = workspace_manager
        self.http_retry = http_retry_handler
        self.mwc_token_client = mwc_token_client

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

            mwc_token_info = await self.mwc_token_client.get_spark_core_mwc_token(workspace_id, capacity_id)
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

        try:
            spark_settings = self._build_spark_settings_payload(spark_params)

            self.logger.info(f"Sending Spark configuration for {len(spark_params.pools)} pool(s) to workspace: {workspace_id}")
            self.logger.debug(f"Spark settings payload: {json.dumps(spark_settings, indent=2)}")

            response = self.http_retry.execute(
                requests.put,
                url,
                headers=headers,
                json=spark_settings,
            )

            self.logger.info(f"Successfully configured {len(spark_params.pools)} Spark pool(s) for workspace: {workspace_id}")
            self.logger.debug(f"Response status: {response.status_code}")

        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to create Spark pools for workspace {workspace_id}: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def _build_spark_settings_payload(self, spark_params: FabricSparkParams) -> dict:
        """
        Build the Spark settings payload from all SparkPools.

        Args:
            spark_params: Parameters containing all Spark pool configurations

        Returns:
            dict: Spark settings payload for API with all pools
        """
        pool_conf_items = []
        for pool_details in spark_params.pools:
            pool_conf_item = {
                "id": pool_details.id,
                "details": {
                    "name": pool_details.name,
                    "nodeSizeFamily": pool_details.node_size_family.value,
                    "nodeSize": pool_details.node_size.value,
                    "autoScale": {
                        "enabled": pool_details.auto_scale.enabled,
                        "minNodeCount": pool_details.auto_scale.min_node_count,
                        "maxNodeCount": pool_details.auto_scale.max_node_count,
                    },
                    "dynamicExecutorAllocation": {
                        "enabled": pool_details.dynamic_executor_allocation.enabled,
                        "minExecutors": pool_details.dynamic_executor_allocation.min_executors,
                        "maxExecutors": pool_details.dynamic_executor_allocation.max_executors,
                    },
                },
            }
            pool_conf_items.append(pool_conf_item)

        payload = {
            "mountPoints": {},
            "enableCustomizedComputeConf": spark_params.enable_customized_compute_conf,
            "machineLearningAutoLog": {
                "enabled": spark_params.machine_learning_auto_log.enabled,
            },
            "jobManagement": {
                "conservativeJobAdmissionEnabled": spark_params.job_management.conservative_job_admission_enabled,
                "sessionTimeoutInMinutes": spark_params.job_management.session_timeout_in_minutes,
            },
            "highConcurrency": {
                "enabled": spark_params.high_concurrency.enabled,
                "notebookPipelineRunEnabled": spark_params.high_concurrency.notebook_pipeline_run_enabled,
            },
            "currentPoolId": spark_params.current_pool_id,
            "poolConf": pool_conf_items,
            "sparkConf": {},
            "runtimeVersion": spark_params.runtime_version.value,
        }

        return payload
