# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging

from fabric_workspace_deployment.factories.management_factory import ContainerizedManagementFactory, ManagementFactory
from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.fabric.cli import FabricCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    CapacityManager,
    CicdManager,
    EntryPointOperator,
    MonitoringManager,
    Operation,
    OperationParams,
    RbacManager,
    ModelManager,
    SeedManager,
    ShortcutManager,
    SparkManager,
    WorkspaceManager,
)


class CentralOperator(EntryPointOperator):
    """Central operator that handles all operations."""

    def __init__(self, operation_params: "OperationParams"):
        """
        Creates a new instance of the CentralOperator.

        Args:
            operation_params: The operation parameters containing all configuration
        """
        super().__init__(operation_params)
        self.logger = logging.getLogger(__name__)
        self.management_factory: ManagementFactory = ContainerizedManagementFactory(operation_params)
        self.azure_cli: AzCli = self.management_factory.create_azure_cli()
        self.fabric_cli: FabricCli = self.management_factory.create_fabric_cli()
        self.capacity_manager: CapacityManager = self.management_factory.create_fabric_capacity_manager()
        self.workspace_manager: WorkspaceManager = self.management_factory.create_fabric_workspace_manager()
        self.cicd_manager: CicdManager = self.management_factory.create_fabric_cicd_manager()
        self.seed_manager: SeedManager = self.management_factory.create_fabric_seed_manager()
        self.shortcut_manager: ShortcutManager = self.management_factory.create_fabric_shortcut_manager()
        self.spark_manager: SparkManager = self.management_factory.create_fabric_spark_manager()
        self.rbac_manager: RbacManager = self.management_factory.create_fabric_rbac_manager()
        self.model_manager: ModelManager = self.management_factory.create_semantic_model_manager()
        self.monitoring_manager: MonitoringManager = self.management_factory.create_fabric_monitoring_manager()

    async def execute(self) -> None:
        """Execute the operation based on the operation type."""
        try:
            self.logger.info(f"Fabric CLI version: {self.fabric_cli.run_command('version')}")
            self.logger.info(f"Executing operation: {self.operation.value}")

            match self.operation:
                case Operation.DRY_RUN:
                    await self._execute_dry_run()

                case Operation.DEPLOY_FABRIC_CAPACITY:
                    await self._execute_deploy_fabric_capacity()

                case Operation.DEPLOY_FABRIC_WORKSPACE:
                    await self._execute_deploy_fabric_workspace()

                case Operation.DEPLOY_GIT_LINK:
                    await self._execute_deploy_git_link()

                case Operation.DEPLOY_TEMPLATE:
                    await self._execute_deploy_template()

                case Operation.DEPLOY_RBAC:
                    await self._execute_deploy_rbac()

                case Operation.DEPLOY_SEED:
                    await self._execute_deploy_seed()

                case Operation.DEPLOY_SHORTCUT:
                    await self._execute_deploy_shortcut()

                case Operation.DEPLOY_SPARK:
                    await self._execute_deploy_spark()

                case Operation.DEPLOY_MODEL:
                    await self._execute_deploy_model()

                case Operation.DEPLOY_MONITORING:
                    await self._execute_deploy_monitoring()

                case _:
                    error_message = f"Unknown operation: {self.operation}"
                    raise ValueError(error_message)

            self.logger.info(f"Successfully completed operation: {self.operation.value}")

        except Exception as e:
            self.logger.error(f"Failed to execute operation {self.operation.value}: {e}")
            raise

    # ---------------------------------------------------------------------------- #

    async def _execute_dry_run(self) -> None:
        """
        Execute dry run operation.
        """
        self.logger.info("Dry run completed.")

    async def _execute_deploy_fabric_capacity(self) -> None:
        """
        Execute deploy Fabric capacity operation.
        """
        self._azure_set()
        await self.capacity_manager.execute()

    async def _execute_deploy_fabric_workspace(self) -> None:
        """
        Execute deploy Fabric workspace operation.
        """
        await self.workspace_manager.execute()

    async def _execute_deploy_git_link(self) -> None:
        """
        Execute deploy Git link operation.
        """
        self.logger.info("Deploy Git link operation not yet implemented.")

    async def _execute_deploy_template(self) -> None:
        """
        Execute deploy template operation.
        """
        await self.cicd_manager.execute()

    async def _execute_deploy_rbac(self) -> None:
        """
        Execute deploy RBAC operation.
        """
        await self.rbac_manager.execute()

    async def _execute_deploy_seed(self) -> None:
        """
        Execute deploy seed operation.
        """
        await self.seed_manager.execute()

    async def _execute_deploy_shortcut(self) -> None:
        """
        Execute deploy shortcut operation.
        """
        await self.shortcut_manager.execute()

    async def _execute_deploy_spark(self) -> None:
        """
        Execute deploy Spark operation.
        """
        await self.spark_manager.execute()

    async def _execute_deploy_model(self) -> None:
        """
        Execute deploy model operation.
        """
        await self.model_manager.execute()

    async def _execute_deploy_monitoring(self) -> None:
        """
        Execute deploy monitoring operation.
        """
        await self.monitoring_manager.execute()

    def _azure_set(self) -> None:
        self.azure_cli.run_command(f"account set --subscription {self.operation_params.common.arm.subscription_id}")
        self.azure_cli.run_command("provider register --namespace Microsoft.Fabric")
        self.azure_cli.run_command(f"group create --name {self.operation_params.common.arm.resource_group} --location {self.operation_params.common.arm.location}")  # fmt: skip  # noqa: E501
