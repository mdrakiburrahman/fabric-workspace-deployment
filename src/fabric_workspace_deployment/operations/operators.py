# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging

from fabric_workspace_deployment.factories.management_factory import ContainerizedManagementFactory, ManagementFactory
from fabric_workspace_deployment.manager.fabric.cli import FabricCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    AlertManager,
    CapacityManager,
    CicdManager,
    EntitlementManager,
    EntryPointOperator,
    GitLinkManager,
    Manager,
    MonitoringManager,
    ModelManager,
    Operation,
    OperationParams,
    RbacManager,
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
        self.fabric_cli: FabricCli = self.management_factory.create_fabric_cli()
        self.capacity_manager: CapacityManager = self.management_factory.create_fabric_capacity_manager()
        self.alert_manager: AlertManager = self.management_factory.create_fabric_alert_manager()
        self.workspace_manager: WorkspaceManager = self.management_factory.create_fabric_workspace_manager()
        self.cicd_manager: CicdManager = self.management_factory.create_fabric_cicd_manager()
        self.seed_manager: SeedManager = self.management_factory.create_fabric_seed_manager()
        self.shortcut_manager: ShortcutManager = self.management_factory.create_fabric_shortcut_manager()
        self.spark_manager: SparkManager = self.management_factory.create_fabric_spark_manager()
        self.rbac_manager: RbacManager = self.management_factory.create_fabric_rbac_manager()
        self.model_manager: ModelManager = self.management_factory.create_semantic_model_manager()
        self.monitoring_manager: MonitoringManager = self.management_factory.create_fabric_monitoring_manager()
        self.entitlement_manager: EntitlementManager = self.management_factory.create_entitlement_manager()
        self.git_link_manager: GitLinkManager = self.management_factory.create_fabric_git_link_manager()
        self.managers: dict[Operation, Manager] = {
            Operation.DRY_RUN: self.entitlement_manager,
            Operation.DEPLOY_ALERT: self.alert_manager,
            Operation.DEPLOY_FABRIC_CAPACITY: self.capacity_manager,
            Operation.DEPLOY_FABRIC_WORKSPACE: self.workspace_manager,
            Operation.DEPLOY_GIT_LINK: self.git_link_manager,
            Operation.DEPLOY_TEMPLATE: self.cicd_manager,
            Operation.DEPLOY_RBAC: self.rbac_manager,
            Operation.DEPLOY_SEED: self.seed_manager,
            Operation.DEPLOY_SHORTCUT: self.shortcut_manager,
            Operation.DEPLOY_SPARK: self.spark_manager,
            Operation.DEPLOY_MODEL: self.model_manager,
            Operation.DEPLOY_MONITORING: self.monitoring_manager,
        }

    async def _execute(self) -> None:
        """Execute the operation based on the operation type."""
        try:
            self.logger.info(f"Fabric CLI version: {self.fabric_cli.run_command('version')}")
            self.logger.info(f"Executing operation: {self.operation.value}")

            manager = self.managers.get(self.operation)
            if manager is None:
                raise ValueError(f"Unknown operation: {self.operation}")

            await manager.execute()
            if self.operation == Operation.DRY_RUN:
                self.logger.info("Dry run completed.")

            self.logger.info(f"Successfully completed operation: {self.operation.value}")

        except Exception as e:
            self.logger.error(f"Failed to execute operation {self.operation.value}: {e}")
            raise
