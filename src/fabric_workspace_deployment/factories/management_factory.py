# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging
import os
import time
from abc import ABC, abstractmethod

from azure.identity import AzureCliCredential

from fabric_workspace_deployment.identity.token_credential import StaticTokenCredential
from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.fabric.capacity import FabricCapacityManager
from fabric_workspace_deployment.manager.fabric.cicd import FabricCicdManager
from fabric_workspace_deployment.manager.fabric.cli import FabricCli
from fabric_workspace_deployment.manager.fabric.rbac import FabricRbacManager
from fabric_workspace_deployment.manager.fabric.shortcut import FabricShortcutManager
from fabric_workspace_deployment.manager.fabric.workspace import FabricWorkspaceManager
from fabric_workspace_deployment.operations.operation_interfaces import OperationParams


class ManagementFactory(ABC):
    """
    Factory for creating various managers.
    """

    @abstractmethod
    def create_azure_cli(self) -> AzCli:
        """
        Create a Azure CLI instance.
        """
        pass

    @abstractmethod
    def create_fabric_cli(self) -> FabricCli:
        """
        Create a Fabric CLI instance.
        """
        pass

    @abstractmethod
    def create_fabric_capacity_manager(self) -> FabricCapacityManager:
        """
        Create a Fabric Capacity Manager instance.
        """
        pass

    @abstractmethod
    def create_fabric_workspace_manager(self) -> FabricWorkspaceManager:
        """
        Create a Fabric Workspace Manager instance.
        """
        pass

    @abstractmethod
    def create_fabric_cicd_manager(self) -> FabricCicdManager:
        """
        Create a Fabric CICD Manager instance.
        """
        pass

    @abstractmethod
    def create_fabric_shortcut_manager(self) -> FabricShortcutManager:
        """
        Create a Fabric Shortcut Manager instance.
        """
        pass

    @abstractmethod
    def create_fabric_rbac_manager(self) -> FabricRbacManager:
        """
        Create a Fabric RBAC Manager instance.
        """
        pass


class ContainerizedManagementFactory(ManagementFactory):
    """Containerized implementation of the ManagementFactory."""

    def __init__(self, operation_params: "OperationParams"):
        """
        Initialize the factory with operation parameters.

        Args:
            operation_params: The operation parameters containing all configuration
        """
        self.operation_params = operation_params
        self.logger = logging.getLogger(__name__)

    def create_azure_cli(self) -> AzCli:
        return AzCli(exit_on_error=True, logger=self.logger)

    def create_fabric_cli(self) -> FabricCli:
        return FabricCli(exit_on_error=True, logger=self.logger)

    def create_fabric_capacity_manager(self) -> FabricCapacityManager:
        return FabricCapacityManager(self.operation_params.common, self.create_azure_cli(), self.create_fabric_cli())

    def create_fabric_workspace_manager(self) -> FabricWorkspaceManager:
        return FabricWorkspaceManager(self.operation_params.common, self.create_azure_cli(), self.create_fabric_cli())

    def create_fabric_cicd_manager(self) -> FabricCicdManager:
        fab_token_cicd = os.getenv("FAB_TOKEN_CICD", "").strip()
        if fab_token_cicd:
            expiry = int(time.time()) + (365 * 24 * 60 * 60)
            token_credential = StaticTokenCredential(fab_token_cicd, expiry)
        else:
            token_credential = AzureCliCredential()

        return FabricCicdManager(
            self.operation_params.common,
            token_credential,
            self.create_azure_cli(),
            self.create_fabric_cli(),
            self.create_fabric_workspace_manager(),
        )

    def create_fabric_shortcut_manager(self) -> FabricShortcutManager:
        return FabricShortcutManager(
            self.operation_params.common,
            self.create_azure_cli(),
            self.create_fabric_cli(),
            self.create_fabric_workspace_manager(),
        )

    def create_fabric_rbac_manager(self) -> FabricRbacManager:
        return FabricRbacManager(
            self.operation_params.common,
            self.create_azure_cli(),
            self.create_fabric_cli(),
            self.create_fabric_workspace_manager(),
        )
