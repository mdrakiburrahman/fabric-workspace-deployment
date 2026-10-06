# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging
import os
import time
from abc import ABC, abstractmethod

from azure.core.credentials import TokenCredential
from azure.identity import AzureCliCredential

from fabric_workspace_deployment.client.fabric_artifact import FabricArtifactClient
from fabric_workspace_deployment.client.fabric_folder import FabricFolderClient
from fabric_workspace_deployment.client.fabric_gateway import FabricGatewayClient
from fabric_workspace_deployment.client.fabric_semantic_model import FabricSemanticModelClient
from fabric_workspace_deployment.client.rayfin_database import FabricRayfinDatabaseClient
from fabric_workspace_deployment.client.fabric_pipeline import FabricPipelineClient
from fabric_workspace_deployment.client.fabric_pipeline_run import FabricPipelineRunClient
from fabric_workspace_deployment.client.fabric_spark_job_definition import FabricSparkJobDefinitionClient
from fabric_workspace_deployment.environment_variables import FAB_TOKEN_CICD_ENV_VAR
from fabric_workspace_deployment.identity.token_credential import StaticTokenCredential
from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.azure.entitlement import AzEntitlementManager
from fabric_workspace_deployment.manager.azure.rbac import ArmRbacManager
from fabric_workspace_deployment.manager.azure.storage import AzStorageManager
from fabric_workspace_deployment.manager.docker.cli import DockerCli
from fabric_workspace_deployment.manager.fabric.capacity import FabricCapacityManager
from fabric_workspace_deployment.manager.fabric.cicd import FabricCicdManager
from fabric_workspace_deployment.manager.fabric.cli import FabricCli
from fabric_workspace_deployment.manager.fabric.contacts import FabricAlertManager
from fabric_workspace_deployment.manager.fabric.git_link import FabricGitLinkManager
from fabric_workspace_deployment.manager.fabric.gateway import FabricGatewayManager
from fabric_workspace_deployment.manager.fabric.model import SemanticModelManager
from fabric_workspace_deployment.manager.fabric.monitoring import FabricMonitoringManager
from fabric_workspace_deployment.manager.fabric.rbac import FabricRbacManager
from fabric_workspace_deployment.manager.fabric.seed import FabricSeedManager
from fabric_workspace_deployment.manager.fabric.shortcut import FabricShortcutManager
from fabric_workspace_deployment.manager.fabric.spark import FabricSparkOperations
from fabric_workspace_deployment.manager.fabric.workspace import FabricWorkspaceManager
from fabric_workspace_deployment.manager.rayfin.deployment import RayfinDeploymentManager
from fabric_workspace_deployment.operations.operation_interfaces import GraphClient, HttpRetryHandler, MwcTokenClient, OperationParams, SparkEnvironmentClient


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
    def create_docker_cli(self) -> DockerCli:
        """
        Create a Docker CLI instance.
        """
        pass

    @abstractmethod
    def create_fabric_capacity_manager(self) -> FabricCapacityManager:
        """
        Create a Fabric Capacity Manager instance.
        """
        pass

    @abstractmethod
    def create_fabric_alert_manager(self) -> FabricAlertManager:
        """
        Create a Fabric Alert Manager instance.
        """
        pass

    @abstractmethod
    def create_arm_rbac_manager(self) -> ArmRbacManager:
        """
        Create an ARM RBAC Manager instance.
        """
        pass

    @abstractmethod
    def create_graph_client(self) -> "GraphClient":
        """
        Create a Microsoft Graph Client instance.
        """
        pass

    @abstractmethod
    def create_entitlement_manager(self) -> AzEntitlementManager:
        """
        Create an Entitlement Manager instance.
        """
        pass

    @abstractmethod
    def create_fabric_git_link_manager(self) -> FabricGitLinkManager:
        """
        Create a Fabric Git Link Manager instance.
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
    def create_fabric_seed_manager(self) -> FabricSeedManager:
        """
        Create a Fabric Seed Manager instance.
        """
        pass

    @abstractmethod
    def create_fabric_shortcut_manager(self) -> FabricShortcutManager:
        """
        Create a Fabric Shortcut Manager instance.
        """
        pass

    @abstractmethod
    def create_fabric_spark_manager(self) -> FabricSparkOperations:
        """
        Create a Fabric Spark Manager instance.
        """
        pass

    @abstractmethod
    def create_fabric_rbac_manager(self) -> FabricRbacManager:
        """
        Create a Fabric RBAC Manager instance.
        """
        pass

    @abstractmethod
    def create_semantic_model_manager(self) -> SemanticModelManager:
        """
        Create a Semantic Model Manager instance.
        """
        pass

    @abstractmethod
    def create_fabric_gateway_client(self) -> FabricGatewayClient:
        pass

    @abstractmethod
    def create_fabric_gateway_manager(self) -> FabricGatewayManager:
        pass

    @abstractmethod
    def create_semantic_model_client(self) -> FabricSemanticModelClient:
        pass

    @abstractmethod
    def create_fabric_monitoring_manager(self) -> FabricMonitoringManager:
        """
        Create a Fabric Monitoring Manager instance.
        """
        pass

    @abstractmethod
    def create_rayfin_manager(self) -> RayfinDeploymentManager:
        """
        Create a Rayfin deployment manager instance.
        """
        pass

    @abstractmethod
    def create_rayfin_database_client(self) -> FabricRayfinDatabaseClient:
        pass

    @abstractmethod
    def create_fabric_folder_client(self) -> "FabricFolderClient":
        """
        Create a Fabric Folder Client instance.
        """
        pass

    @abstractmethod
    def create_fabric_artifact_client(self) -> "FabricArtifactClient":
        """
        Create a Fabric Artifact Client instance.
        """
        pass

    @abstractmethod
    def create_fabric_spark_job_definition_client(self) -> "FabricSparkJobDefinitionClient":
        """
        Create a Fabric Spark Job Definition Client instance.
        """
        pass

    @abstractmethod
    def create_fabric_pipeline_client(self) -> "FabricPipelineClient":
        """
        Create a Fabric Pipeline Client instance.
        """
        pass

    @abstractmethod
    def create_fabric_pipeline_run_client(self) -> "FabricPipelineRunClient":
        """
        Create a Fabric Pipeline Run Client instance.
        """
        pass

    @abstractmethod
    def create_fabric_mwc_token_client(self) -> "MwcTokenClient":
        """
        Create a Fabric MWC Token Client instance.
        """
        pass

    @abstractmethod
    def create_fabric_spark_environment_client(self) -> "SparkEnvironmentClient":
        """
        Create a Fabric Spark Environment Client instance.
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
        self.http_retry_handler = HttpRetryHandler(logger=self.logger)

    def create_azure_cli(self) -> AzCli:
        return AzCli(exit_on_error=True, logger=self.logger)

    def create_fabric_cli(self) -> FabricCli:
        return FabricCli(exit_on_error=True, logger=self.logger)

    def _create_cicd_token_credential(self) -> TokenCredential:
        fab_token_cicd = os.getenv(FAB_TOKEN_CICD_ENV_VAR, "").strip()
        if fab_token_cicd:
            expiry = int(time.time()) + (365 * 24 * 60 * 60)
            return StaticTokenCredential(fab_token_cicd, expiry)
        return AzureCliCredential()

    def create_docker_cli(self) -> DockerCli:
        return DockerCli(logger=self.logger)

    def create_fabric_capacity_manager(self) -> FabricCapacityManager:
        return FabricCapacityManager(self.operation_params.common, self.create_azure_cli(), self.create_fabric_cli())

    def create_fabric_alert_manager(self) -> FabricAlertManager:
        return FabricAlertManager(
            self.operation_params.common,
            self.create_azure_cli(),
            self.create_fabric_workspace_manager(),
            self.create_fabric_folder_client(),
            self.http_retry_handler,
        )

    def create_arm_rbac_manager(self) -> ArmRbacManager:
        return ArmRbacManager(self.operation_params.common, self.http_retry_handler, self.logger)

    def create_graph_client(self) -> "GraphClient":
        from fabric_workspace_deployment.client.graph_membership import MsGraphMembershipClient

        return MsGraphMembershipClient(
            self.operation_params.common,
            self.create_azure_cli(),
            self.http_retry_handler,
            self.logger,
        )

    def create_entitlement_manager(self) -> AzEntitlementManager:
        return AzEntitlementManager(
            self.operation_params.common,
            self.create_graph_client(),
            self.logger,
        )

    def create_fabric_git_link_manager(self) -> FabricGitLinkManager:
        return FabricGitLinkManager(self.operation_params.common)

    def create_fabric_workspace_manager(self) -> FabricWorkspaceManager:
        return FabricWorkspaceManager(
            self.operation_params.common,
            self.create_azure_cli(),
            self.create_fabric_cli(),
            self.http_retry_handler,
            arm_rbac_manager=self.create_arm_rbac_manager(),
        )

    def create_fabric_cicd_manager(self) -> FabricCicdManager:
        return FabricCicdManager(
            self.operation_params.common,
            self._create_cicd_token_credential(),
            self.create_azure_cli(),
            self.create_fabric_cli(),
            self.create_fabric_workspace_manager(),
            self.create_fabric_spark_job_definition_client(),
            self.create_fabric_folder_client(),
            self.create_fabric_monitoring_manager(),
            self.create_fabric_spark_environment_client(),
        )

    def create_fabric_seed_manager(self) -> FabricSeedManager:
        return FabricSeedManager(
            self.operation_params.common,
            AzStorageManager(
                self.operation_params.common,
                self.create_azure_cli(),
                self.logger,
            ),
            self.logger,
        )

    def create_fabric_shortcut_manager(self) -> FabricShortcutManager:
        return FabricShortcutManager(
            self.operation_params.common,
            self.create_azure_cli(),
            self.create_fabric_cli(),
            self.create_fabric_workspace_manager(),
            self.http_retry_handler,
            self.create_fabric_mwc_token_client(),
        )

    def create_fabric_spark_manager(self) -> FabricSparkOperations:
        return FabricSparkOperations(
            self.operation_params.common,
            self.create_azure_cli(),
            self.create_fabric_capacity_manager(),
            self.create_fabric_workspace_manager(),
            self.http_retry_handler,
            self.create_fabric_mwc_token_client(),
        )

    def create_fabric_rbac_manager(self) -> FabricRbacManager:
        return FabricRbacManager(
            self.operation_params.common,
            self.create_azure_cli(),
            self.create_fabric_cli(),
            self.create_fabric_workspace_manager(),
            self.create_fabric_folder_client(),
            self.http_retry_handler,
        )

    def create_semantic_model_manager(self) -> SemanticModelManager:
        return SemanticModelManager(
            self.operation_params.common,
            self.create_azure_cli(),
            self.create_fabric_workspace_manager(),
            self.create_fabric_folder_client(),
            self.http_retry_handler,
            self._create_cicd_token_credential(),
            self.create_fabric_gateway_client(),
            self.create_semantic_model_client(),
        )

    def create_fabric_gateway_client(self) -> FabricGatewayClient:
        return FabricGatewayClient(self.operation_params.common, self.create_azure_cli(), self.http_retry_handler)

    def create_fabric_gateway_manager(self) -> FabricGatewayManager:
        return FabricGatewayManager(self.operation_params.common, self.create_fabric_gateway_client())

    def create_semantic_model_client(self) -> FabricSemanticModelClient:
        return FabricSemanticModelClient(self.operation_params.common, self.create_azure_cli(), self.http_retry_handler)

    def create_fabric_monitoring_manager(self) -> FabricMonitoringManager:
        return FabricMonitoringManager(
            self.operation_params.common,
            self.create_azure_cli(),
            self.create_fabric_workspace_manager(),
            self.http_retry_handler,
            self.create_fabric_folder_client(),
            self.create_fabric_mwc_token_client(),
            self.create_fabric_capacity_manager(),
        )

    def create_rayfin_manager(self) -> RayfinDeploymentManager:
        return RayfinDeploymentManager(
            self.operation_params.common,
            self.operation_params.common.fabric.workspaces,
            self.create_azure_cli(),
            self.create_fabric_cli(),
            self.create_docker_cli(),
            database_client=self.create_rayfin_database_client(),
            logger=self.logger,
        )

    def create_rayfin_database_client(self) -> FabricRayfinDatabaseClient:
        return FabricRayfinDatabaseClient(self.operation_params.common, self.create_azure_cli(), self.http_retry_handler)

    def create_fabric_folder_client(self) -> FabricFolderClient:
        return FabricFolderClient(
            self.operation_params.common,
            self.create_azure_cli(),
            self.http_retry_handler,
        )

    def create_fabric_artifact_client(self) -> FabricArtifactClient:
        return FabricArtifactClient(
            self.operation_params.common,
            self.create_azure_cli(),
            self.http_retry_handler,
        )

    def create_fabric_spark_job_definition_client(self) -> FabricSparkJobDefinitionClient:
        return FabricSparkJobDefinitionClient(
            self.operation_params.common,
            self.create_azure_cli(),
            self.http_retry_handler,
            self.create_fabric_artifact_client(),
        )

    def create_fabric_pipeline_client(self) -> FabricPipelineClient:
        return FabricPipelineClient(
            self.operation_params.common,
            self.create_fabric_folder_client(),
        )

    def create_fabric_pipeline_run_client(self) -> FabricPipelineRunClient:
        return FabricPipelineRunClient(
            self.operation_params.common,
            self.create_azure_cli(),
            self.http_retry_handler,
        )

    def create_fabric_mwc_token_client(self) -> "MwcTokenClient":
        from fabric_workspace_deployment.client.fabric_mwc_token_client import FabricMwcTokenClient

        return FabricMwcTokenClient(
            self.operation_params.common,
            self.create_azure_cli(),
            self.http_retry_handler,
        )

    def create_fabric_spark_environment_client(self) -> "SparkEnvironmentClient":
        from fabric_workspace_deployment.client.fabric_spark_environment import FabricSparkEnvironmentClient

        return FabricSparkEnvironmentClient(
            self.operation_params.common,
            self.create_fabric_mwc_token_client(),
            self.http_retry_handler,
            self.create_azure_cli(),
        )
