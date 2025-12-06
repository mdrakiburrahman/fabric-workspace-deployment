# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

from __future__ import annotations

import logging

from fabric_workspace_deployment.client.fabric_artifact import FabricArtifactClient
from fabric_workspace_deployment.client.fabric_folder import FabricFolderClient
from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    ArtifactRequest,
    ArtifactType,
    CommonParams,
    FabricArtifact,
    HttpRetryHandler,
    SparkJobDefinition,
    SparkJobDefinitionClient,
)


class FabricSparkJobDefinitionClient(SparkJobDefinitionClient):
    """Concrete implementation of SparkJobDefinitionClient for Fabric Spark Job Definition operations."""

    def __init__(
        self,
        common_params: CommonParams,
        az_cli: AzCli,
        http_retry_handler: HttpRetryHandler,
        artifact_client: FabricArtifactClient,
    ):
        """
        Initialize the Fabric Spark Job Definition client.

        Args:
            common_params: Common configuration parameters
            az_cli: Azure CLI manager for authentication
            http_retry_handler: HTTP retry handler with exponential backoff
            artifact_client: Fabric Artifact Client for artifact operations
        """
        super().__init__(common_params)
        self.az_cli = az_cli
        self.http_retry = http_retry_handler
        self.logger = logging.getLogger(__name__)

        self.folder_client = FabricFolderClient(
            common_params=common_params,
            az_cli=az_cli,
            http_retry_handler=http_retry_handler,
        )

        self.artifact_client = artifact_client

    async def create_spark_job_definition_artifact(
        self,
        workspace_id: str,
        spark_job_definition: SparkJobDefinition,
    ) -> FabricArtifact:
        self.logger.info(f"Creating or retrieving Spark Job Definition '{spark_job_definition.display_name}' in workspace ID: {workspace_id}')")
        artifact_collection = await self.folder_client.get_fabric_folder_collection(workspace_id)
        matching_artifacts = [artifact for artifact in artifact_collection.artifacts if artifact.type_name == ArtifactType.SPARK_JOB_DEFINITION.value and artifact.display_name == spark_job_definition.display_name]

        if matching_artifacts:
            existing_artifact = matching_artifacts[0]
            self.logger.info(f"Found existing Spark Job Definition '{existing_artifact.display_name}' " f"with ID '{existing_artifact.object_id}'")

            description_to_use = spark_job_definition.description
            return FabricArtifact(object_id=existing_artifact.object_id, artifact_type=existing_artifact.type_name, display_name=existing_artifact.display_name, description=description_to_use)

        self.logger.info(f"No existing Spark Job Definition found with name '{spark_job_definition.display_name}', creating new one")

        artifact_request = ArtifactRequest(
            artifact_type=ArtifactType.SPARK_JOB_DEFINITION.value,
            description=spark_job_definition.description,
            display_name=spark_job_definition.display_name,
        )

        return await self.artifact_client.post_definition(
            workspace_id=workspace_id,
            artifact_request=artifact_request,
        )
