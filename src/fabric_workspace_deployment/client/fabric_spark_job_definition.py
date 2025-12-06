# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

from __future__ import annotations

import json
import logging

import requests

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
    SparkJobDefinitionV1Config,
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

    async def update_spark_job_definition_config(
        self,
        workspace_id: str,
        spark_job_definition_id: str,
        lakehouse_name: str,
        config: SparkJobDefinitionV1Config,
    ) -> None:
        self.logger.info(f"Updating Spark Job Definition configuration for artifact ID: {spark_job_definition_id}")
        artifact_collection = await self.folder_client.get_fabric_folder_collection(workspace_id)
        lakehouse_map = {artifact.display_name: artifact.object_id for artifact in artifact_collection.artifacts if artifact.type_name == ArtifactType.LAKEHOUSE.value}
        if lakehouse_name not in lakehouse_map:
            error_msg = f"Default Lakehouse '{lakehouse_name}' not found in workspace. " f"Available Lakehouses: {list(lakehouse_map.keys())}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        lakehouse_artifact_id = lakehouse_map[lakehouse_name]
        self.logger.info(f"Resolved Lakehouse '{lakehouse_name}' to artifact ID: {lakehouse_artifact_id}")

        url = f"{self.common_params.endpoint.analysis_service}/metadata/artifacts/{spark_job_definition_id}"

        access_token = self.az_cli.get_access_token(self.common_params.scope.analysis_service)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        workload_payload = {
            "mainClass": config.main_class,
            "executableFile": config.executable_file,
            "environmentArtifactId": config.environment_artifact_id or "",
            "language": config.language,
            "commandLineArguments": config.command_line_arguments,
            "defaultLakehouseArtifactId": lakehouse_artifact_id,
            "sparkSettings": {},
            "additionalLibraryUris": config.additional_library_uris,
        }

        if config.retry_policy:
            workload_payload["retryPolicy"] = config.retry_policy

        if config.additional_lakehouse_ids:
            workload_payload["additionalLakehouseIds"] = config.additional_lakehouse_ids

        workload_payload_str = json.dumps(workload_payload, separators=(",", ":"))

        patch_payload = {
            "workloadPayload": workload_payload_str,
            "payloadContentType": "InlineJson",
        }

        self.logger.debug(f"PATCH payload: {json.dumps(patch_payload, indent=2)}")

        try:
            response = self.http_retry.execute(
                requests.patch,
                url,
                headers=headers,
                json=patch_payload,
            )

            self.logger.info(f"Successfully updated Spark Job Definition configuration for artifact ID: {spark_job_definition_id}")

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to update Spark Job Definition configuration: {e}")
            raise RuntimeError(f"Failed to update Spark Job Definition configuration: {e}") from e
