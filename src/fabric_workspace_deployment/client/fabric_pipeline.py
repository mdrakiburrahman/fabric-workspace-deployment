# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging
from dataclasses import dataclass

from fabric_workspace_deployment.operations.operation_interfaces import (
    ArtifactType,
    CommonParams,
    FolderClient,
    PipelineClient,
)


@dataclass
class PipelineProperties:
    """Properties of a Fabric Pipeline."""

    description: str


@dataclass
class Pipeline:
    """Represents a Fabric Pipeline artifact."""

    id: str
    object_id: str
    type: int
    type_name: str
    display_name: str
    permissions: int
    is_hidden: bool
    artifact_permissions: int
    properties: PipelineProperties


class FabricPipelineClient(PipelineClient):
    """
    Client for interacting with Fabric Pipeline artifacts.

    This client provides methods to list and retrieve pipeline information
    from a Fabric workspace.
    """

    def __init__(
        self,
        common_params: CommonParams,
        folder_client: FolderClient,
    ):
        """
        Initialize the Fabric Pipeline client.

        Args:
            common_params: Common configuration parameters
            folder_client: Client for accessing Fabric folder operations
        """
        super().__init__(common_params)
        self.folder_client = folder_client
        self.logger = logging.getLogger(__name__)

    async def list_pipelines(self, workspace_object_id: str) -> list[Pipeline]:
        """
        List all pipelines in a workspace.

        Args:
            workspace_object_id: The Fabric workspace object ID

        Returns:
            List of Pipeline objects

        Raises:
            RuntimeError: If the API call fails
        """
        self.logger.info(f"Listing pipelines in workspace {workspace_object_id}")

        try:
            folder_info = await self.folder_client.get_fabric_folder_collection(workspace_object_id)

            pipelines = []
            for artifact in folder_info.artifacts:
                if artifact.type_name == ArtifactType.PIPELINE.value:
                    pipeline = self._cast_to_pipeline(artifact)
                    pipelines.append(pipeline)

            self.logger.info(f"Found {len(pipelines)} pipelines in workspace {workspace_object_id}")
            return pipelines

        except Exception as e:
            error_msg = f"Failed to list pipelines in workspace {workspace_object_id}: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def get_pipeline(self, workspace_object_id: str, pipeline_display_name: str) -> Pipeline | None:
        """
        Get a specific pipeline by display name.

        Args:
            workspace_object_id: The Fabric workspace object ID
            pipeline_display_name: The display name of the pipeline

        Returns:
            Pipeline object if found, None otherwise

        Raises:
            RuntimeError: If the API call fails
        """
        self.logger.info(f"Getting pipeline '{pipeline_display_name}' in workspace {workspace_object_id}")

        try:
            pipelines = await self.list_pipelines(workspace_object_id)

            for pipeline in pipelines:
                if pipeline.display_name == pipeline_display_name:
                    self.logger.info(f"Found pipeline '{pipeline_display_name}' with object_id {pipeline.object_id}")
                    return pipeline

            self.logger.warning(f"Pipeline '{pipeline_display_name}' not found in workspace {workspace_object_id}")
            return None

        except Exception as e:
            error_msg = f"Failed to get pipeline '{pipeline_display_name}' in workspace {workspace_object_id}: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def _cast_to_pipeline(self, artifact) -> Pipeline:
        """
        Convert a FabricFolderArtifact to a Pipeline object.

        Args:
            artifact: FabricFolderArtifact from folder collection

        Returns:
            Pipeline object
        """
        return Pipeline(
            id=str(artifact.id),
            object_id=artifact.object_id,
            type=artifact.type,
            type_name=artifact.type_name,
            display_name=artifact.display_name,
            permissions=artifact.permissions,
            is_hidden=artifact.is_hidden,
            artifact_permissions=artifact.artifact_permissions,
            properties=PipelineProperties(description=""),  # Fabric doesn't expose description in list view
        )
