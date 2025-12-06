# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

from __future__ import annotations

import logging

import requests

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    ArtifactClient,
    ArtifactRequest,
    CommonParams,
    FabricArtifact,
    HttpRetryHandler,
)


class FabricArtifactClient(ArtifactClient):
    """Concrete implementation of ArtifactClient for Fabric artifact operations."""

    def __init__(
        self,
        common_params: CommonParams,
        az_cli: AzCli,
        http_retry_handler: HttpRetryHandler,
    ):
        """
        Initialize the Fabric artifact client.

        Args:
            common_params: Common configuration parameters
            az_cli: Azure CLI manager for authentication
            http_retry_handler: HTTP retry handler with exponential backoff
        """
        super().__init__(common_params)
        self.az_cli = az_cli
        self.http_retry = http_retry_handler
        self.logger = logging.getLogger(__name__)

    async def post_definition(
        self,
        workspace_id: str,
        artifact_request: ArtifactRequest,
    ) -> FabricArtifact:
        url = f"{self.common_params.endpoint.analysis_service}/metadata/workspaces/{workspace_id}/artifacts"

        access_token = self.az_cli.get_access_token(self.common_params.scope.analysis_service)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        payload = {
            "artifactType": artifact_request.artifact_type,
            "description": artifact_request.description,
            "displayName": artifact_request.display_name,
        }

        self.logger.info(f"Creating artifact '{artifact_request.display_name}' " f"of type '{artifact_request.artifact_type}' " f"in workspace '{workspace_id}'")

        try:
            response = self.http_retry.execute(
                requests.post,
                url,
                headers=headers,
                json=payload,
            )

            response_data = response.json()

            artifact = FabricArtifact(
                object_id=response_data["objectId"],
                artifact_type=response_data["artifactType"],
                display_name=response_data["displayName"],
                description=response_data["description"],
            )

            self.logger.info(f"Successfully created artifact '{artifact.display_name}' " f"with ID '{artifact.object_id}'")

            return artifact

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to create artifact: {e}")
            raise RuntimeError(f"Failed to create artifact: {e}") from e
        except (KeyError, ValueError) as e:
            self.logger.error(f"Failed to parse artifact creation response: {e}")
            raise RuntimeError(f"Failed to parse artifact creation response: {e}") from e
