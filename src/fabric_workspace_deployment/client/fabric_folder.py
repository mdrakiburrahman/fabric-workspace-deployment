# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging

import dacite
import requests

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    CommonParams,
    FabricFolderCollection,
    FolderClient,
    HttpRetryHandler,
)
from fabric_workspace_deployment.static.transformers import StringTransformer


class FabricFolderClient(FolderClient):
    """Concrete implementation of FolderClient for Fabric folder operations."""

    def __init__(
        self,
        common_params: CommonParams,
        az_cli: AzCli,
        http_retry_handler: HttpRetryHandler,
    ):
        """
        Initialize the Fabric folder client.

        Args:
            common_params: Common parameters shared across operations
            az_cli: Azure CLI manager for authentication
            http_retry_handler: HTTP retry handler with exponential backoff
        """
        super().__init__(common_params)
        self.az_cli = az_cli
        self.http_retry = http_retry_handler
        self.logger = logging.getLogger(__name__)

    async def get_fabric_folder_collection(self, workspace_id: str) -> FabricFolderCollection:
        """
        Get Fabric folder information for a workspace.

        Args:
            workspace_id: The Fabric workspace id

        Returns:
            FabricFolderCollection: Fabric workspace folder information

        Raises:
            RuntimeError: If the API call fails or response parsing fails
        """
        self.logger.info(f"Getting Fabric folder info for workspace {workspace_id}")
        try:
            response = self.http_retry.execute(
                requests.get,
                f"{self.common_params.endpoint.analysis_service}/metadata/relations/folder/{workspace_id}",
                headers={
                    "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                    "Content-Type": "application/json",
                    "X-Consuming-Feature": "ListView",
                },
                timeout=60,
            )
            folder_data = response.json()
            self.logger.debug(f"Fabric folder info raw response for {workspace_id}: {folder_data}")

            folder_data_snake_case = StringTransformer.convert_keys_to_snake_case(folder_data)

            fabric_folder = dacite.from_dict(
                data_class=FabricFolderCollection,
                data=folder_data_snake_case,
                config=dacite.Config(
                    check_types=False,
                    cast=[str, int, bool, float],
                ),
            )

            self.logger.info(f"Successfully retrieved Fabric folder info for workspace {workspace_id}")
            return fabric_folder

        except Exception as e:
            error_msg = f"Failed to get Fabric folder info for workspace '{workspace_id}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e
