# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging

import dacite
import requests

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    CommonParams,
    HttpRetryHandler,
    MwcScopedToken,
    MwcTokenClient,
)
from fabric_workspace_deployment.static.transformers import StringTransformer


class FabricMwcTokenClient(MwcTokenClient):
    """Concrete implementation of MwcTokenClient for Microsoft Fabric."""

    def __init__(
        self,
        common_params: CommonParams,
        az_cli: AzCli,
        http_retry_handler: HttpRetryHandler,
    ):
        """
        Initialize the Fabric MWC token client.

        Args:
            common_params: Common configuration parameters
            az_cli: Azure CLI instance for authentication
            http_retry_handler: HTTP retry handler for resilient API calls
        """
        super().__init__(common_params)
        self.az_cli = az_cli
        self.http_retry = http_retry_handler
        self.logger = logging.getLogger(__name__)

    async def get_kusto_database_mwc_token(self, workspace_id: str, database_id: str) -> MwcScopedToken:
        self.logger.info(f"Getting MWC token for database {database_id} in workspace {workspace_id}")
        try:
            response = self.http_retry.execute(
                requests.post,
                f"{self.common_params.endpoint.analysis_service}/metadata/v201606/generatemwctokenv2",
                headers={
                    "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                    "Content-Type": "application/json",
                },
                json={
                    "workloadType": "Kusto",
                    "workspaceObjectId": workspace_id,
                    "artifacts": [{"artifactType": "KustoDatabase", "artifactObjectId": database_id}],
                },
                timeout=60,
            )
            token_data = response.json()
            token_data_snake_case = StringTransformer.convert_keys_to_snake_case(token_data)
            mwc_token = dacite.from_dict(
                data_class=MwcScopedToken,
                data=token_data_snake_case,
                config=dacite.Config(
                    check_types=False,
                    cast=[str, int, bool, float],
                ),
            )

            self.logger.info(f"Successfully retrieved MWC token for database {database_id}")
            return mwc_token

        except Exception as e:
            error_msg = f"Failed to get MWC token for database '{database_id}' in workspace '{workspace_id}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def get_spark_core_mwc_token(self, workspace_id: str, capacity_id: str) -> MwcScopedToken:
        self.logger.info(f"Getting MWC token for workspace: {workspace_id}, capacity: {capacity_id}")
        normalized_capacity_id = capacity_id.lower().replace("-", "")
        url = f"{self.common_params.endpoint.analysis_service}/metadata/v201606/generatemwctoken"

        headers = {
            "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
            "Content-Type": "application/json",
        }

        payload = {
            "type": "[Start] GetMWCToken",
            "workloadType": "SparkCore",
            "workspaceObjectId": workspace_id,
            "capacityObjectId": normalized_capacity_id,
        }

        try:
            response = self.http_retry.execute(
                requests.post,
                url,
                headers=headers,
                json=payload,
            )

            response_data = response.json()

            if "Token" not in response_data:
                error_msg = f"MWC token not found in response: {response_data}"
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)

            mwc_token = response_data["Token"]
            target_uri_host = f"{normalized_capacity_id}.pbidedicated.windows.net"

            self.logger.info(f"Successfully obtained MWC token for workspace: {workspace_id}")

            return MwcScopedToken(
                token=mwc_token,
                target_uri_host=target_uri_host,
                capacity_object_id=normalized_capacity_id,
            )

        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to get MWC token for workspace {workspace_id}: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e
