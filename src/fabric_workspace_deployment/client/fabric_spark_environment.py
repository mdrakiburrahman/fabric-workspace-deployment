# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging

import requests
import yaml

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    CommonParams,
    HttpRetryHandler,
    MwcTokenClient,
    SparkEnvironmentClient,
)


class FabricSparkEnvironmentClient(SparkEnvironmentClient):
    """Concrete SparkEnvironmentClient using MWC token + SparkCore internal API."""

    def __init__(
        self,
        common_params: CommonParams,
        mwc_token_client: MwcTokenClient,
        http_retry_handler: HttpRetryHandler,
        az_cli: AzCli,
    ):
        super().__init__(common_params)
        self.mwc_token_client = mwc_token_client
        self.http_retry = http_retry_handler
        self.az_cli = az_cli
        self.logger = logging.getLogger(__name__)

    def _build_sparkcore_base_url(self, capacity_id: str) -> str:
        """Build the SparkCore service base URL from a capacity ID."""
        capacity_id_no_dashes = capacity_id.lower().replace("-", "")
        capacity_id_upper = capacity_id.upper()
        return f"https://{capacity_id_no_dashes}.pbidedicated.windows.net" f"/webapi/capacities/{capacity_id_upper}" f"/workloads/SparkCore/SparkCoreService/automatic/v1"

    def _convert_yaml_to_sparkcore_payload(self, yaml_body: dict) -> dict:
        """
        Convert a parsed Sparkcompute.yml dict to the SparkCore PUT payload format.

        Key transformations:
        - snake_case keys -> camelCase
        - spark_conf dict stays as a flat dict (NOT array of {key,value})
        - instance_pool_id -> instancePoolId (string)
        - Add sparkEnvVar: {} and mountPoints: {} defaults if not present
        """
        payload = {}

        for key, value in yaml_body.items():
            if key == "spark_conf":
                payload["sparkConf"] = value if isinstance(value, dict) else {}
            elif key == "instance_pool_id":
                payload["instancePoolId"] = str(value)
            else:
                parts = key.split("_")
                camel_key = parts[0] + "".join(p.title() for p in parts[1:])

                if isinstance(value, dict):
                    payload[camel_key] = self._convert_nested(value)
                else:
                    payload[camel_key] = value

        payload.setdefault("instancePoolId", "00000000-0000-0000-0000-000000000000")
        payload.setdefault("sparkConf", {})
        payload.setdefault("sparkEnvVar", {})
        payload.setdefault("mountPoints", {})
        payload.setdefault("enableNativeExecutionEngine", False)

        return payload

    def _convert_nested(self, d: dict) -> dict:
        """Convert a nested dict's keys from snake_case to camelCase."""
        result = {}
        for key, value in d.items():
            parts = key.split("_")
            camel_key = parts[0] + "".join(p.title() for p in parts[1:])
            result[camel_key] = value
        return result

    async def put_spark_settings(
        self,
        capacity_id: str,
        workspace_id: str,
        environment_artifact_id: str,
        sparkcompute_yaml_path: str,
    ) -> None:
        with open(sparkcompute_yaml_path, encoding="utf-8") as f:
            yaml_body = yaml.safe_load(f.read())

        payload = self._convert_yaml_to_sparkcore_payload(yaml_body)

        mwc_token = await self.mwc_token_client.get_spark_core_mwc_token(
            workspace_id=workspace_id,
            capacity_id=capacity_id,
            artifact_id=environment_artifact_id,
        )

        base_url = self._build_sparkcore_base_url(capacity_id)
        url = f"{base_url}/workspaces/{workspace_id}" f"/artifacts/{environment_artifact_id}/sparkSettings/content"

        self.http_retry.execute(
            requests.put,
            url,
            headers={
                "Authorization": f"mwctoken {mwc_token.token}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60,
        )
        self.logger.info(f"PUT spark settings for environment {environment_artifact_id} " f"in workspace {workspace_id}")

    async def publish_spark_settings(
        self,
        capacity_id: str,
        workspace_id: str,
        environment_artifact_id: str,
    ) -> None:
        url = f"{self.common_params.endpoint.power_bi}/v1/workspaces/{workspace_id}" f"/environments/{environment_artifact_id}/staging/publish?beta=False"

        self.http_retry.execute(
            requests.post,
            url,
            headers={
                "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                "Content-Type": "application/json",
            },
            json={},
            timeout=60,
        )
        self.logger.info(f"Published spark settings for environment {environment_artifact_id} " f"in workspace {workspace_id}")
