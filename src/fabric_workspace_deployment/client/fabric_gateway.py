# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

from urllib.parse import quote

import requests

from fabric_workspace_deployment.client.fabric_rest import FabricRestClient, response_array, response_guid, response_object, response_string
from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import CommonParams, GatewayClient, GatewayConnection, GatewayParams, GatewayUser, HttpRetryHandler


class FabricGatewayClient(FabricRestClient, GatewayClient):
    """Power BI transport for existing gateway/cloud connections."""

    def __init__(self, common_params: CommonParams, az_cli: AzCli, http_retry_handler: HttpRetryHandler):
        super().__init__(common_params, az_cli, http_retry_handler, common_params.endpoint.power_bi)

    def _path(self, gateway: GatewayParams) -> str:
        cluster = response_guid(gateway.gateway_cluster_id, "Configured gateway cluster")
        connection = response_guid(gateway.connection_id, "Configured connection")
        return f"/v2.0/myorg/me/gatewayClusters/{cluster}/datasources/{connection}"

    async def get_connection(self, gateway: GatewayParams) -> GatewayConnection:
        response_guid(gateway.connection_id, "Configured connection")
        response_guid(gateway.gateway_cluster_id, "Configured gateway cluster")
        data = self._get_json("/v2.0/myorg/me/gatewayClusterDatasources?$expand=users", "Gateway connection inventory")
        records = response_array(data if isinstance(data, list) else response_object(data, "Gateway inventory").get("value"), "Gateway inventory value")
        matches = []
        for index, record in enumerate(records):
            entry = response_object(record, f"Gateway inventory[{index}]")
            connection_id = response_guid(entry.get("id"), f"Gateway inventory[{index}].id")
            if connection_id.casefold() == gateway.connection_id.casefold():
                matches.append(GatewayConnection(connection_id, response_guid(entry.get("clusterId"), "Gateway inventory clusterId"), response_string(entry.get("datasourceName"), "Gateway inventory datasourceName")))
        if len(matches) != 1:
            raise RuntimeError("Configured gateway connection is missing or ambiguous in accessible connection inventory")
        if matches[0].cluster_id.casefold() != gateway.gateway_cluster_id.casefold():
            raise RuntimeError("Configured gateway connection belongs to a different cluster")
        return matches[0]

    async def list_users(self, gateway: GatewayParams) -> list[GatewayUser]:
        data = response_object(self._get_json(f"{self._path(gateway)}/users", "Gateway direct access read"), "Gateway users response")
        records = response_array(data.get("value"), "Gateway users value")
        users = []
        for index, record in enumerate(records):
            entry = response_object(record, f"Gateway users[{index}]")
            identifier = response_string(entry.get("identifier"), "Gateway user identifier")
            principal_type = response_string(entry.get("principalType"), "Gateway user principalType")
            if principal_type == "Group":
                response_guid(identifier, "Gateway group identifier")
            object_id = entry.get("objectId")
            if object_id is not None:
                object_id = response_guid(object_id, "Gateway user objectId")
            users.append(GatewayUser(identifier, principal_type, response_string(entry.get("role"), "Gateway user role"), response_string(entry.get("datasourceAccessRight"), "Gateway user datasourceAccessRight"), object_id))
        if len({user.key for user in users}) != len(users):
            raise RuntimeError("Gateway direct access contains duplicate principal identifiers")
        return users

    async def rename(self, gateway: GatewayParams) -> None:
        self._request(requests.patch, self._path(gateway), "Gateway connection rename", json={"datasourceName": gateway.display_name})

    async def add_user(self, gateway: GatewayParams, user: GatewayUser) -> None:
        self._request(requests.post, f"{self._path(gateway)}/users", "Gateway direct access addition", json={"identifier": user.identifier, "datasourceAccessRight": user.datasource_access_right, "emailAddress": None, "role": user.role})

    async def delete_user(self, gateway: GatewayParams, user: GatewayUser) -> None:
        self._request(requests.delete, f"{self._path(gateway)}/users/{quote(user.identifier, safe='')}", "Gateway direct access removal")

    def get_caller_identifiers(self) -> frozenset[str]:
        claims = self.az_cli.get_token_claims(self.common_params.scope.analysis_service)
        identifiers = {response_guid(claims.get("oid"), "Gateway caller token oid").casefold()}
        for name in ("upn", "preferred_username", "unique_name"):
            value = claims.get(name)
            if value is not None:
                identifiers.add(response_string(value, "Gateway caller token identity").casefold())
        return frozenset(identifiers)
