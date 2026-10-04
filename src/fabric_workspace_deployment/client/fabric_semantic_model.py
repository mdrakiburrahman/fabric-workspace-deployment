# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import requests

from fabric_workspace_deployment.client.fabric_rest import FabricRestClient, response_array, response_guid, response_integer, response_object, response_string
from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import CommonParams, HttpRetryHandler, ModelBindingState, ModelDatasourceBinding, ModelDatasourceCandidate, RlsMember, RlsRoleDelta, RlsRoleMembership, SemanticModelClient


class FabricSemanticModelClient(FabricRestClient, SemanticModelClient):
    """Captured model datamovement and RLS membership contracts."""

    def __init__(self, common_params: CommonParams, az_cli: AzCli, http_retry_handler: HttpRetryHandler):
        super().__init__(common_params, az_cli, http_retry_handler, common_params.endpoint.analysis_service)

    def _binding_path(self, model_id: int, action: str) -> str:
        response_integer(model_id, "Internal model ID")
        return f"/metadata/datamovement/models/{model_id}/{action}?api-version=10.0"

    async def get_bindings(self, model_id: int) -> ModelBindingState:
        data = response_object(self._get_json(self._binding_path(model_id, "dataSources"), "Model binding read"), "Model binding response")
        monikers = []
        for index, record in enumerate(response_array(data.get("monikers"), "Model binding monikers")):
            entry = response_object(record, f"Model monikers[{index}]")
            moniker = response_guid(entry.get("moniker"), "Model datasource moniker")
            gateway_ids = frozenset(response_guid(response_object(gateway, "Moniker gateway").get("gatewayObjectId"), "Moniker gatewayObjectId").casefold() for gateway in response_array(entry.get("monikerGateways"), "Moniker gateways"))
            connection_ids: set[str] = set()
            candidate_ids: set[str] = set()
            for datasource in response_array(entry.get("monikerDataSources"), "Moniker datasources"):
                source = response_object(datasource, "Moniker datasource")
                bound_id = source.get("dataSourceObjectId")
                if bound_id is not None:
                    connection_ids.add(response_guid(bound_id, "Moniker dataSourceObjectId").casefold())
                    candidate_ids.add(response_guid(bound_id, "Moniker dataSourceObjectId").casefold())
                candidate_ids.update(response_guid(candidate, "Moniker matching datasource ID").casefold() for candidate in response_array(source.get("matchingDataSourceObjectIds"), "Moniker matchingDataSourceObjectIds"))
            if len(gateway_ids) > 1 or len(connection_ids) > 1:
                raise RuntimeError("Model datasource moniker has an ambiguous current binding")
            monikers.append(ModelDatasourceBinding(moniker, gateway_ids, frozenset(connection_ids), frozenset(candidate_ids)))
        if len({entry.moniker.casefold() for entry in monikers}) != len(monikers):
            raise RuntimeError("Model binding response contains duplicate datasource monikers")
        datasources = []
        for record in response_array(data.get("datasources"), "Model binding datasources"):
            source = response_object(record, "Model candidate datasource")
            datasources.append(ModelDatasourceCandidate(response_guid(source.get("dataSourceObjectId"), "Model candidate dataSourceObjectId"), response_guid(source.get("gatewayObjectId"), "Model candidate gatewayObjectId")))
        return ModelBindingState(monikers, datasources)

    async def bind(self, model_id: int, gateway_cluster_id: str, bindings: dict[str, str]) -> None:
        response_guid(gateway_cluster_id, "Configured gateway cluster")
        if not bindings:
            raise ValueError("Model binding updates require at least one moniker")
        for moniker, connection_id in bindings.items():
            response_guid(moniker, "Configured model datasource moniker")
            response_guid(connection_id, "Configured model connection")
        payload = {"gatewayObjectId": gateway_cluster_id, "monikerBindings": [{"moniker": moniker, "dataSourceObjectId": connection_id, "boundVirtualConnectionType": None} for moniker, connection_id in bindings.items()]}
        response = self._request(requests.post, self._binding_path(model_id, "explicitBind"), "Model explicit binding", json=payload)
        try:
            data = response_object(response.json(), "Model explicit binding response")
        except ValueError:
            raise RuntimeError("Model explicit binding response is not valid JSON") from None
        errors = response_array(data.get("errors"), "Model explicit binding errors")
        if type(data.get("status")) is not int or data["status"] != 1 or errors:
            raise RuntimeError("Model explicit binding reported an unsuccessful business status")

    def _rls_path(self, model_id: int) -> str:
        response_integer(model_id, "Internal model ID")
        return f"/metadata/model/{model_id}/rlsmembership/"

    async def get_rls_membership(self, model_id: int) -> list[RlsRoleMembership]:
        data = response_object(self._get_json(self._rls_path(model_id), "Model RLS read"), "Model RLS response")
        roles = []
        for index, record in enumerate(response_array(data.get("roleMembershipsInformation"), "RLS roleMembershipsInformation")):
            entry = response_object(record, f"RLS roles[{index}]")
            members = []
            for record_member in response_array(entry.get("members"), "RLS role members"):
                member = response_object(record_member, "RLS member")
                members.append(RlsMember(response_guid(member.get("objectId"), "RLS member objectId"), dict(member)))
            if len({member.object_id.casefold() for member in members}) != len(members):
                raise RuntimeError("RLS role contains duplicate member object IDs")
            roles.append(RlsRoleMembership(response_integer(entry.get("id"), "RLS role ID"), response_string(entry.get("name"), "RLS role name"), members))
        if len({role.id for role in roles}) != len(roles):
            raise RuntimeError("RLS response contains duplicate role IDs")
        return roles

    async def update_rls_membership(self, model_id: int, deltas: list[RlsRoleDelta]) -> None:
        if not deltas or any(not delta.added_members and not delta.removed_members for delta in deltas):
            raise ValueError("RLS updates require non-empty membership deltas")
        self._request(requests.post, self._rls_path(model_id), "Model RLS reconciliation", json={"roleMemberships": [{"id": delta.id, "name": delta.name, "addedMembers": delta.added_members, "removedMembers": delta.removed_members} for delta in deltas]})
