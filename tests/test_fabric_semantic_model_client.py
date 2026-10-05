# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging
import json
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import asyncio
import pytest
import requests

from fabric_workspace_deployment.client.fabric_semantic_model import FabricSemanticModelClient
from fabric_workspace_deployment.operations.operation_interfaces import HttpRetryHandler, ModelSourceItemParams, RlsRoleDelta, SqlEndpointIdentity

CONNECTION_ID = "11111111-1111-1111-1111-111111111111"
CLUSTER_ID = "22222222-2222-2222-2222-222222222222"
GROUP_ID = "33333333-3333-3333-3333-333333333333"
MONIKER = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
WORKSPACE_ID = "44444444-4444-4444-4444-444444444444"
SQL_ENDPOINT_ID = "55555555-5555-5555-5555-555555555555"
SQL_SERVER = "example.datawarehouse.fabric.microsoft.com"
SQL_NAME = "Example SQL endpoint"


class FakeHttp:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def execute(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        value = self.responses.pop(0)

        def json():
            if value is None:
                pytest.fail("An empty mutation response must not be decoded")
            return value

        return SimpleNamespace(json=json)


class FakeAz:
    def get_access_token(self, scope):
        assert scope == "https://analysis.windows.net/powerbi/api"
        return "test-bearer-token"


def _client(responses):
    common = SimpleNamespace(endpoint=SimpleNamespace(analysis_service="https://analysis.example.invalid/", cicd="https://fabric.example.invalid/"), scope=SimpleNamespace(analysis_service="https://analysis.windows.net/powerbi/api"))
    http = FakeHttp(responses)
    return FabricSemanticModelClient(common, FakeAz(), http), http


def _bindings():
    return {
        "monikers": [{"moniker": MONIKER, "monikerGateways": [{"gatewayObjectId": CLUSTER_ID}], "monikerDataSources": [{"dataSourceObjectId": CONNECTION_ID, "matchingDataSourceObjectIds": [CONNECTION_ID], "dataSourceReference": json.dumps({"kind": "SQL", "path": f"{SQL_SERVER};{SQL_NAME}"})}]}],
        "datasources": [{"dataSourceObjectId": CONNECTION_ID, "gatewayObjectId": CLUSTER_ID}],
    }


def test_binding_read_uses_internal_model_id_and_env_first_scope():
    client, http = _client([_bindings()])
    state = asyncio.run(client.get_bindings(42))
    assert state.monikers[0].candidate_ids == frozenset({CONNECTION_ID})
    assert state.monikers[0].connection_ids == frozenset({CONNECTION_ID})
    assert state.monikers[0].sql_identities == frozenset({(SQL_SERVER, SQL_NAME.casefold())})
    assert http.calls[0][1] == "https://analysis.example.invalid/metadata/datamovement/models/42/dataSources?api-version=10.0"
    assert http.calls[0][2]["headers"]["Authorization"] == "Bearer test-bearer-token"
    assert http.calls[0][2]["safe_log_context"] == "Model binding read"


def _source_item(**overrides):
    return {"id": SQL_ENDPOINT_ID, "type": "SQLEndpoint", "displayName": SQL_NAME, **overrides}


@pytest.mark.parametrize(
    "connection_string,databases",
    [
        (f"TCP:{SQL_SERVER.upper()}.,1433", {SQL_ENDPOINT_ID, SQL_NAME.casefold()}),
        (f"Server=tcp:{SQL_SERVER.upper()},1433;Database=Explicit Database;User ID=private-user;Password=private-password", {SQL_ENDPOINT_ID, SQL_NAME.casefold(), "explicit database"}),
        (f'Data Source={SQL_SERVER};Initial Catalog="Explicit Database";Encrypt=True', {SQL_ENDPOINT_ID, SQL_NAME.casefold(), "explicit database"}),
    ],
)
def test_source_item_resolves_single_exact_workspace_endpoint(connection_string, databases, caplog):
    client, http = _client([{"value": [_source_item(type="SqlEndpoint"), _source_item(displayName=SQL_NAME.lower()), _source_item()]}, {"connectionString": connection_string}])
    caplog.set_level(logging.DEBUG)
    identity = asyncio.run(client.resolve_source_item(WORKSPACE_ID, ModelSourceItemParams("SqlEndpoint", SQL_NAME)))
    assert identity == SqlEndpointIdentity(SQL_SERVER, frozenset(databases))
    assert len(http.calls) == 2
    assert http.calls[0][0] is requests.get
    assert http.calls[0][1] == f"https://fabric.example.invalid/v1/workspaces/{WORKSPACE_ID}/items?type=SQLEndpoint&recursive=true"
    assert http.calls[1][0] is requests.get
    assert http.calls[1][1] == f"https://fabric.example.invalid/v1/workspaces/{WORKSPACE_ID}/sqlEndpoints/{SQL_ENDPOINT_ID}/connectionString"
    assert http.calls[0][2]["safe_log_context"] == "Source item inventory"
    assert http.calls[1][2]["safe_log_context"] == "Source SQL endpoint identity"
    assert all(call[2]["headers"]["Authorization"] == "Bearer " + FakeAz().get_access_token("https://analysis.windows.net/powerbi/api") for call in http.calls)
    assert connection_string not in caplog.text
    assert "private-password" not in caplog.text


@pytest.mark.parametrize("items", [[], [_source_item(), _source_item(id=GROUP_ID)], [_source_item(type="Lakehouse"), _source_item(displayName=SQL_NAME.lower())]])
def test_source_item_missing_or_duplicate_fails_before_connection_read(items):
    client, http = _client([{"value": items}])
    with pytest.raises(RuntimeError, match="zero or multiple"):
        asyncio.run(client.resolve_source_item(WORKSPACE_ID, ModelSourceItemParams("SqlEndpoint", SQL_NAME)))
    assert len(http.calls) == 1


def test_source_item_lookup_follows_encoded_continuation_token():
    token = "next+page/with=padding"
    client, http = _client([{"value": [], "continuationToken": token}, {"value": [_source_item()]}, {"connectionString": SQL_SERVER}])
    identity = asyncio.run(client.resolve_source_item(WORKSPACE_ID, ModelSourceItemParams("SqlEndpoint", SQL_NAME)))
    assert identity == SqlEndpointIdentity(SQL_SERVER, frozenset({SQL_ENDPOINT_ID, SQL_NAME.casefold()}))
    query = parse_qs(urlparse(http.calls[1][1]).query)
    assert query == {"type": ["SQLEndpoint"], "recursive": ["true"], "continuationToken": [token]}
    assert len(http.calls) == 3


def test_source_item_duplicate_across_pages_fails_before_connection_read():
    client, http = _client([{"value": [_source_item()], "continuationToken": "next"}, {"value": [_source_item(id=GROUP_ID)]}])
    with pytest.raises(RuntimeError, match="zero or multiple"):
        asyncio.run(client.resolve_source_item(WORKSPACE_ID, ModelSourceItemParams("SqlEndpoint", SQL_NAME)))
    assert len(http.calls) == 2


def test_source_item_repeated_continuation_token_fails_bounded_lookup():
    client, http = _client([{"value": [], "continuationToken": "next"}, {"value": [], "continuationToken": "next"}])
    with pytest.raises(RuntimeError, match="repeated a continuation token"):
        asyncio.run(client.resolve_source_item(WORKSPACE_ID, ModelSourceItemParams("SqlEndpoint", SQL_NAME)))
    assert len(http.calls) == 2


@pytest.mark.parametrize(
    "responses",
    [
        [{"value": "private-metadata"}],
        [{"value": [_source_item(id="private-metadata")]}],
        [{"value": [_source_item()], "continuationToken": 42}],
        [{"value": [_source_item()]}, {"connectionString": ""}],
        [{"value": [_source_item()]}, {"connectionString": 42}],
        [{"value": [_source_item()]}, {"connectionString": "Database=private-metadata;Password=private-password"}],
    ],
)
def test_source_item_invalid_metadata_errors_are_sanitized(responses, caplog):
    client, _ = _client(responses)
    caplog.set_level(logging.DEBUG)
    with pytest.raises(RuntimeError) as error:
        asyncio.run(client.resolve_source_item(WORKSPACE_ID, ModelSourceItemParams("SqlEndpoint", SQL_NAME)))
    assert "private-metadata" not in str(error.value)
    assert "private-password" not in str(error.value)
    assert "private-metadata" not in caplog.text
    assert "private-password" not in caplog.text


@pytest.mark.parametrize("location", ["moniker", "monikerDataSources", "datasources"])
@pytest.mark.parametrize(
    "metadata",
    [
        {"connectionDetails": {"server": f"TCP:{SQL_SERVER.upper()},1433", "database": f" {SQL_NAME} "}},
        {"connectionDetails": json.dumps({"server": SQL_SERVER, "database": SQL_NAME}), "dataSourceType": "SQL"},
        {"dataSourceReference": {"kind": "SQL", "path": f"{SQL_SERVER};{SQL_NAME}"}},
        {"dataSourceReference": json.dumps({"kind": "SQL", "path": f"tcp:{SQL_SERVER.upper()},1433;{SQL_NAME}"})},
    ],
)
def test_binding_read_parses_sql_identity_from_all_supported_records(location, metadata):
    data = _bindings()
    data["monikers"][0]["monikerDataSources"][0].pop("dataSourceReference")
    if location == "moniker":
        data["monikers"][0].update(metadata)
    elif location == "monikerDataSources":
        data["monikers"][0]["monikerDataSources"][0].update(metadata)
    else:
        data["datasources"][0].update(metadata)
    client, _ = _client([data])
    state = asyncio.run(client.get_bindings(42))
    assert state.monikers[0].sql_identities == frozenset({(SQL_SERVER, SQL_NAME.casefold())})


def test_binding_read_does_not_attach_unbound_datasource_identity():
    data = _bindings()
    data["datasources"].append({"dataSourceObjectId": GROUP_ID, "gatewayObjectId": CLUSTER_ID, "connectionDetails": {"server": "other.example.invalid", "database": "Other"}})
    client, _ = _client([data])
    state = asyncio.run(client.get_bindings(42))
    assert state.monikers[0].sql_identities == frozenset({(SQL_SERVER, SQL_NAME.casefold())})


@pytest.mark.parametrize(
    "metadata",
    [
        {"connectionDetails": "private-metadata"},
        {"dataSourceReference": "private-metadata"},
        {"dataSourceReference": ["private-metadata"]},
        {"dataSourceReference": {"kind": "SQL", "path": "private-metadata"}},
        {"dataSourceReference": {"kind": "SQL", "path": ";private-metadata"}},
        {"dataSourceReference": {"kind": "SQL", "path": "private-metadata;"}},
    ],
)
@pytest.mark.parametrize("location", ["monikerDataSources", "datasources"])
def test_binding_read_invalid_sql_metadata_is_sanitized(metadata, location, caplog):
    data = _bindings()
    source = data["monikers"][0]["monikerDataSources"][0] if location == "monikerDataSources" else data["datasources"][0]
    source.update(metadata)
    client, _ = _client([data])
    caplog.set_level(logging.DEBUG)
    with pytest.raises(RuntimeError) as error:
        asyncio.run(client.get_bindings(42))
    assert "private-metadata" not in str(error.value)
    assert "private-metadata" not in caplog.text


@pytest.mark.parametrize("data", [{}, {"monikers": [], "datasources": None}, {"monikers": "secret", "datasources": []}])
def test_binding_read_does_not_default_malformed_state_to_empty(data):
    client, http = _client([data])
    with pytest.raises(RuntimeError):
        asyncio.run(client.get_bindings(42))
    assert len(http.calls) == 1


def test_binding_write_uses_captured_payload():
    client, http = _client([{"status": 1, "errors": []}])
    asyncio.run(client.bind(42, CLUSTER_ID, {MONIKER: CONNECTION_ID}))
    assert http.calls[0][0] is requests.post
    assert http.calls[0][1].endswith("/42/explicitBind?api-version=10.0")
    assert http.calls[0][2]["json"] == {"gatewayObjectId": CLUSTER_ID, "monikerBindings": [{"moniker": MONIKER, "dataSourceObjectId": CONNECTION_ID, "boundVirtualConnectionType": None}]}


@pytest.mark.parametrize("data", [{"status": 0, "errors": []}, {"status": 1, "errors": ["secret identity"]}, {"status": True, "errors": []}, {"status": 1}])
def test_http_success_is_not_binding_business_success(data):
    client, _ = _client([data])
    with pytest.raises(RuntimeError) as error:
        asyncio.run(client.bind(42, CLUSTER_ID, {MONIKER: CONNECTION_ID}))
    assert "secret identity" not in str(error.value)


def test_rls_read_preserves_original_removal_member_data():
    member = {"objectId": GROUP_ID, "displayName": "secret group name", "objectType": 2, "groupType": 3, "isSecurityGroup": True}
    client, http = _client([{"roleMembershipsInformation": [{"id": 7, "name": "Role", "members": [member]}]}, None])
    roles = asyncio.run(client.get_rls_membership(42))
    assert roles[0].members[0].data == member
    asyncio.run(client.update_rls_membership(42, [RlsRoleDelta(7, "Role", [], [roles[0].members[0].data])]))
    assert http.calls[1][2]["json"] == {"roleMemberships": [{"id": 7, "name": "Role", "addedMembers": [], "removedMembers": [member]}]}
    assert http.calls[1][1].endswith("/metadata/model/42/rlsmembership/")


def test_rls_requires_valid_members_and_nonempty_deltas():
    client, _ = _client([{"roleMembershipsInformation": [{"id": 7, "name": "Role", "members": [{"objectId": "bad-secret"}]}]}])
    with pytest.raises(RuntimeError, match="GUID"):
        asyncio.run(client.get_rls_membership(42))
    with pytest.raises(ValueError, match="non-empty"):
        asyncio.run(client.update_rls_membership(42, []))
    with pytest.raises(ValueError, match="non-empty"):
        asyncio.run(client.update_rls_membership(42, [RlsRoleDelta(7, "Role", [], [])]))


@pytest.mark.parametrize("model_id", [0, -1, True, "model-guid"])
def test_internal_model_identifier_is_a_positive_integer(model_id):
    client, http = _client([])
    with pytest.raises(RuntimeError, match="Internal model ID"):
        asyncio.run(client.get_bindings(model_id))
    assert http.calls == []


@pytest.mark.parametrize("status", [403, 429])
def test_safe_retry_context_redacts_response_exception_and_url(status, caplog):
    secret = "private-person@example.invalid"
    response = requests.Response()
    response.status_code = status
    response.url = f"https://api.example.invalid/users/{secret}"
    response._content = f"private bearer token and {secret}".encode()

    def request(url, **kwargs):
        return response

    retry = HttpRetryHandler(max_attempts=2, initial_delay_seconds=0, logger=logging.getLogger("safe-retry"))
    caplog.set_level(logging.DEBUG)
    with pytest.raises(RuntimeError) as error:
        retry.execute(request, response.url, safe_log_context="Gateway removal")
    assert secret not in caplog.text
    assert secret not in str(error.value)
    assert "bearer token" not in caplog.text
    assert error.value.__suppress_context__
    assert f"HTTP {status}" in str(error.value)


def test_safe_network_retry_does_not_expose_exception_details(caplog):
    secret = "private-principal-and-token"

    def request(url, **kwargs):
        raise requests.Timeout(secret)

    retry = HttpRetryHandler(max_attempts=2, initial_delay_seconds=0)
    caplog.set_level(logging.DEBUG)
    with pytest.raises(RuntimeError) as error:
        retry.execute(request, f"https://example.invalid/{secret}", safe_log_context="RLS read")
    assert secret not in caplog.text
    assert secret not in str(error.value)


def test_legacy_retry_still_raises_the_original_http_error():
    response = requests.Response()
    response.status_code = 403
    response._content = b"legacy response"
    with pytest.raises(requests.HTTPError):
        HttpRetryHandler().execute(lambda url: response, "https://example.invalid")
