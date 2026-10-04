# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import base64
import json
from types import SimpleNamespace

import pytest
import requests
from dataclasses import replace

from fabric_workspace_deployment.client.fabric_gateway import FabricGatewayClient
from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.fabric.gateway import FabricGatewayManager
from fabric_workspace_deployment.operations.operation_interfaces import CommonParams, FabricParams, GatewayClient, GatewayConnection, GatewayParams, GatewayRole, GatewayUser, GatewayUserParams, Identity, PrincipalType

CONNECTION_ID = "11111111-1111-1111-1111-111111111111"
CLUSTER_ID = "22222222-2222-2222-2222-222222222222"
GROUP_ID = "33333333-3333-3333-3333-333333333333"
OWNER_ID = "44444444-4444-4444-4444-444444444444"


def _gateway():
    return GatewayParams("example", CONNECTION_ID, CLUSTER_ID, "Example", [GatewayUserParams("owner", GatewayRole.OWNER, "Read")])


class FakeHttp:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def execute(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        data = self.responses.pop(0)
        return SimpleNamespace(json=lambda: data)


class FakeAz:
    def get_access_token(self, scope):
        return "test-token"


def _client(responses, az=None):
    common = SimpleNamespace(endpoint=SimpleNamespace(power_bi="https://powerbi.example.invalid/"), scope=SimpleNamespace(analysis_service="https://analysis.windows.net/powerbi/api"))
    http = FakeHttp(responses)
    return FabricGatewayClient(common, az or FakeAz(), http), http


def _record(**overrides):
    return {"id": CONNECTION_ID, "clusterId": CLUSTER_ID, "datasourceName": "Old name", **overrides}


@pytest.mark.parametrize("envelope", [True, False])
def test_discovery_uses_stable_guid_and_checks_cluster(envelope):
    data = [_record()]
    client, http = _client([{"value": data} if envelope else data])
    connection = asyncio.run(client.get_connection(_gateway()))
    assert connection.id == CONNECTION_ID
    assert connection.datasource_name == "Old name"
    assert http.calls[0][1].endswith("/v2.0/myorg/me/gatewayClusterDatasources?$expand=users")


@pytest.mark.parametrize("data", [{"value": []}, {"value": [_record(), _record()]}, {"value": [_record(clusterId=GROUP_ID)]}, {}, {"value": "secret"}])
def test_missing_ambiguous_wrong_cluster_and_unknown_envelopes_fail(data):
    client, _ = _client([data])
    with pytest.raises(RuntimeError):
        asyncio.run(client.get_connection(_gateway()))


def test_rename_does_not_clear_connection_properties():
    client, http = _client([None])
    asyncio.run(client.rename(_gateway()))
    assert http.calls[0][0] is requests.patch
    assert http.calls[0][2]["json"] == {"datasourceName": "Example"}


def test_read_add_and_remove_use_captured_users_contract():
    client, http = _client([{"value": [{"identifier": GROUP_ID, "principalType": "Group", "role": "User", "datasourceAccessRight": "Read"}]}, None, None])
    user = asyncio.run(client.list_users(_gateway()))[0]
    asyncio.run(client.add_user(_gateway(), user))
    asyncio.run(client.delete_user(_gateway(), GatewayUser("reader@example.invalid", "User", "User", "Read")))
    assert http.calls[1][2]["json"] == {"identifier": GROUP_ID, "datasourceAccessRight": "Read", "emailAddress": None, "role": "User"}
    assert http.calls[2][0] is requests.delete
    assert http.calls[2][1].endswith("/users/reader%40example.invalid")
    assert http.calls[2][2]["safe_log_context"] == "Gateway direct access removal"


@pytest.mark.parametrize("data", [{}, {"value": None}, {"value": [{"identifier": GROUP_ID, "principalType": "Group", "role": "User"}]}])
def test_malformed_users_never_become_empty_current_access(data):
    client, _ = _client([data])
    with pytest.raises(RuntimeError):
        asyncio.run(client.list_users(_gateway()))


def _token(claims):
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"header.{payload}.signature"


def test_owner_caller_uses_fab_token_not_the_azure_cli_session(monkeypatch):
    other_id = "55555555-5555-5555-5555-555555555555"
    monkeypatch.setenv("FAB_TOKEN", _token({"oid": OWNER_ID, "upn": "owner@example.invalid"}))
    az = AzCli()
    commands = []

    def run_command(command, timeout):
        commands.append(command)
        return _token({"oid": other_id, "upn": "azure-session@example.invalid"})

    monkeypatch.setattr(az, "run_command", run_command)
    client, _ = _client([], az)
    assert client.get_caller_identifiers() == frozenset({OWNER_ID, "owner@example.invalid"})
    assert commands == []
    assert az.get_claim("oid") == other_id
    assert len(commands) == 1


@pytest.mark.parametrize("token", ["malformed-secret", "header.e30.signature", "header.W10.signature", "header.!!invalid.signature"])
def test_caller_claim_errors_never_expose_token_contents(monkeypatch, token):
    monkeypatch.setenv("FAB_TOKEN", token)
    client, _ = _client([], AzCli())
    with pytest.raises(RuntimeError) as error:
        client.get_caller_identifiers()
    assert token not in str(error.value)


class StatefulGatewayClient(GatewayClient):
    def __init__(self, users, name="Example", apply_changes=True):
        self.users = list(users)
        self.name = name
        self.apply_changes = apply_changes
        self.writes = []
        self.caller = frozenset({OWNER_ID, "owner@example.invalid"})

    async def get_connection(self, gateway):
        return GatewayConnection(gateway.connection_id, gateway.gateway_cluster_id, self.name)

    async def list_users(self, gateway):
        return list(self.users)

    async def rename(self, gateway):
        self.writes.append(("rename", gateway.display_name))
        if self.apply_changes:
            self.name = gateway.display_name

    async def add_user(self, gateway, user):
        self.writes.append(("add", user))
        if self.apply_changes:
            self.users.append(user)

    async def delete_user(self, gateway, user):
        self.writes.append(("delete", user))
        if self.apply_changes:
            self.users = [entry for entry in self.users if entry.key != user.key]

    def get_caller_identifiers(self):
        return self.caller


def _owner():
    return GatewayUser("owner@example.invalid", "User", "Owner", "Read", OWNER_ID)


def _manager(users, *, name="Example", preview=False, readers=False, apply_changes=True):
    gateway = _gateway()
    gateway.dry_run = preview
    if readers:
        gateway.users.append(GatewayUserParams("readers", GatewayRole.USER, "Read"))
    common = CommonParams(local=SimpleNamespace(), endpoint=SimpleNamespace(), scope=SimpleNamespace(), arm=SimpleNamespace(), fabric=FabricParams([], [], [gateway]), identities=[Identity("owner", OWNER_ID, PrincipalType.USER, user_principal_name="owner@example.invalid"), Identity("readers", GROUP_ID, PrincipalType.GROUP)])
    client = StatefulGatewayClient(users, name, apply_changes)
    return FabricGatewayManager(common, client), client, gateway


def test_gateway_reconciliation_noop_and_rename_only():
    manager, client, gateway = _manager([_owner()])
    asyncio.run(manager.reconcile(gateway))
    assert client.writes == []
    client.name = "Old name"
    asyncio.run(manager.reconcile(gateway))
    assert client.writes == [("rename", "Example")]


def test_gateway_add_only_and_second_run_noop():
    manager, client, gateway = _manager([_owner()], readers=True)
    asyncio.run(manager.reconcile(gateway))
    assert [action for action, _ in client.writes] == ["add"]
    assert client.users[-1].identifier == GROUP_ID
    client.writes.clear()
    asyncio.run(manager.reconcile(gateway))
    assert client.writes == []


def test_gateway_remove_only_and_mixed_changes_preserve_owner():
    old = GatewayUser("55555555-5555-5555-5555-555555555555", "Group", "User", "Read")
    manager, client, gateway = _manager([_owner(), old])
    asyncio.run(manager.reconcile(gateway))
    assert client.writes == [("delete", old)]
    manager, client, gateway = _manager([_owner(), old], name="Old name", readers=True)
    asyncio.run(manager.reconcile(gateway))
    assert [action for action, _ in client.writes] == ["add", "delete", "rename"]
    assert {user.key for user in client.users} == {_owner().key, ("group", GROUP_ID)}


def test_gateway_permission_replacement_is_delete_add():
    reader = GatewayUser(GROUP_ID, "Group", "Owner", "Read")
    manager, client, gateway = _manager([_owner(), reader], readers=True)
    asyncio.run(manager.reconcile(gateway))
    assert [action for action, _ in client.writes] == ["delete", "add"]
    assert client.users[-1].role == "User"


def test_gateway_preview_never_renames_or_modifies_access(caplog):
    old = GatewayUser("55555555-5555-5555-5555-555555555555", "Group", "User", "Read")
    manager, client, gateway = _manager([_owner(), old], name="Old name", readers=True, preview=True)
    caplog.set_level("INFO")
    asyncio.run(manager.reconcile(gateway))
    assert client.writes == []
    assert "rename=True, add=1, replace=0, remove=1, dryRun=True" in caplog.text
    assert OWNER_ID not in caplog.text
    assert GROUP_ID not in caplog.text
    assert "owner@example.invalid" not in caplog.text


@pytest.mark.parametrize("preview", [False, True])
def test_gateway_explicit_caller_owner_removal_fails_before_any_write(preview):
    manager, client, gateway = _manager([_owner()], name="Old name", preview=preview)
    gateway.users = [GatewayUserParams("readers", GatewayRole.OWNER, "Read")]
    with pytest.raises(ValueError, match="deploying principal"):
        asyncio.run(manager.reconcile(gateway))
    assert client.writes == []


def test_gateway_ownerless_config_fails_before_reads_or_writes():
    manager, client, gateway = _manager([_owner()], readers=True)
    gateway.users = [GatewayUserParams("readers", GatewayRole.USER, "Read")]
    with pytest.raises(ValueError, match="at least one Owner"):
        asyncio.run(manager.reconcile(gateway))
    assert client.writes == []


def test_new_owner_is_added_before_old_owner_is_removed():
    old = GatewayUser("55555555-5555-5555-5555-555555555555", "Group", "Owner", "Read")
    manager, client, gateway = _manager([old])
    asyncio.run(manager.reconcile(gateway))
    assert [action for action, _ in client.writes] == ["add", "delete"]
    assert client.writes[0][1].role == "Owner"


def test_replacing_only_owner_permission_fails_before_any_write():
    owner = GatewayUser(GROUP_ID, "Group", "Owner", "ReadOverrideEffectiveIdentity")
    manager, client, gateway = _manager([owner], name="Old")
    gateway.users = [GatewayUserParams("readers", GatewayRole.OWNER, "Read")]
    with pytest.raises(ValueError, match="only current Owner"):
        asyncio.run(manager.reconcile(gateway))
    assert client.writes == []


def test_mixed_gateway_preview_and_apply_flags_are_independent():
    manager, client, gateway = _manager([_owner()], name="Old", preview=True)
    second = replace(gateway, name="second", connection_id=GROUP_ID, dry_run=False)
    manager.common_params.fabric.gateways.append(second)
    asyncio.run(manager.execute())
    assert client.writes == [("rename", "Example")]


def test_gateway_failed_final_state_is_an_error(monkeypatch):
    monkeypatch.setattr("fabric_workspace_deployment.client.fabric_rest.VERIFY_DELAY_SECONDS", 0)
    manager, client, gateway = _manager([_owner()], name="Old", apply_changes=False)
    with pytest.raises(RuntimeError, match="did not converge"):
        asyncio.run(manager.reconcile(gateway))


def test_gateway_manager_honors_execution_skip(monkeypatch):
    manager, client, _ = _manager([_owner()], name="Old")
    monkeypatch.setenv("FAB_SKIP_GATEWAY_DEPLOYMENT", "1")
    asyncio.run(manager.execute())
    assert client.writes == []
