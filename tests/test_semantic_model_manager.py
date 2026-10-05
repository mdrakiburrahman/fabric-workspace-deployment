# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio

from pathlib import Path
from types import MethodType, SimpleNamespace

import pytest

from fabric_workspace_deployment.manager.fabric import model as model_module
from fabric_workspace_deployment.manager.fabric.model import SemanticModelManager
from fabric_workspace_deployment.operations.operation_interfaces import CommonParams, FabricParams, GatewayClient, GatewayConnection, GatewayParams, GatewayRole, GatewayUserParams, Identity, ModelBindingState, ModelConnectionParams, ModelDatasourceBinding, ModelDatasourceCandidate, ModelParams, PrincipalType, RlsMember, RlsRoleMembership, SemanticModelClient

CONNECTION_ID = "11111111-1111-1111-1111-111111111111"
CLUSTER_ID = "22222222-2222-2222-2222-222222222222"
GROUP_ID = "33333333-3333-3333-3333-333333333333"
OTHER_ID = "55555555-5555-5555-5555-555555555555"
MONIKER = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


def _common(root: Path):
    return SimpleNamespace(
        local=SimpleNamespace(root_folder=str(root)),
        endpoint=SimpleNamespace(
            cicd="https://api.fabric.microsoft.com",
            analysis_service="https://wabi.example.invalid",
        ),
        scope=SimpleNamespace(
            analysis_service="https://analysis.windows.net/powerbi/api",
        ),
    )


def _workspace_params():
    return SimpleNamespace(
        template=SimpleNamespace(
            artifacts_folder="artifacts",
            environment_key="dev",
            feature_flags=["configured-flag"],
        )
    )


def _model_artifact(model_id: int = 42):
    return SimpleNamespace(
        id=model_id,
        type_name="Model",
        display_name="insights",
    )


class FakeFolderClient:
    def __init__(self, artifact_sequences):
        self.artifact_sequences = list(artifact_sequences)
        self.calls = []

    async def get_fabric_folder_collection(self, workspace_id):
        self.calls.append(workspace_id)
        if len(self.artifact_sequences) > 1:
            artifacts = self.artifact_sequences.pop(0)
        else:
            artifacts = self.artifact_sequences[0]
        return SimpleNamespace(artifacts=artifacts)


class FakeHttpRetry:
    def __init__(self):
        self.calls = []

    def execute(self, func, url, **kwargs):
        self.calls.append((func, url, kwargs))
        return SimpleNamespace()


class FakeAzCli:
    def get_access_token(self, scope):
        assert scope == "https://analysis.windows.net/powerbi/api"
        return "fabric-token"


class FakeGatewayClient(GatewayClient):
    def __init__(self):
        self.calls = []
        self.connections = {CONNECTION_ID: GatewayConnection(CONNECTION_ID, CLUSTER_ID, "Remote name")}

    async def get_connection(self, connection_id):
        self.calls.append(connection_id)
        if connection_id.casefold() not in self.connections:
            raise RuntimeError("Configured connection is missing")
        return self.connections[connection_id.casefold()]

    async def list_users(self, connection):
        pytest.fail("Model deployment must not reconcile gateway ACLs")

    async def rename(self, connection, display_name):
        pytest.fail("Model deployment must not rename gateways")

    async def add_user(self, connection, user):
        pytest.fail("Model deployment must not modify gateways")

    async def delete_user(self, connection, user):
        pytest.fail("Model deployment must not modify gateways")

    def get_caller_identifiers(self):
        pytest.fail("Model binding does not require gateway ownership mutation")


class FakeModelClient(SemanticModelClient):
    def __init__(self, bindings=None, roles=None, apply_changes=True):
        self.bindings = bindings or ModelBindingState([], [])
        self.roles = list(roles or [])
        self.apply_changes = apply_changes
        self.calls = []

    async def get_bindings(self, model_id):
        self.calls.append(("read-bindings", model_id))
        return self.bindings

    async def bind(self, model_id, cluster_id, bindings):
        self.calls.append(("bind", model_id, cluster_id, bindings))
        if self.apply_changes:
            for entry in self.bindings.monikers:
                if entry.moniker in bindings:
                    entry.gateway_ids = frozenset({cluster_id.casefold()})
                    entry.connection_ids = frozenset({bindings[entry.moniker].casefold()})

    async def get_rls_membership(self, model_id):
        self.calls.append(("read-rls", model_id))
        return self.roles

    async def update_rls_membership(self, model_id, deltas):
        self.calls.append(("rls", model_id, deltas))
        if self.apply_changes:
            for delta in deltas:
                role = next(role for role in self.roles if role.id == delta.id)
                removed = {member["objectId"].casefold() for member in delta.removed_members}
                role.members = [member for member in role.members if member.object_id.casefold() not in removed] + [RlsMember(member["objectId"], member) for member in delta.added_members]


def _manager(root: Path, folder_client: FakeFolderClient, http_retry: FakeHttpRetry, token_credential=None, gateway_client=None, model_client=None):
    return SemanticModelManager(
        _common(root),
        FakeAzCli(),
        SimpleNamespace(),
        folder_client,
        http_retry,
        token_credential or object(),
        gateway_client or FakeGatewayClient(),
        model_client or FakeModelClient(),
    )


def _patch_fabric_cicd(monkeypatch):
    import fabric_cicd

    captured = {
        "feature_flags": [],
    }

    class FakeFabricWorkspace:
        def __init__(self, **kwargs):
            captured["workspace_kwargs"] = kwargs

    def fake_publish_all_items(workspace, **kwargs):
        captured["workspace"] = workspace
        captured["publish_kwargs"] = kwargs

    monkeypatch.setattr(fabric_cicd, "disable_file_logging", lambda: None)
    monkeypatch.setattr(fabric_cicd, "append_feature_flag", captured["feature_flags"].append)
    monkeypatch.setattr(fabric_cicd, "FabricWorkspace", FakeFabricWorkspace)
    monkeypatch.setattr(fabric_cicd, "publish_all_items", fake_publish_all_items)
    return captured


def test_existing_model_only_reconciles_settings(tmp_path, monkeypatch):
    import fabric_cicd

    monkeypatch.setattr(fabric_cicd, "publish_all_items", lambda *args, **kwargs: pytest.fail("existing models must not be republished"))
    folder_client = FakeFolderClient([[_model_artifact()]])
    http_retry = FakeHttpRetry()
    manager = _manager(tmp_path, folder_client, http_retry)

    asyncio.run(
        manager.reconcile(
            "workspace-id",
            ModelParams(display_name="insights", direct_lake_auto_sync=False),
            _workspace_params(),
        )
    )

    assert len(folder_client.calls) == 1
    assert http_retry.calls[0][1].endswith("/metadata/models/42/settings")
    assert http_retry.calls[0][2]["data"] == '{"directLakeAutoSync":false}'


def test_missing_model_is_published_from_workspace_artifacts(tmp_path, monkeypatch):
    source_directory = tmp_path / "artifacts" / "projects" / "insights" / "insights.SemanticModel"
    source_directory.mkdir(parents=True)
    folder_client = FakeFolderClient([[], [_model_artifact()]])
    http_retry = FakeHttpRetry()
    token_credential = object()
    manager = _manager(tmp_path, folder_client, http_retry, token_credential)
    captured = _patch_fabric_cicd(monkeypatch)

    asyncio.run(
        manager.reconcile(
            "workspace-id",
            ModelParams(display_name="insights", direct_lake_auto_sync=True),
            _workspace_params(),
        )
    )

    assert captured["workspace_kwargs"] == {
        "workspace_id": "workspace-id",
        "environment": "dev",
        "repository_directory": str(tmp_path / "artifacts"),
        "item_type_in_scope": ["SemanticModel"],
        "token_credential": token_credential,
    }
    assert captured["publish_kwargs"] == {
        "items_to_include": ["insights.SemanticModel"],
    }
    assert captured["feature_flags"] == [
        "configured-flag",
        "enable_experimental_features",
        "enable_items_to_include",
    ]
    assert folder_client.calls == ["workspace-id", "workspace-id"]
    assert http_retry.calls[0][1].endswith("/metadata/models/42/settings")
    assert http_retry.calls[0][2]["data"] == '{"directLakeAutoSync":true}'


def _bound_state(*, bound_id=CONNECTION_ID, candidates=None):
    return ModelBindingState([ModelDatasourceBinding(MONIKER, frozenset({CLUSTER_ID}), frozenset({bound_id}), frozenset([CONNECTION_ID] if candidates is None else candidates))], [ModelDatasourceCandidate(CONNECTION_ID, CLUSTER_ID)])


def _member(object_id):
    return RlsMember(object_id, {"objectId": object_id, "displayName": "Private group name", "isSecurityGroup": True, "objectType": 2, "groupType": 1})


def _feature_manager(tmp_path, *, bindings=None, roles=None, missing=False, apply_changes=True):
    client = FakeModelClient(bindings, roles, apply_changes)
    http = FakeHttpRetry()
    gateway_client = FakeGatewayClient()
    manager = _manager(tmp_path, FakeFolderClient([[] if missing else [_model_artifact()]]), http, gateway_client=gateway_client, model_client=client)
    common = manager.common_params
    common.fabric = FabricParams([], [], [GatewayParams(CONNECTION_ID, "Example", [GatewayUserParams("readers", GatewayRole.OWNER, "Read")])])
    common.identities = [Identity("readers", GROUP_ID, PrincipalType.GROUP), Identity("user", "44444444-4444-4444-4444-444444444444", PrincipalType.USER, user_principal_name="reader@example.invalid")]
    common.get_identity_by_given_name = MethodType(CommonParams.get_identity_by_given_name, common)
    return manager, client, http, gateway_client


def test_binding_and_rls_noop_only_preserves_legacy_settings_write(tmp_path):
    manager, client, http, gateway = _feature_manager(tmp_path, bindings=_bound_state(), roles=[RlsRoleMembership(7, "Role", [_member(GROUP_ID)])])
    params = ModelParams("insights", False, [ModelConnectionParams(MONIKER, CONNECTION_ID)], {"Role": ["readers"]})
    asyncio.run(manager.reconcile("workspace", params))
    assert [call[0] for call in client.calls] == ["read-bindings", "read-rls"]
    assert len(http.calls) == 1
    assert gateway.calls == [CONNECTION_ID]


def test_binding_update_and_second_run_noop(tmp_path):
    manager, client, _, _ = _feature_manager(tmp_path, bindings=_bound_state(bound_id=OTHER_ID))
    params = ModelParams("insights", False, [ModelConnectionParams(MONIKER, CONNECTION_ID)])
    asyncio.run(manager.reconcile("workspace", params))
    assert [call for call in client.calls if call[0] == "bind"] == [("bind", 42, CLUSTER_ID, {MONIKER: CONNECTION_ID})]
    client.calls.clear()
    asyncio.run(manager.reconcile("workspace", params))
    assert not any(call[0] == "bind" for call in client.calls)


def test_binding_uses_runtime_resolved_cluster_not_configuration(tmp_path):
    resolved_cluster = "66666666-6666-6666-6666-666666666666"
    state = _bound_state(bound_id=OTHER_ID)
    state.datasources = [ModelDatasourceCandidate(CONNECTION_ID, resolved_cluster)]
    manager, client, _, gateway = _feature_manager(tmp_path, bindings=state)
    gateway.connections[CONNECTION_ID] = GatewayConnection(CONNECTION_ID, resolved_cluster, "Different remote display name")
    asyncio.run(manager.reconcile("workspace", ModelParams("insights", False, [ModelConnectionParams(MONIKER, CONNECTION_ID)])))
    assert ("bind", 42, resolved_cluster, {MONIKER: CONNECTION_ID}) in client.calls
    assert gateway.calls == [CONNECTION_ID]


def test_same_connection_for_multiple_monikers_is_resolved_once(tmp_path):
    second_moniker = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    state = _bound_state(bound_id=OTHER_ID)
    state.monikers.append(ModelDatasourceBinding(second_moniker, frozenset({CLUSTER_ID}), frozenset({OTHER_ID}), frozenset({CONNECTION_ID})))
    manager, client, _, gateway = _feature_manager(tmp_path, bindings=state)
    asyncio.run(manager.reconcile("workspace", ModelParams("insights", False, [ModelConnectionParams(MONIKER, CONNECTION_ID), ModelConnectionParams(second_moniker, CONNECTION_ID)])))
    assert gateway.calls == [CONNECTION_ID]
    assert [call for call in client.calls if call[0] == "bind"] == [("bind", 42, CLUSTER_ID, {MONIKER: CONNECTION_ID, second_moniker: CONNECTION_ID})]


@pytest.mark.parametrize("preview", [False, True])
def test_missing_runtime_connection_fails_before_model_writes(tmp_path, preview):
    manager, client, http, gateway = _feature_manager(tmp_path, bindings=_bound_state(bound_id=OTHER_ID))
    gateway.connections.clear()
    with pytest.raises(RuntimeError, match="connection is missing"):
        asyncio.run(manager.reconcile("workspace", ModelParams("insights", False, [ModelConnectionParams(MONIKER, CONNECTION_ID)], dry_run=preview)))
    assert client.calls == []
    assert http.calls == []


@pytest.mark.parametrize("current,desired,added,removed", [([], ["readers"], 1, 0), ([GROUP_ID], [], 0, 1), ([OTHER_ID], ["readers"], 1, 1), ([GROUP_ID], ["readers"], 0, 0)])
def test_rls_exact_add_remove_mixed_and_noop(tmp_path, current, desired, added, removed):
    manager, client, _, _ = _feature_manager(tmp_path, roles=[RlsRoleMembership(7, "Role", [_member(member) for member in current]), RlsRoleMembership(8, "Unconfigured", [_member(OTHER_ID)])])
    asyncio.run(manager.reconcile("workspace", ModelParams("insights", False, security={"Role": desired})))
    updates = [call for call in client.calls if call[0] == "rls"]
    assert len(updates) == bool(added or removed)
    if updates:
        delta = updates[0][2][0]
        assert len(delta.added_members) == added
        assert len(delta.removed_members) == removed
        assert delta.id == 7
    assert client.roles[1].members[0].object_id == OTHER_ID


def test_rls_user_payload_uses_existing_identity_metadata(tmp_path):
    manager, client, _, _ = _feature_manager(tmp_path, roles=[RlsRoleMembership(7, "Role", [])])
    asyncio.run(manager.reconcile("workspace", ModelParams("insights", False, security={"Role": ["user"]})))
    member = next(call for call in client.calls if call[0] == "rls")[2][0].added_members[0]
    assert member["objectType"] == 1
    assert member["isSecurityGroup"] is False
    assert member["userPrincipalName"] == "reader@example.invalid"


@pytest.mark.parametrize("roles", [[], [RlsRoleMembership(7, "Role", []), RlsRoleMembership(8, "Role", [])]])
def test_missing_or_duplicate_role_fails_before_all_model_writes(tmp_path, roles):
    manager, client, http, _ = _feature_manager(tmp_path, bindings=_bound_state(bound_id=OTHER_ID), roles=roles)
    with pytest.raises(RuntimeError, match="RLS role is missing or ambiguous"):
        asyncio.run(manager.reconcile("workspace", ModelParams("insights", False, [ModelConnectionParams(MONIKER, CONNECTION_ID)], {"Role": ["readers"]})))
    assert http.calls == []
    assert not any(call[0] in ("bind", "rls") for call in client.calls)


def test_moniker_specific_candidate_missing_fails_without_writes(tmp_path):
    manager, client, http, _ = _feature_manager(tmp_path, bindings=_bound_state(bound_id=OTHER_ID, candidates=[OTHER_ID]))
    with pytest.raises(RuntimeError, match="not a valid candidate"):
        asyncio.run(manager.reconcile("workspace", ModelParams("insights", False, [ModelConnectionParams(MONIKER, CONNECTION_ID)])))
    assert http.calls == []
    assert not any(call[0] == "bind" for call in client.calls)


def test_model_preview_suppresses_settings_bindings_and_rls(tmp_path, caplog):
    manager, client, http, _ = _feature_manager(tmp_path, bindings=_bound_state(bound_id=OTHER_ID), roles=[RlsRoleMembership(7, "Role", [_member(OTHER_ID)])])
    caplog.set_level("INFO")
    asyncio.run(manager.reconcile("workspace", ModelParams("insights", False, [ModelConnectionParams(MONIKER, CONNECTION_ID)], {"Role": ["readers"]}, True)))
    assert http.calls == []
    assert not any(call[0] in ("bind", "rls") for call in client.calls)
    assert GROUP_ID not in caplog.text
    assert "Private group name" not in caplog.text


def test_missing_model_preview_validates_source_but_never_publishes(tmp_path, monkeypatch, caplog):
    (tmp_path / "artifacts" / "insights.SemanticModel").mkdir(parents=True)
    manager, client, http, _ = _feature_manager(tmp_path, missing=True)
    monkeypatch.setattr(manager, "_publish_missing_model", lambda *args: pytest.fail("preview must not publish"))
    caplog.set_level("INFO")
    asyncio.run(manager.reconcile("workspace", ModelParams("insights", False, security={"Role": ["readers"]}, dry_run=True), _workspace_params()))
    assert client.calls == []
    assert http.calls == []
    assert "would publish" in caplog.text
    assert "preflight deferred" in caplog.text


def test_duplicate_model_names_fail_before_settings(tmp_path):
    http = FakeHttpRetry()
    manager = _manager(tmp_path, FakeFolderClient([[_model_artifact(), _model_artifact(43)]]), http)
    with pytest.raises(RuntimeError, match="ambiguous"):
        asyncio.run(manager.reconcile("workspace", ModelParams("insights", False)))
    assert http.calls == []


def test_unresolved_rls_principal_fails_before_model_writes(tmp_path):
    manager, client, http, _ = _feature_manager(tmp_path)
    with pytest.raises(RuntimeError, match="exactly one"):
        asyncio.run(manager.reconcile("workspace", ModelParams("insights", False, security={"Role": ["missing"]})))
    assert http.calls == []
    assert client.calls == []


def test_multiple_monikers_cluster_groups_and_unconfigured_binding(tmp_path):
    second_moniker = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    untouched_moniker = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    other_cluster = "66666666-6666-6666-6666-666666666666"
    state = _bound_state(bound_id=OTHER_ID)
    state.monikers.extend([ModelDatasourceBinding(second_moniker, frozenset({CLUSTER_ID}), frozenset({CONNECTION_ID}), frozenset({OTHER_ID})), ModelDatasourceBinding(untouched_moniker, frozenset({CLUSTER_ID}), frozenset({CONNECTION_ID}), frozenset({CONNECTION_ID}))])
    state.datasources.append(ModelDatasourceCandidate(OTHER_ID, other_cluster))
    manager, client, _, gateway_client = _feature_manager(tmp_path, bindings=state)
    manager.common_params.fabric.gateways.append(GatewayParams(OTHER_ID, "Second", [GatewayUserParams("readers", GatewayRole.OWNER, "Read")]))
    gateway_client.connections[OTHER_ID] = GatewayConnection(OTHER_ID, other_cluster, "Remote second")
    asyncio.run(manager.reconcile("workspace", ModelParams("insights", False, [ModelConnectionParams(MONIKER, CONNECTION_ID), ModelConnectionParams(second_moniker, OTHER_ID)])))
    assert len([call for call in client.calls if call[0] == "bind"]) == 2
    assert client.bindings.monikers[2].connection_ids == frozenset({CONNECTION_ID})


def test_failed_rls_verification_is_not_success(tmp_path, monkeypatch):
    monkeypatch.setattr("fabric_workspace_deployment.client.fabric_rest.VERIFY_DELAY_SECONDS", 0)
    manager, _, _, _ = _feature_manager(tmp_path, roles=[RlsRoleMembership(7, "Role", [])], apply_changes=False)
    with pytest.raises(RuntimeError, match="did not converge"):
        asyncio.run(manager.reconcile("workspace", ModelParams("insights", False, security={"Role": ["readers"]})))


def test_missing_model_source_fails_instead_of_warning(tmp_path):
    (tmp_path / "artifacts").mkdir()
    manager = _manager(tmp_path, FakeFolderClient([[]]), FakeHttpRetry())

    with pytest.raises(RuntimeError, match="source directory 'insights.SemanticModel' was not found"):
        asyncio.run(
            manager.reconcile(
                "workspace-id",
                ModelParams(display_name="insights", direct_lake_auto_sync=False),
                _workspace_params(),
            )
        )


def test_publish_fails_when_model_never_becomes_available(tmp_path, monkeypatch):
    (tmp_path / "artifacts" / "insights.SemanticModel").mkdir(parents=True)
    folder_client = FakeFolderClient([[]])
    manager = _manager(tmp_path, folder_client, FakeHttpRetry())
    _patch_fabric_cicd(monkeypatch)
    monkeypatch.setattr(model_module, "MODEL_PUBLISH_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(model_module, "MODEL_PUBLISH_RETRY_DELAY_SECONDS", 0)

    with pytest.raises(RuntimeError, match="did not become available"):
        asyncio.run(
            manager.reconcile(
                "workspace-id",
                ModelParams(display_name="insights", direct_lake_auto_sync=False),
                _workspace_params(),
            )
        )

    assert folder_client.calls == ["workspace-id", "workspace-id", "workspace-id"]
