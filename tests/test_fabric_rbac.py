# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import logging

from types import SimpleNamespace

import pytest

from fabric_workspace_deployment.factories.management_factory import ContainerizedManagementFactory
from fabric_workspace_deployment.manager.fabric import rbac as rbac_module
from fabric_workspace_deployment.manager.fabric.rbac import FabricRbacManager
from fabric_workspace_deployment.operations.operation_interfaces import (
    AccessSource,
    CicdArtifactType,
    FabricFolderArtifact,
    FabricFolderCollection,
    FabricWorkspaceItem,
    FabricWorkspaceItemRbacDetail,
    FabricWorkspaceItemRbacInfo,
    FolderRole,
    Identity,
    ItemRbacDetailParams,
    ItemRbacParams,
    OperationParams,
    PrincipalType,
    RbacParams,
    SqlEndpointSecurity,
    UniversalSecurity,
)

MODEL_OBJECT_ID = "22222222-2222-2222-2222-222222222222"
APP_OBJECT_ID = "33333333-3333-3333-3333-333333333333"
GROUP_OBJECT_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
USER_OBJECT_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
SP_OBJECT_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"


class FakeResponse:
    def __init__(self, data=None):
        self.data = data or {}

    def json(self):
        return self.data


class FakeHttpRetry:
    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.calls = []

    def execute(self, func, url, **kwargs):
        self.calls.append((func, url, kwargs))
        response = self.responses.pop(0) if self.responses else {}
        return FakeResponse(response)


class FakeFolderClient:
    def __init__(self, artifacts=None):
        self.artifacts = list(artifacts or [])
        self.calls = []

    async def get_fabric_folder_collection(self, workspace_id):
        self.calls.append(workspace_id)
        return FabricFolderCollection(artifacts=self.artifacts)


class FakeAzCli:
    def __init__(self):
        self.calls = []

    def get_access_token(self, scope, force_run_az=False):
        assert scope == "https://analysis.windows.net/powerbi/api"
        self.calls.append((scope, force_run_az))
        return "fabric-token"


def _identity(principal_type: PrincipalType, object_id: str) -> Identity:
    return Identity(
        given_name=f"{principal_type.value}-{object_id[:4]}",
        object_id=object_id,
        principal_type=principal_type,
        aad_app_id="dddddddd-dddd-dddd-dddd-dddddddddddd" if principal_type == PrincipalType.SERVICE_PRINCIPAL else None,
    )


def _common(identities=None):
    return SimpleNamespace(
        endpoint=SimpleNamespace(
            analysis_service="https://analysis.example.invalid",
            power_bi="https://api.fabric.microsoft.com",
        ),
        scope=SimpleNamespace(
            analysis_service="https://analysis.windows.net/powerbi/api",
        ),
        fabric=SimpleNamespace(workspaces=[]),
        identities=list(identities or []),
    )


def _manager(identities=None, responses=None, artifacts=None):
    http_retry = FakeHttpRetry(responses)
    folder_client = FakeFolderClient(artifacts)
    manager = FabricRbacManager(
        _common(identities),
        FakeAzCli(),
        SimpleNamespace(),
        SimpleNamespace(),
        folder_client,
        http_retry,
    )
    return manager, http_retry, folder_client


def _access_source():
    return AccessSource(
        id=500001,
        folder_role_id=4,
        folder_role=FolderRole(id=4, name="Viewer", tenant_id=None),
        artifact_link_id=None,
        expiration=None,
        artifact_link=None,
        artifact_access_source_id=None,
        artifact_access_source=None,
    )


def _current_detail(
    object_id: str,
    permissions: int,
    principal_type: PrincipalType = PrincipalType.GROUP,
    artifact_permissions: int | None = None,
    inherited: bool = False,
):
    return FabricWorkspaceItemRbacDetail(
        id=2000002,
        user_id=None if principal_type == PrincipalType.GROUP else 800001,
        group_id=900001 if principal_type == PrincipalType.GROUP else None,
        permissions=permissions,
        given_name=f"{principal_type.value}-{object_id[:4]}",
        object_id=object_id,
        artifact_permissions=artifact_permissions,
        aad_app_id="dddddddd-dddd-dddd-dddd-dddddddddddd" if principal_type == PrincipalType.SERVICE_PRINCIPAL else None,
        access_source=_access_source() if inherited else None,
    )


def _item_info(item_type: str, display_name: str, object_id: str, item_id: int, detail=None):
    return FabricWorkspaceItemRbacInfo(
        id=item_id,
        type=item_type,
        display_name=display_name,
        object_id=object_id,
        permissions=15,
        shared_with_count=0,
        detail=list(detail or []),
    )


def _rbac_params(identities, purge=True):
    return RbacParams(
        purge_unmatched_role_assignments=purge,
        workspace=[],
        items=[],
        universal_security=UniversalSecurity(sql_endpoint=SqlEndpointSecurity(enabled=False, skip_reconcile=[])),
    )


def _validation_params():
    params = OperationParams.__new__(OperationParams)
    params.logger = logging.getLogger("test-fabric-rbac-validation")
    return params


def test_cicd_artifact_type_exposes_app_backend():
    assert CicdArtifactType.APP_BACKEND.value == "AppBackend"


@pytest.mark.parametrize(
    ("item_type", "permissions"),
    [(CicdArtifactType.SEMANTIC_MODEL.value, value) for value in (1, 3, 5, 7, 9, 11, 13, 15)] + [(CicdArtifactType.APP_BACKEND.value, value) for value in (65, 67, 69, 71)],
)
@pytest.mark.parametrize("artifact_permissions", [None, 0])
def test_item_rbac_validation_accepts_exact_permissions(item_type, permissions, artifact_permissions):
    params = _validation_params()
    item = ItemRbacParams(
        type=item_type,
        display_name="item",
        detail=[
            ItemRbacDetailParams(
                permissions=permissions,
                artifact_permissions=artifact_permissions,
                object_id=GROUP_OBJECT_ID,
                purpose="test",
            )
        ],
    )

    assert params._validate_item_rbac_params(item, 2, 3, {GROUP_OBJECT_ID}) is True


@pytest.mark.parametrize(
    ("item_type", "permissions"),
    [(CicdArtifactType.SEMANTIC_MODEL.value, value) for value in (0, 2, 4, 6, 8, 10, 12, 14, 16, 17, 32, 64, 128)] + [(CicdArtifactType.APP_BACKEND.value, value) for value in (0, 1, 3, 8, 64, 73, 81, 97, 193, 321)],
)
def test_item_rbac_validation_rejects_invalid_permission_values(item_type, permissions, caplog):
    params = _validation_params()
    item = ItemRbacParams(
        type=item_type,
        display_name="item",
        detail=[
            ItemRbacDetailParams(
                permissions=permissions,
                object_id=GROUP_OBJECT_ID,
                purpose="test",
            )
        ],
    )
    caplog.set_level(logging.ERROR)

    assert params._validate_item_rbac_params(item, 2, 3, {GROUP_OBJECT_ID}) is False
    assert f"Workspace rbac items[3].detail[0].permissions at index 2" in caplog.text
    assert f"for {item_type}, got {permissions}" in caplog.text


@pytest.mark.parametrize("item_type", [CicdArtifactType.SEMANTIC_MODEL.value, CicdArtifactType.APP_BACKEND.value])
@pytest.mark.parametrize("artifact_permissions", [1, 7])
def test_item_rbac_validation_rejects_nonzero_artifact_permissions(item_type, artifact_permissions, caplog):
    params = _validation_params()
    valid_permissions = 9 if item_type == CicdArtifactType.SEMANTIC_MODEL.value else 65
    item = ItemRbacParams(
        type=item_type,
        display_name="item",
        detail=[
            ItemRbacDetailParams(
                permissions=valid_permissions,
                artifact_permissions=artifact_permissions,
                object_id=GROUP_OBJECT_ID,
                purpose="test",
            )
        ],
    )
    caplog.set_level(logging.ERROR)

    assert params._validate_item_rbac_params(item, 4, 5, {GROUP_OBJECT_ID}) is False
    assert "Workspace rbac items[5].detail[0].artifactPermissions at index 4" in caplog.text
    assert f"must be omitted or 0 for {item_type}" in caplog.text


@pytest.mark.parametrize(
    ("principal_type", "object_id"),
    [
        (PrincipalType.GROUP, GROUP_OBJECT_ID),
        (PrincipalType.USER, USER_OBJECT_ID),
        (PrincipalType.SERVICE_PRINCIPAL, SP_OBJECT_ID),
    ],
)
def test_app_backend_payload_uses_object_ids_for_all_principal_types(principal_type, object_id):
    identity = _identity(principal_type, object_id)
    manager, http_retry, _ = _manager([identity])
    assignment = ItemRbacDetailParams(
        permissions=69,
        artifact_permissions=0,
        object_id=object_id,
        purpose="test",
    )

    asyncio.run(manager.update_item_role_assignment(APP_OBJECT_ID, assignment, identity))

    func, url, kwargs = http_retry.calls[-1]
    assert func is rbac_module.requests.put
    assert url == "https://analysis.example.invalid/metadata/access"
    assert kwargs["json"]["models"] == []
    assert kwargs["json"]["artifacts"] == [
        {
            "artifactObjectId": APP_OBJECT_ID,
            "permissions": 69,
            "isServicePrincipal": principal_type == PrincipalType.SERVICE_PRINCIPAL,
            "artifactPermissions": 0,
            "groupObjectId": object_id if principal_type == PrincipalType.GROUP else None,
            "userId": None,
            "groupId": None,
            "userObjectId": None if principal_type == PrincipalType.GROUP else object_id,
        }
    ]


def test_app_backend_payload_omits_artifact_permissions_when_not_configured():
    identity = _identity(PrincipalType.GROUP, GROUP_OBJECT_ID)
    manager, http_retry, _ = _manager([identity])
    assignment = ItemRbacDetailParams(
        permissions=65,
        object_id=GROUP_OBJECT_ID,
        purpose="test",
    )

    asyncio.run(manager.update_item_role_assignment(APP_OBJECT_ID, assignment, identity))

    artifact_data = http_retry.calls[-1][2]["json"]["artifacts"][0]
    assert "artifactPermissions" not in artifact_data


@pytest.mark.parametrize(
    ("principal_type", "object_id"),
    [
        (PrincipalType.GROUP, GROUP_OBJECT_ID),
        (PrincipalType.USER, USER_OBJECT_ID),
        (PrincipalType.SERVICE_PRINCIPAL, SP_OBJECT_ID),
    ],
)
def test_semantic_model_payload_uses_object_ids_for_all_principal_types(principal_type, object_id):
    identity = _identity(principal_type, object_id)
    manager, http_retry, _ = _manager([identity])
    assignment = ItemRbacDetailParams(
        permissions=13,
        artifact_permissions=0,
        object_id=object_id,
        purpose="test",
    )

    asyncio.run(manager.update_model_role_assignment(2000002, assignment, identity))

    assert manager.az_cli.calls[-1] == ("https://analysis.windows.net/powerbi/api", True)
    func, url, kwargs = http_retry.calls[-1]
    assert func is rbac_module.requests.put
    assert url == "https://analysis.example.invalid/metadata/access"
    assert kwargs["json"]["artifacts"] == []
    assert kwargs["json"]["models"] == [
        {
            "id": 2000002,
            "permissions": 13,
            "isServicePrincipal": principal_type == PrincipalType.SERVICE_PRINCIPAL,
            "userId": None,
            "groupId": None,
            "userObjectId": None if principal_type == PrincipalType.GROUP else object_id,
            "groupObjectId": object_id if principal_type == PrincipalType.GROUP else None,
        }
    ]
    assert "artifactPermissions" not in kwargs["json"]["models"][0]


def test_app_backend_reads_from_artifacts_access_endpoint():
    response = {
        "id": 3000003,
        "displayName": "my-rayfin-app",
        "objectId": APP_OBJECT_ID,
        "permissions": 71,
        "sharedWithCount": 0,
        "detail": [],
    }
    manager, http_retry, _ = _manager(responses=[response])

    result = asyncio.run(manager.get_fabric_workspace_item_rbac_info(APP_OBJECT_ID, CicdArtifactType.APP_BACKEND.value))

    func, url, kwargs = http_retry.calls[-1]
    assert func is rbac_module.requests.get
    assert url == f"https://analysis.example.invalid/metadata/access/artifacts/{APP_OBJECT_ID}"
    assert kwargs["params"] == {"includeRestrictedUsers": "true"}
    assert result.type == CicdArtifactType.APP_BACKEND.value
    assert result.object_id == APP_OBJECT_ID


def test_semantic_model_reads_by_internal_integer_id():
    response = {
        "id": 2000002,
        "displayName": "my_model",
        "objectId": MODEL_OBJECT_ID,
        "permissions": 15,
        "sharedWithCount": 0,
        "detail": [
            {
                "id": 2000002,
                "userId": None,
                "groupId": 900001,
                "permissions": 9,
                "givenName": "Readers",
                "objectId": GROUP_OBJECT_ID,
            }
        ],
        "relatedReportIds": [7000007],
    }
    manager, http_retry, _ = _manager(responses=[response])

    result = asyncio.run(manager.get_fabric_model_rbac_info(2000002))

    assert manager.az_cli.calls[-1] == ("https://analysis.windows.net/powerbi/api", True)
    func, url, kwargs = http_retry.calls[-1]
    assert func is rbac_module.requests.get
    assert url == "https://analysis.example.invalid/metadata/access/models/2000002"
    assert kwargs["params"] == {"includeRestrictedUsers": "true"}
    assert result.type == CicdArtifactType.SEMANTIC_MODEL.value
    assert result.object_id == MODEL_OBJECT_ID
    assert result.detail[0].artifact_permissions is None
    assert result.detail[0].aad_app_id is None


def test_semantic_model_integer_id_resolution_uses_folder_relation_collection():
    artifacts = [
        FabricFolderArtifact(
            id=2000002,
            object_id=MODEL_OBJECT_ID,
            type=3,
            type_name="Model",
            display_name="my_model",
            permissions=15,
            is_hidden=False,
            artifact_permissions=0,
        ),
        FabricFolderArtifact(
            id=3000003,
            object_id=APP_OBJECT_ID,
            type=12,
            type_name="AppBackend",
            display_name="my-rayfin-app",
            permissions=71,
            is_hidden=False,
            artifact_permissions=0,
        ),
    ]
    manager, _, folder_client = _manager(artifacts=artifacts)
    workspace_items = [
        FabricWorkspaceItem(
            id=MODEL_OBJECT_ID,
            type=CicdArtifactType.SEMANTIC_MODEL.value,
            display_name="my_model",
            description="",
            workspace_id="workspace-id",
        ),
        FabricWorkspaceItem(
            id=APP_OBJECT_ID,
            type=CicdArtifactType.APP_BACKEND.value,
            display_name="my-rayfin-app",
            description="",
            workspace_id="workspace-id",
        ),
    ]

    result = asyncio.run(manager._resolve_semantic_model_ids("workspace-id", workspace_items))

    assert result == {MODEL_OBJECT_ID: 2000002}
    assert folder_client.calls == ["workspace-id"]


def test_reconcile_routes_semantic_model_and_app_backend_to_correct_access_apis(monkeypatch):
    artifacts = [
        FabricFolderArtifact(
            id=2000002,
            object_id=MODEL_OBJECT_ID,
            type=3,
            type_name="Model",
            display_name="my_model",
            permissions=15,
            is_hidden=False,
            artifact_permissions=0,
        )
    ]
    manager, _, folder_client = _manager(artifacts=artifacts)
    workspace_items = [
        FabricWorkspaceItem(
            id=MODEL_OBJECT_ID,
            type=CicdArtifactType.SEMANTIC_MODEL.value,
            display_name="my_model",
            description="",
            workspace_id="workspace-id",
        ),
        FabricWorkspaceItem(
            id=APP_OBJECT_ID,
            type=CicdArtifactType.APP_BACKEND.value,
            display_name="my-rayfin-app",
            description="",
            workspace_id="workspace-id",
        ),
    ]
    calls = {"models": [], "artifacts": [], "reconciled": []}

    async def get_folder_info(workspace_id):
        assert workspace_id == "workspace-id"
        return SimpleNamespace(id=1000001)

    async def get_items(workspace_id):
        assert workspace_id == "workspace-id"
        return workspace_items

    async def get_workspace_rbac(folder_id):
        assert folder_id == 1000001
        return SimpleNamespace(id=1000001, detail=[])

    async def get_model_rbac(model_id):
        calls["models"].append(model_id)
        return _item_info(CicdArtifactType.SEMANTIC_MODEL.value, "my_model", MODEL_OBJECT_ID, model_id)

    async def get_artifact_rbac(item_id, item_type):
        calls["artifacts"].append((item_id, item_type))
        return _item_info(item_type, "my-rayfin-app", item_id, 3000003)

    async def reconcile_assignments(desired_state, current_state, item_infos):
        calls["reconciled"].extend(item_infos)

    async def reconcile_security(universal_security, items):
        assert items == workspace_items

    monkeypatch.setattr(manager, "get_fabric_workspace_folder_info", get_folder_info)
    monkeypatch.setattr(manager, "get_fabric_workspace_item_info", get_items)
    monkeypatch.setattr(manager, "get_fabric_workspace_folder_rbac_info", get_workspace_rbac)
    monkeypatch.setattr(manager, "get_fabric_model_rbac_info", get_model_rbac)
    monkeypatch.setattr(manager, "get_fabric_workspace_item_rbac_info", get_artifact_rbac)
    monkeypatch.setattr(manager, "_reconcile_role_assignments", reconcile_assignments)
    monkeypatch.setattr(manager, "_reconcile_one_security", reconcile_security)

    asyncio.run(manager.reconcile("workspace-id", _rbac_params([])))

    assert folder_client.calls == ["workspace-id"]
    assert calls["models"] == [2000002]
    assert calls["artifacts"] == [(APP_OBJECT_ID, CicdArtifactType.APP_BACKEND.value)]
    assert [item.type for item in calls["reconciled"]] == [
        CicdArtifactType.SEMANTIC_MODEL.value,
        CicdArtifactType.APP_BACKEND.value,
    ]


def test_factory_injects_fabric_folder_client_into_rbac_manager(monkeypatch):
    common = _common()
    factory = ContainerizedManagementFactory(SimpleNamespace(common=common))
    az_cli = object()
    fabric_cli = object()
    workspace = object()
    folder_client = object()

    monkeypatch.setattr(factory, "create_azure_cli", lambda: az_cli)
    monkeypatch.setattr(factory, "create_fabric_cli", lambda: fabric_cli)
    monkeypatch.setattr(factory, "create_fabric_workspace_manager", lambda: workspace)
    monkeypatch.setattr(factory, "create_fabric_folder_client", lambda: folder_client)

    manager = factory.create_fabric_rbac_manager()

    assert manager.az_cli is az_cli
    assert manager.fabric_cli is fabric_cli
    assert manager.workspace is workspace
    assert manager.folder_client is folder_client


def test_app_backend_reconcile_adds_updates_removes_and_protects_inherited_rows(monkeypatch):
    inherited_explicit = "11111111-1111-1111-1111-111111111111"
    direct_unmatched = "22222222-3333-4444-5555-666666666666"
    inherited_unmatched = "77777777-8888-9999-aaaa-bbbbbbbbbbbb"
    new_direct = "cccccccc-dddd-eeee-ffff-000000000000"
    identities = [
        _identity(PrincipalType.GROUP, inherited_explicit),
        _identity(PrincipalType.USER, new_direct),
    ]
    manager, _, _ = _manager(identities)
    calls = []

    async def capture(item_id, assignment, identity):
        calls.append((item_id, assignment, identity))

    monkeypatch.setattr(manager, "update_item_role_assignment", capture)
    current_item = _item_info(
        CicdArtifactType.APP_BACKEND.value,
        "my-rayfin-app",
        APP_OBJECT_ID,
        3000003,
        [
            _current_detail(inherited_explicit, 65, inherited=True),
            _current_detail(direct_unmatched, 69),
            _current_detail(inherited_unmatched, 65, inherited=True),
        ],
    )
    desired_item = ItemRbacParams(
        type=CicdArtifactType.APP_BACKEND.value,
        display_name="my-rayfin-app",
        detail=[
            ItemRbacDetailParams(permissions=69, artifact_permissions=0, object_id=inherited_explicit, purpose="explicit desired grant"),
            ItemRbacDetailParams(permissions=65, artifact_permissions=0, object_id=new_direct, purpose="new grant"),
        ],
    )

    asyncio.run(manager._reconcile_single_item_permissions(desired_item, current_item, _rbac_params(identities)))

    changes = {(assignment.object_id, assignment.permissions) for _, assignment, _ in calls}
    assert changes == {
        (inherited_explicit, 69),
        (new_direct, 65),
        (direct_unmatched, 0),
    }
    assert all(item_id == APP_OBJECT_ID for item_id, _, _ in calls)
    assert inherited_unmatched not in {assignment.object_id for _, assignment, _ in calls}


def test_semantic_model_reconcile_adds_updates_removes_and_protects_workspace_roles(monkeypatch):
    inherited_unmatched = "11111111-1111-1111-1111-111111111111"
    direct_unmatched = "22222222-3333-4444-5555-666666666666"
    inherited_explicit = "77777777-8888-9999-aaaa-bbbbbbbbbbbb"
    new_direct = "cccccccc-dddd-eeee-ffff-000000000000"
    identities = [
        _identity(PrincipalType.GROUP, inherited_explicit),
        _identity(PrincipalType.SERVICE_PRINCIPAL, new_direct),
    ]
    manager, _, _ = _manager(identities)
    calls = []

    async def capture(model_id, assignment, identity):
        calls.append((model_id, assignment, identity))

    monkeypatch.setattr(manager, "update_model_role_assignment", capture)
    current_item = _item_info(
        CicdArtifactType.SEMANTIC_MODEL.value,
        "my_model",
        MODEL_OBJECT_ID,
        2000002,
        [
            _current_detail(inherited_unmatched, 1),
            _current_detail(direct_unmatched, 9),
            _current_detail(inherited_explicit, 1),
        ],
    )
    desired_item = ItemRbacParams(
        type=CicdArtifactType.SEMANTIC_MODEL.value,
        display_name="my_model",
        detail=[
            ItemRbacDetailParams(permissions=9, object_id=inherited_explicit, purpose="explicit desired grant"),
            ItemRbacDetailParams(permissions=15, object_id=new_direct, purpose="new grant"),
        ],
    )

    asyncio.run(
        manager._reconcile_single_item_permissions(
            desired_item,
            current_item,
            _rbac_params(identities),
            {inherited_unmatched, inherited_explicit},
        )
    )

    changes = {(assignment.object_id, assignment.permissions) for _, assignment, _ in calls}
    assert changes == {
        (inherited_explicit, 9),
        (new_direct, 15),
        (direct_unmatched, 0),
    }
    assert all(model_id == 2000002 for model_id, _, _ in calls)
    assert inherited_unmatched not in {assignment.object_id for _, assignment, _ in calls}


@pytest.mark.parametrize(
    ("item_type", "permissions", "artifact_permissions"),
    [
        (CicdArtifactType.APP_BACKEND.value, 65, 0),
        (CicdArtifactType.SEMANTIC_MODEL.value, 9, None),
    ],
)
def test_item_reconcile_is_idempotent(item_type, permissions, artifact_permissions, monkeypatch):
    identity = _identity(PrincipalType.GROUP, GROUP_OBJECT_ID)
    manager, _, _ = _manager([identity])

    async def unexpected(*args):
        pytest.fail(f"Idempotent reconciliation must not write: {args}")

    monkeypatch.setattr(manager, "update_item_role_assignment", unexpected)
    monkeypatch.setattr(manager, "update_model_role_assignment", unexpected)
    object_id = APP_OBJECT_ID if item_type == CicdArtifactType.APP_BACKEND.value else MODEL_OBJECT_ID
    item_id = 3000003 if item_type == CicdArtifactType.APP_BACKEND.value else 2000002
    current_item = _item_info(
        item_type,
        "item",
        object_id,
        item_id,
        [_current_detail(GROUP_OBJECT_ID, permissions, artifact_permissions=None)],
    )
    desired_item = ItemRbacParams(
        type=item_type,
        display_name="item",
        detail=[
            ItemRbacDetailParams(
                permissions=permissions,
                artifact_permissions=artifact_permissions,
                object_id=GROUP_OBJECT_ID,
                purpose="already correct",
            )
        ],
    )

    asyncio.run(manager._reconcile_single_item_permissions(desired_item, current_item, _rbac_params([identity])))


def test_item_reconcile_does_not_purge_when_purge_is_disabled(monkeypatch):
    manager, _, _ = _manager()

    async def unexpected(*args):
        pytest.fail(f"Purge-disabled reconciliation must not remove: {args}")

    monkeypatch.setattr(manager, "update_item_role_assignment", unexpected)
    current_item = _item_info(
        CicdArtifactType.APP_BACKEND.value,
        "my-rayfin-app",
        APP_OBJECT_ID,
        3000003,
        [_current_detail(GROUP_OBJECT_ID, 65)],
    )
    desired_item = ItemRbacParams(
        type=CicdArtifactType.APP_BACKEND.value,
        display_name="my-rayfin-app",
        detail=[],
    )

    asyncio.run(manager._reconcile_single_item_permissions(desired_item, current_item, _rbac_params([], purge=False)))


def test_missing_configured_item_warns_without_failing(caplog):
    identity = _identity(PrincipalType.GROUP, GROUP_OBJECT_ID)
    manager, _, _ = _manager([identity])
    desired_item = ItemRbacParams(
        type=CicdArtifactType.SEMANTIC_MODEL.value,
        display_name="missing_model",
        detail=[
            ItemRbacDetailParams(
                permissions=9,
                object_id=GROUP_OBJECT_ID,
                purpose="test",
            )
        ],
    )
    caplog.set_level(logging.WARNING)

    asyncio.run(manager._reconcile_item_level_permissions([desired_item], [], _rbac_params([identity]), set()))

    assert "Desired item not found in workspace: SemanticModel - missing_model" in caplog.text
