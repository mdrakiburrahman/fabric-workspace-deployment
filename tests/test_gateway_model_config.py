# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging
import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from fabric_workspace_deployment.operations.operation_interfaces import CommonParams, FabricParams, GatewayParams, GatewayRole, GatewayUserParams, Identity, ModelConnectionParams, ModelSourceItemParams, ModelParams, Operation, OperationParams, PrincipalType
from fabric_workspace_deployment.operations import operators
from fabric_workspace_deployment.factories.management_factory import ContainerizedManagementFactory

CONNECTION_ID = "11111111-1111-1111-1111-111111111111"
GROUP_ID = "33333333-3333-3333-3333-333333333333"
USER_ID = "44444444-4444-4444-4444-444444444444"
MONIKER = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


def _gateway():
    return GatewayParams(CONNECTION_ID, "Example connection", [GatewayUserParams("owner", GatewayRole.OWNER, "Read"), GatewayUserParams("readers", GatewayRole.USER, "Read")])


def _params():
    params = OperationParams.__new__(OperationParams)
    params.logger = logging.getLogger("gateway-config-test")
    params.operation = Operation.DEPLOY_GATEWAY
    params.common = CommonParams(
        local=SimpleNamespace(),
        endpoint=SimpleNamespace(),
        scope=SimpleNamespace(),
        arm=SimpleNamespace(),
        fabric=FabricParams([], [], [_gateway()]),
        identities=[Identity("owner", USER_ID, PrincipalType.USER, user_principal_name="owner@example.invalid"), Identity("readers", GROUP_ID, PrincipalType.GROUP)],
    )
    return params


def _raw_gateway(**overrides):
    return {"connectionId": CONNECTION_ID, "displayName": "Example connection", "users": [{"identity": "owner", "role": "Owner", "datasourceAccessRight": "Read"}], **overrides}


def test_gateway_enum_and_backward_compatible_model_defaults():
    assert Operation("deployGateway") is Operation.DEPLOY_GATEWAY
    model = ModelParams("Example", False)
    assert model.connections == []
    assert model.security == {}
    assert model.dry_run is False
    assert FabricParams([], []).gateways == []


def test_parse_gateway_and_model_preview():
    params = _params()
    gateway = params._parse_gateway_params([_raw_gateway(dryRun=True)])[0]
    model = params._parse_model_params([{"displayName": "Example", "directLakeAutoSync": False, "dryRun": True, "connections": [{"sourceItem": {"type": "SqlEndpoint", "name": "insights"}, "connectionId": CONNECTION_ID}], "security": {"Existing role": ["readers"]}}])[0]
    assert vars(gateway).keys() == {"connection_id", "display_name", "users", "dry_run"}
    assert gateway.dry_run is True
    assert gateway.users[0].role is GatewayRole.OWNER
    assert model.dry_run is True
    assert model.connections == [ModelConnectionParams(None, CONNECTION_ID, ModelSourceItemParams("SqlEndpoint", "insights"))]
    assert model.security == {"Existing role": ["readers"]}
    assert params._validate_gateway_params()
    assert params._validate_model_params([model], 0)


@pytest.mark.parametrize("source", [None, [], "", {}, {"type": "Lakehouse", "name": "insights"}, {"type": "SqlEndpoint"}, {"type": "SqlEndpoint", "name": ""}])
def test_source_item_requires_supported_type_and_name(source):
    with pytest.raises(ValueError, match="sourceItem"):
        _params()._parse_model_params([{"displayName": "Example", "directLakeAutoSync": False, "connections": [{"sourceItem": source, "connectionId": CONNECTION_ID}]}])


def test_source_item_and_expert_moniker_are_mutually_exclusive():
    with pytest.raises(ValueError, match="both"):
        _params()._parse_model_params([{"displayName": "Example", "directLakeAutoSync": False, "connections": [{"sourceItem": {"type": "SqlEndpoint", "name": "insights"}, "moniker": MONIKER, "connectionId": CONNECTION_ID}]}])


def test_duplicate_source_items_are_rejected():
    source = ModelSourceItemParams("SqlEndpoint", "insights")
    assert not _params()._validate_model_params([ModelParams("Example", False, [ModelConnectionParams(None, CONNECTION_ID, source), ModelConnectionParams(None, CONNECTION_ID, source)])], 0)


@pytest.mark.parametrize("value", [None, {}, "connection"])
def test_gateway_array_is_strict(value):
    with pytest.raises(ValueError, match="gateways must be an array"):
        _params()._parse_gateway_params(value)


@pytest.mark.parametrize("value", [None, 0, 1, "true", []])
def test_preview_flag_is_a_boolean(value):
    params = _params()
    with pytest.raises(ValueError, match="dryRun must be a boolean"):
        params._parse_gateway_params([_raw_gateway(dryRun=value)])
    with pytest.raises(ValueError, match="dryRun must be a boolean"):
        params._parse_model_params([{"displayName": "Example", "directLakeAutoSync": False, "dryRun": value}])


@pytest.mark.parametrize("security", [[], None, "role", {"": []}, {"role": "readers"}, {"role": [None]}, {"role": [""]}])
def test_security_requires_a_role_to_identity_array_object(security):
    with pytest.raises(ValueError, match="security"):
        _params()._parse_model_params([{"displayName": "Example", "directLakeAutoSync": False, "security": security}])


@pytest.mark.parametrize("connections", [None, {}, "connection", [None], [{"moniker": MONIKER}], [{"moniker": "", "connectionId": CONNECTION_ID}]])
def test_binding_configuration_is_strict(connections):
    with pytest.raises(ValueError, match="connections"):
        _params()._parse_model_params([{"displayName": "Example", "directLakeAutoSync": False, "connections": connections}])


def test_empty_named_role_means_clear_and_empty_object_is_unmanaged():
    params = _params()
    models = params._parse_model_params([{"displayName": "clear", "directLakeAutoSync": False, "security": {"Existing role": []}}, {"displayName": "unmanaged", "directLakeAutoSync": False}])
    assert models[0].security == {"Existing role": []}
    assert models[1].security == {}
    assert params._validate_model_params(models, 0)


@pytest.mark.parametrize("field,value", [("connection_id", "bad"), ("display_name", ""), ("dry_run", "true"), ("users", [])])
def test_invalid_gateway_values_fail_validation(field, value):
    params = _params()
    params.common.fabric.gateways = [replace(_gateway(), **{field: value})]
    assert not params._validate_gateway_params()


def test_connection_ids_are_unique_but_display_names_can_repeat():
    params = _params()
    params.common.fabric.gateways.append(replace(_gateway(), connection_id="55555555-5555-5555-5555-555555555555"))
    assert params._validate_gateway_params()
    params.common.fabric.gateways[1] = _gateway()
    assert not params._validate_gateway_params()


def test_connection_guid_references_and_uniqueness_are_case_insensitive():
    params = _params()
    guid = "abcdef01-2345-6789-abcd-ef0123456789"
    params.common.fabric.gateways = [replace(_gateway(), connection_id=guid)]
    assert params._validate_model_params([ModelParams("Example", False, [ModelConnectionParams(MONIKER, guid.upper())])], 0)
    params.common.fabric.gateways.append(replace(_gateway(), connection_id=guid.upper()))
    assert not params._validate_gateway_params()
    assert not params._validate_model_params([ModelParams("Example", False, [ModelConnectionParams(MONIKER, guid)])], 0)


@pytest.mark.parametrize("role,access", [("UserWithReshare", "Read"), ("Owner", "Write")])
def test_only_captured_gateway_permissions_are_accepted(role, access):
    with pytest.raises(ValueError):
        _params()._parse_gateway_params([_raw_gateway(users=[{"identity": "owner", "role": role, "datasourceAccessRight": access}])])


def test_referenced_identities_must_resolve_uniquely_and_have_supported_metadata():
    params = _params()
    assert not params._validate_membership_identities(["missing"], "reference")
    params.common.identities.append(Identity("readers", "55555555-5555-5555-5555-555555555555", PrincipalType.GROUP))
    assert not params._validate_membership_identities(["readers"], "reference")
    params = _params()
    params.common.identities[1] = replace(params.common.identities[1], principal_type=PrincipalType.SERVICE_PRINCIPAL)
    assert not params._validate_membership_identities(["readers"], "reference")
    params.common.identities[0] = replace(params.common.identities[0], user_principal_name=None)
    assert not params._validate_membership_identities(["owner"], "reference")


def test_duplicate_principals_in_a_role_are_rejected_by_object_id():
    params = _params()
    params.common.identities.append(Identity("alias", GROUP_ID.upper(), PrincipalType.GROUP))
    assert not params._validate_membership_identities(["readers", "alias"], "reference")


def test_model_binding_refs_and_monikers_are_validated_even_in_preview():
    params = _params()
    assert not params._validate_model_params([ModelParams("Example", False, [ModelConnectionParams(MONIKER, "55555555-5555-5555-5555-555555555555")], dry_run=True)], 0)
    assert not params._validate_model_params([ModelParams("Example", False, [ModelConnectionParams("bad", CONNECTION_ID)])], 0)
    assert not params._validate_model_params([ModelParams("Example", False, [ModelConnectionParams(MONIKER, CONNECTION_ID), ModelConnectionParams(MONIKER.upper(), CONNECTION_ID)])], 0)
    assert not params._validate_model_params([ModelParams("Example", False), ModelParams("Example", False)], 0)
    assert not params._validate_model_params([ModelParams("Example", False, [ModelConnectionParams(MONIKER, "Example connection")])], 0)


def test_gateway_only_operation_does_not_require_a_workspace(monkeypatch):
    params = _params()
    monkeypatch.setattr(params, "_validate_all_fabric_storage_params", lambda: True)
    assert params._validate_fabric_params()
    params.operation = Operation.DEPLOY_MODEL
    assert not params._validate_fabric_params()


def test_unknown_principal_type_is_not_silently_a_user():
    with pytest.raises(ValueError, match="principalType"):
        _params()._parse_identity_params({"givenName": "invalid", "objectId": USER_ID, "principalType": "Typo"})


def test_gateway_dispatch_executes_only_the_gateway_manager(monkeypatch):
    calls = []

    class DummyManager:
        def __init__(self, name):
            self.name = name

        async def execute(self):
            calls.append(self.name)

    class FakeFactory:
        def __init__(self, params):
            pass

        def create_fabric_cli(self):
            return SimpleNamespace(run_command=lambda command: "test-version")

        def create_fabric_gateway_manager(self):
            return DummyManager("gateway")

        def __getattr__(self, name):
            if name.startswith("create_"):
                return lambda: DummyManager("other")
            raise AttributeError(name)

    monkeypatch.setattr(operators, "ContainerizedManagementFactory", FakeFactory)
    asyncio.run(operators.CentralOperator(_params()).execute())
    assert calls == ["gateway"]


def test_factory_injects_shared_clients_without_live_io(monkeypatch):
    params = _params()
    params.common.endpoint = SimpleNamespace(power_bi="https://powerbi.example.invalid", analysis_service="https://analysis.example.invalid")
    factory = ContainerizedManagementFactory(params)
    gateway_client = factory.create_fabric_gateway_client()
    model_client = factory.create_semantic_model_client()
    monkeypatch.setattr(factory, "create_fabric_gateway_client", lambda: gateway_client)
    monkeypatch.setattr(factory, "create_semantic_model_client", lambda: model_client)
    monkeypatch.setattr(factory, "create_fabric_workspace_manager", lambda: SimpleNamespace())
    monkeypatch.setattr(factory, "create_fabric_folder_client", lambda: SimpleNamespace())
    monkeypatch.setattr(factory, "_create_cicd_token_credential", lambda: SimpleNamespace())
    assert factory.create_fabric_gateway_manager().gateway_client is gateway_client
    manager = factory.create_semantic_model_manager()
    assert manager.gateway_client is gateway_client
    assert manager.semantic_model_client is model_client
