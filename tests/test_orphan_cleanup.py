# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import fabric_cicd  # type: ignore[import-untyped]
import pytest
from fabric_cicd._items._base_publisher import ItemPublisher  # type: ignore[import-untyped]

from fabric_workspace_deployment.manager.fabric.cicd import FabricCicdManager
from fabric_workspace_deployment.manager.fabric.publishing import validate_orphan_cleanup_exceptions
from fabric_workspace_deployment.operations.operation_interfaces import OperationParams

RULES = [
    {"name": "ExternalPipeline", "type": "DataPipeline", "id": "11111111-1111-4111-8111-111111111111"},
    {"name": "ExternalAlert", "type": "Reflex", "id": "22222222-2222-4222-8222-222222222222"},
]


def workspace_listing(items):
    workspace = MagicMock(spec=fabric_cicd.FabricWorkspace)
    workspace.base_api_url = "https://example.test/v1/workspaces/workspace"
    workspace.endpoint = MagicMock()
    workspace.endpoint.invoke.return_value = {"body": {"value": items}, "header": {}}
    return workspace


def listed(rule):
    return {"displayName": rule["name"], "type": rule["type"], "id": rule["id"]}


def test_real_sdk_cleanup_preserves_exact_pairs_only_in_selected_workspace(monkeypatch):
    monkeypatch.setattr(fabric_cicd.constants, "FEATURE_FLAG", set())
    monkeypatch.setattr(ItemPublisher, "get_item_types_to_unpublish", lambda workspace: ["DataPipeline", "Reflex"])
    monkeypatch.setattr(ItemPublisher, "create", lambda *args: SimpleNamespace(has_dependency_tracking=False))
    selected = workspace_listing([listed(rule) for rule in RULES])
    selected.deployed_items = {"DataPipeline": {"ExternalPipeline": None, "ExternalPipeline_extra": None, "TemporaryProof": None}, "Reflex": {"ExternalAlert": None}}
    selected.repository_items = {}
    regex = validate_orphan_cleanup_exceptions(selected, RULES)
    fabric_cicd.unpublish_all_orphan_items(selected, item_name_exclude_regex=regex)
    deleted = {(call.kwargs["item_type"], call.kwargs["item_name"]) for call in selected._unpublish_item.call_args_list}
    assert deleted == {("DataPipeline", "ExternalPipeline_extra"), ("DataPipeline", "TemporaryProof")}

    other = workspace_listing([listed(rule) for rule in RULES])
    other.deployed_items = selected.deployed_items
    other.repository_items = {}
    fabric_cicd.unpublish_all_orphan_items(other)
    assert {(call.kwargs["item_type"], call.kwargs["item_name"]) for call in other._unpublish_item.call_args_list} == {("DataPipeline", "ExternalPipeline"), ("DataPipeline", "ExternalPipeline_extra"), ("DataPipeline", "TemporaryProof"), ("Reflex", "ExternalAlert")}


@pytest.mark.parametrize(
    "items",
    [
        [],
        [dict(listed(RULES[0]), type="Notebook"), listed(RULES[1])],
        [dict(listed(RULES[0]), id="33333333-3333-4333-8333-333333333333"), listed(RULES[1])],
        [listed(RULES[0]), listed(RULES[0]), listed(RULES[1])],
        [listed(RULES[0]), dict(listed(RULES[0]), type="Notebook"), listed(RULES[1])],
    ],
)
def test_invalid_or_ambiguous_live_identity_fails(items):
    with pytest.raises(ValueError):
        validate_orphan_cleanup_exceptions(workspace_listing(items), RULES)


def test_paginated_listing_detects_ambiguous_name():
    workspace = workspace_listing([])
    workspace.endpoint.invoke.side_effect = [
        {"body": {"value": [listed(rule) for rule in RULES], "continuationUri": workspace.base_api_url + "/items?continuationToken=next"}, "header": {}},
        {"body": {"value": [dict(listed(RULES[0]), type="Notebook")]}, "header": {}},
    ]
    with pytest.raises(ValueError, match="uniquely"):
        validate_orphan_cleanup_exceptions(workspace, RULES)


def test_regex_escapes_metacharacters_and_anchors_full_name():
    rule = {"name": "External[1].Pipeline", "type": "DataPipeline", "id": RULES[0]["id"]}
    workspace = workspace_listing([listed(rule)])
    pattern = validate_orphan_cleanup_exceptions(workspace, [rule])
    workspace.deployed_items = {"DataPipeline": {"External[1].Pipeline": None, "External1xPipeline": None, "External[1].Pipeline_extra": None}}
    workspace.repository_items = {}
    assert set(ItemPublisher.get_orphaned_items(workspace, "DataPipeline", item_name_exclude_regex=pattern)) == {"External1xPipeline", "External[1].Pipeline_extra"}


def template_data():
    return {"artifactsFolder": "artifacts", "parameterFilePath": "artifacts/parameter.yml", "itemTypesInScope": ["DataPipeline", "Reflex"], "environmentKey": "dev", "featureFlags": [], "unpublishOrphans": True, "customLibraries": [], "sparkJobDefinitions": [], "generator": {"monitoring": []}}


@pytest.mark.parametrize("invalid", [None, [], {}, [{"name": "x", "type": "Notebook", "id": RULES[0]["id"]}], [dict(RULES[0], id="bad")], [RULES[0], RULES[0]], [dict(RULES[0], extra="x")]])
def test_parser_fails_closed(invalid):
    params = OperationParams.__new__(OperationParams)
    params.logger = logging.getLogger(__name__)
    data = template_data()
    data["orphanCleanupExceptions"] = invalid
    with pytest.raises(ValueError):
        params._parse_fabric_workspace_template_params(data, ".")


def test_parser_default_and_valid_rules():
    params = OperationParams.__new__(OperationParams)
    params.logger = logging.getLogger(__name__)
    data = template_data()
    assert params._parse_fabric_workspace_template_params(data, ".").orphan_cleanup_exceptions == []
    data["orphanCleanupExceptions"] = RULES
    assert params._parse_fabric_workspace_template_params(data, ".").orphan_cleanup_exceptions == RULES
    data["unpublishOrphans"] = False
    with pytest.raises(ValueError):
        params._parse_fabric_workspace_template_params(data, ".")


def test_manager_preflights_before_publish_and_rechecks_before_cleanup(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    (artifacts / "parameter.yml").write_text("find_replace: []\n", encoding="utf-8")
    manager = FabricCicdManager(MagicMock(local=SimpleNamespace(root_folder=str(tmp_path)), endpoint=SimpleNamespace(cicd="https://example.test")), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
    template = SimpleNamespace(artifacts_folder="artifacts", parameter_file_path="artifacts/parameter.yml", environment_key="dev", feature_flags=[], item_types_in_scope=["DataPipeline", "Reflex"], unpublish_orphans=True, spark_job_definitions=[], bulk_publish=False, externally_managed_shortcuts=[], orphan_cleanup_exceptions=RULES)
    selected = workspace_listing([listed(rule) for rule in RULES])
    events = []
    selected.endpoint.invoke.side_effect = lambda **kwargs: events.append("read") or {"body": {"value": [listed(rule) for rule in RULES]}, "header": {}}
    monkeypatch.setattr(fabric_cicd, "FabricWorkspace", lambda **kwargs: selected)
    monkeypatch.setattr(fabric_cicd, "disable_file_logging", lambda: None)
    monkeypatch.setattr(fabric_cicd, "append_feature_flag", lambda flag: None)
    monkeypatch.setattr("fabric_workspace_deployment.manager.fabric.cicd.create_publish_workspace", lambda **kwargs: SimpleNamespace(item_type_in_scope=kwargs["item_type_in_scope"]))
    monkeypatch.setattr(fabric_cicd, "publish_all_items", lambda workspace: events.append("publish"))
    monkeypatch.setattr(fabric_cicd, "unpublish_all_orphan_items", lambda workspace, **kwargs: events.append(("cleanup", kwargs["item_name_exclude_regex"])))
    asyncio.run(manager._reconcile_artifacts("workspace-id", SimpleNamespace(), template, artifacts))
    assert events[0] == "read"
    assert events[-2] == "read"
    assert events[-1][0] == "cleanup"
    assert "publish" in events[1:-2]

    events.clear()
    selected.endpoint.invoke.side_effect = lambda **kwargs: events.append("read") or {"body": {"value": []}, "header": {}}
    with pytest.raises(ValueError, match="uniquely"):
        asyncio.run(manager._reconcile_artifacts("workspace-id", SimpleNamespace(), template, artifacts))
    assert events == ["read"]

    events.clear()
    reads = iter([[listed(rule) for rule in RULES], []])
    selected.endpoint.invoke.side_effect = lambda **kwargs: {"body": {"value": next(reads)}, "header": {}}
    with pytest.raises(ValueError, match="uniquely"):
        asyncio.run(manager._reconcile_artifacts("workspace-id", SimpleNamespace(), template, artifacts))
    assert "publish" in events
    assert all(not isinstance(event, tuple) or event[0] != "cleanup" for event in events)
