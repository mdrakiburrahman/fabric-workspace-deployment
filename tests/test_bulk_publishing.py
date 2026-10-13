# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import base64
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import fabric_cicd  # type: ignore[import-untyped]
import pytest
from azure.identity import AzureCliCredential

from fabric_workspace_deployment.manager.fabric.cicd import FabricCicdManager
from fabric_workspace_deployment.manager.fabric.publishing import configure_publish_features
from fabric_workspace_deployment.operations.operation_interfaces import CommonParams, FabricParams


@pytest.fixture
def publishing_context(tmp_path: Path, monkeypatch):
    events = []
    flags: list[str] = []
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    (artifacts / "parameter.yml").write_text("find_replace: []\n", encoding="utf-8")
    common = MagicMock(spec=CommonParams, local=SimpleNamespace(root_folder=str(tmp_path)), fabric=FabricParams([], []), endpoint=SimpleNamespace(cicd="https://api.fabric.microsoft.com"))
    manager = FabricCicdManager(common, MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
    monkeypatch.setattr(manager, "_post_process_environment_spark_settings", AsyncMock(side_effect=lambda *args: events.append("spark-settings")))
    monkeypatch.setattr(manager, "_update_spark_job_definition_configs", AsyncMock(side_effect=lambda *args: events.append("spark-job-config")))

    class FakeWorkspace:
        def __init__(self, **kwargs):
            events.append(("workspace", kwargs["item_type_in_scope"], list(flags)))
            self.item_type_in_scope = kwargs["item_type_in_scope"]

    def publish(workspace):
        events.append(("publish", workspace.item_type_in_scope))

    def unpublish(workspace):
        events.append(("unpublish", workspace.item_type_in_scope))

    monkeypatch.setattr(fabric_cicd, "disable_file_logging", lambda: None)
    monkeypatch.setattr(fabric_cicd, "append_feature_flag", flags.append)
    monkeypatch.setattr(fabric_cicd, "FabricWorkspace", FakeWorkspace)
    monkeypatch.setattr(fabric_cicd, "publish_all_items", publish)
    monkeypatch.setattr(fabric_cicd, "unpublish_all_orphan_items", unpublish)
    template = SimpleNamespace(artifacts_folder="artifacts", parameter_file_path="artifacts/parameter.yml", environment_key="ci", feature_flags=[], item_types_in_scope=["Notebook"], unpublish_orphans=False, spark_job_definitions=[])
    return manager, template, artifacts, events, flags


def test_template_registers_bulk_defaults_before_workspace_creation(publishing_context):
    manager, template, artifacts, events, flags = publishing_context

    asyncio.run(manager._reconcile_artifacts("workspace-id", SimpleNamespace(), template, artifacts))

    assert flags == ["enable_experimental_features", "enable_bulk_publish"]
    assert events == [("workspace", ["Notebook"], flags), ("publish", ["Notebook"])]


def test_template_preserves_configured_flags_without_duplicates(publishing_context):
    manager, template, artifacts, events, flags = publishing_context
    template.feature_flags = ["enable_shortcut_publish", "enable_bulk_publish", "enable_shortcut_publish", "enable_experimental_features"]

    asyncio.run(manager._reconcile_artifacts("workspace-id", SimpleNamespace(), template, artifacts))

    assert flags == ["enable_shortcut_publish", "enable_bulk_publish", "enable_experimental_features"]
    assert events[0][2] == flags


def test_template_preserves_reflex_order_post_processing_and_orphans(publishing_context):
    manager, template, artifacts, events, flags = publishing_context
    template.item_types_in_scope = ["Reflex", "Environment", "DataPipeline", "Lakehouse"]
    template.unpublish_orphans = True
    template.spark_job_definitions = [SimpleNamespace()]

    asyncio.run(manager._reconcile_artifacts("workspace-id", SimpleNamespace(), template, artifacts))

    batch = ["DataPipeline", "Environment", "Lakehouse"]
    assert events == [
        ("workspace", batch, flags),
        ("publish", batch),
        "spark-settings",
        ("workspace", ["Reflex"], flags),
        ("publish", ["Reflex"]),
        ("workspace", template.item_types_in_scope, flags),
        ("unpublish", template.item_types_in_scope),
        "spark-job-config",
    ]
    assert flags == ["enable_experimental_features", "enable_bulk_publish"]


def test_template_propagates_bulk_publish_failure_without_cleanup(publishing_context, monkeypatch):
    manager, template, artifacts, events, _flags = publishing_context
    template.unpublish_orphans = True

    def fail_publish(workspace):
        raise RuntimeError("bulk import failed")

    monkeypatch.setattr(fabric_cicd, "publish_all_items", fail_publish)

    with pytest.raises(RuntimeError, match="bulk import failed"):
        asyncio.run(manager._reconcile_artifacts("workspace-id", SimpleNamespace(), template, artifacts))

    assert len(events) == 1
    manager._post_process_environment_spark_settings.assert_not_called()
    manager._update_spark_job_definition_configs.assert_not_called()


@pytest.mark.parametrize(
    ("scope", "parameter", "bulk_enabled", "warning"),
    [
        (["Notebook"], {"find_replace": [{"find_value": "source", "replace_value": {"ci": "$items.Lakehouse.Source.id"}, "item_type": "Notebook"}]}, True, None),
        (["Notebook"], {"find_replace": [{"find_value": "source", "replace_value": {"ci": "$items.Lakehouse.Source.id"}}]}, False, "without an item_type"),
        (["Warehouse"], {}, False, "unsupported item types: Warehouse"),
        (["DataBuildToolJob"], {}, False, "unsupported item types: DataBuildToolJob"),
    ],
)
def test_upstream_bulk_gate_preserves_filtered_replacements_and_logged_fallback(scope, parameter, bulk_enabled, warning, monkeypatch, caplog):
    from fabric_cicd._items._base_publisher import ItemPublisher  # type: ignore[import-untyped]

    monkeypatch.setattr(fabric_cicd.constants, "FEATURE_FLAG", set())
    monkeypatch.setattr(logging.getLogger("fabric_cicd"), "propagate", True)
    configure_publish_features([])
    workspace = fabric_cicd.FabricWorkspace.__new__(fabric_cicd.FabricWorkspace)
    workspace.workspace_id = "12345678-1234-5678-abcd-1234567890ab"
    workspace.item_type_in_scope = scope
    workspace.environment = "ci"
    workspace.environment_parameter = parameter
    workspace.contains_param_item_vars = bool(parameter)
    workspace.endpoint = MagicMock()
    workspace.endpoint.invoke.return_value = {"body": {"capacityId": "capacity-id"}}
    for method in ("_refresh_deployed_folders", "_refresh_repository_folders", "_publish_folders", "_refresh_deployed_items", "_refresh_repository_items"):
        monkeypatch.setattr(workspace, method, MagicMock())
    monkeypatch.setattr(ItemPublisher, "get_item_types_to_publish", lambda workspace: [])
    bulk_publish = MagicMock(return_value=[])
    monkeypatch.setattr(ItemPublisher, "publish_all_bulk", bulk_publish)

    fabric_cicd.publish_all_items(workspace)

    assert workspace.bulk_publish_enabled is bulk_enabled
    assert bulk_publish.call_count == int(bulk_enabled)
    if warning:
        assert "Falling back to standard deployment" in caplog.text
        assert warning in caplog.text
    else:
        assert "Falling back to standard deployment" not in caplog.text


def test_real_bulk_publisher_resolves_new_item_id_in_later_batch(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(fabric_cicd.constants, "FEATURE_FLAG", set())
    configure_publish_features([])
    source_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    notebook_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    for name, item_type, logical_id in (
        ("Source", "Lakehouse", "11111111-1111-1111-1111-111111111111"),
        ("Consumer", "Notebook", "22222222-2222-2222-2222-222222222222"),
    ):
        item_dir = tmp_path / f"{name}.{item_type}"
        item_dir.mkdir()
        (item_dir / ".platform").write_text(json.dumps({"metadata": {"type": item_type, "displayName": name}, "config": {"logicalId": logical_id}}), encoding="utf-8")
        (item_dir / "content.txt").write_text("source-id", encoding="utf-8")
    (tmp_path / "parameter.yml").write_text("find_replace:\n  - find_value: source-id\n    replace_value:\n      ci: $items.Lakehouse.Source.id\n    item_type: Notebook\n", encoding="utf-8")
    requests = []
    deployed: list[dict[str, str]] = []

    def invoke(method, url, **kwargs):
        response = {"header": {}, "body": {"value": [], "capacityId": "capacity-id"}}
        if method == "GET" and url.endswith("/items"):
            response["body"]["value"] = list(deployed)
        elif method == "POST" and url.endswith("/items/bulkImportDefinitions"):
            requests.append(kwargs["body"])
            item_type, name, item_id = ("Lakehouse", "Source", source_id) if len(requests) == 1 else ("Notebook", "Consumer", notebook_id)
            deployed.append({"id": item_id, "type": item_type, "displayName": name, "description": ""})
            response["body"]["importItemDefinitionsDetails"] = [{"itemType": item_type, "itemDisplayName": name, "itemId": item_id, "operationType": "Create"}]
        else:
            assert method == "GET", f"Unexpected request: {method} {url}"
        return response

    endpoint = MagicMock()
    endpoint.invoke.side_effect = invoke
    monkeypatch.setattr("fabric_cicd.fabric_workspace.FabricEndpoint", lambda **kwargs: endpoint)
    workspace = fabric_cicd.FabricWorkspace(workspace_id="12345678-1234-5678-abcd-1234567890ab", environment="ci", repository_directory=str(tmp_path), item_type_in_scope=["Lakehouse", "Notebook"], token_credential=AzureCliCredential())

    fabric_cicd.publish_all_items(workspace)

    assert workspace.bulk_publish_enabled
    assert len(requests) == 2
    assert all(part["path"].startswith("/Source.Lakehouse/") for part in requests[0]["definitionParts"])
    notebook_content = next(part for part in requests[1]["definitionParts"] if part["path"].endswith("/content.txt"))
    assert base64.b64decode(notebook_content["payload"]).decode("utf-8") == source_id
    assert workspace.repository_items["Notebook"]["Consumer"].guid == notebook_id
