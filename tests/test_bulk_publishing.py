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
from fabric_workspace_deployment.manager.fabric.publishing import configure_publish_features, create_publish_workspace, validate_publish_metadata
from fabric_workspace_deployment.operations.operation_interfaces import CommonParams, FabricParams, OperationParams


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
    template = SimpleNamespace(artifacts_folder="artifacts", parameter_file_path="artifacts/parameter.yml", environment_key="ci", feature_flags=[], item_types_in_scope=["Notebook"], unpublish_orphans=False, spark_job_definitions=[], bulk_publish=True)
    return manager, template, artifacts, events, flags


def test_template_registers_bulk_opt_in_before_workspace_creation(publishing_context):
    manager, template, artifacts, events, flags = publishing_context

    asyncio.run(manager._reconcile_artifacts("workspace-id", SimpleNamespace(), template, artifacts))

    assert flags == ["enable_experimental_features", "enable_bulk_publish"]
    assert events == [("workspace", ["Notebook"], flags), ("publish", ["Notebook"])]


def test_template_standard_mode_ignores_legacy_bulk_flag_and_bulk_preflight(publishing_context):
    manager, template, artifacts, events, flags = publishing_context
    template.bulk_publish = False
    template.feature_flags = ["enable_bulk_publish", "enable_shortcut_publish"]
    _write_platform(artifacts, "Legacy", "invalid", "Notebook")

    asyncio.run(manager._reconcile_artifacts("workspace-id", SimpleNamespace(), template, artifacts))

    assert "enable_bulk_publish" not in flags
    assert "enable_shortcut_publish" in flags
    assert events[-1] == ("publish", ["Notebook"])


@pytest.mark.parametrize("configured", [None, False, True])
def test_template_bulk_publish_parser_default_and_opt_in(configured):
    params = OperationParams.__new__(OperationParams)
    params.logger = logging.getLogger(__name__)
    data = {"artifactsFolder": "artifacts", "parameterFilePath": "artifacts/parameter.yml", "itemTypesInScope": ["Lakehouse"], "environmentKey": "dev", "featureFlags": [], "unpublishOrphans": False, "customLibraries": [], "sparkJobDefinitions": [], "generator": {"monitoring": []}}
    if configured is not None:
        data["bulkPublish"] = configured

    assert params._parse_fabric_workspace_template_params(data, ".").bulk_publish is (configured is True)


@pytest.mark.parametrize("configured", [None, 1, 0, "true", "false", [], {}])
def test_template_bulk_publish_parser_rejects_non_boolean(configured):
    params = OperationParams.__new__(OperationParams)
    with pytest.raises(ValueError, match="bulkPublish must be a boolean"):
        params._parse_fabric_workspace_template_params({"bulkPublish": configured}, ".")


@pytest.mark.parametrize("order", [(True, False), (False, True)])
def test_upstream_bulk_flag_cannot_opt_in_another_workspace(tmp_path, monkeypatch, order):
    from fabric_cicd._items._base_publisher import ItemPublisher  # type: ignore[import-untyped]

    monkeypatch.setattr(fabric_cicd.constants, "FEATURE_FLAG", set())
    monkeypatch.setattr(fabric_cicd.FabricWorkspace, "__init__", lambda self, **kwargs: None)
    monkeypatch.setattr(ItemPublisher, "get_item_types_to_publish", lambda workspace: [])
    bulk = MagicMock(return_value=[])
    monkeypatch.setattr(ItemPublisher, "publish_all_bulk", bulk)
    configure_publish_features([], bulk_publish=True)
    for opt_in in order:
        workspace = create_publish_workspace(workspace_id="12345678-1234-5678-abcd-1234567890ab", environment="ci", repository_directory=str(tmp_path), item_type_in_scope=["Notebook"], token_credential=MagicMock(), bulk_publish=opt_in)
        workspace.workspace_id = "12345678-1234-5678-abcd-1234567890ab"
        workspace.item_type_in_scope = ["Notebook"]
        workspace.contains_param_item_vars = False
        workspace.endpoint = MagicMock()
        workspace.endpoint.invoke.return_value = {"body": {"capacityId": "capacity-id"}}
        for method in ("_refresh_deployed_folders", "_refresh_repository_folders", "_publish_folders", "_refresh_deployed_items", "_refresh_repository_items"):
            monkeypatch.setattr(workspace, method, MagicMock())
        fabric_cicd.publish_all_items(workspace)
        assert workspace.bulk_publish_enabled is opt_in
    assert bulk.call_count == 1


def test_template_preserves_configured_flags_without_duplicates(publishing_context):
    manager, template, artifacts, events, flags = publishing_context
    template.feature_flags = ["enable_shortcut_publish", "enable_bulk_publish", "enable_shortcut_publish", "enable_experimental_features"]

    asyncio.run(manager._reconcile_artifacts("workspace-id", SimpleNamespace(), template, artifacts))

    assert flags == ["enable_shortcut_publish", "enable_experimental_features", "enable_bulk_publish"]
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


def _write_platform(root: Path, name: str, logical_id, item_type: str = "DataPipeline") -> Path:
    item_dir = root / f"{name}.{item_type}"
    item_dir.mkdir(parents=True)
    path = item_dir / ".platform"
    path.write_text(json.dumps({"metadata": {"type": item_type, "displayName": name}, "config": {"logicalId": logical_id}}), encoding="utf-8")
    return path


@pytest.mark.parametrize("logical_id", ["acff42ae6-074f-48a5-94b6-7b9da79d4622", "", None, 123, "00000000-0000-0000-0000-000000000000", "cff42ae6074f48a594b67b9da79d4622", "{cff42ae6-074f-48a5-94b6-7b9da79d4622}"])
def test_metadata_rejects_invalid_logical_id_with_source_path(tmp_path, logical_id):
    path = _write_platform(tmp_path, "90_maintenance_ev2_db", logical_id)
    original = path.read_bytes()

    with pytest.raises(ValueError, match="Invalid config.logicalId") as error:
        validate_publish_metadata(tmp_path, ["DataPipeline"])

    assert str(path) in str(error.value)
    assert "8-4-4-4-12" in str(error.value)
    assert path.read_bytes() == original


def test_metadata_accepts_valid_ids_and_ignores_out_of_scope_items(tmp_path):
    _write_platform(tmp_path, "Pipeline", "CFF42AE6-074F-48A5-94B6-7B9DA79D4622")
    _write_platform(tmp_path, "Ignored", "invalid", "Notebook")

    validate_publish_metadata(tmp_path, ["DataPipeline"])


def test_metadata_rejects_case_insensitive_duplicate_logical_ids(tmp_path):
    first = _write_platform(tmp_path, "First", "cff42ae6-074f-48a5-94b6-7b9da79d4622")
    second = _write_platform(tmp_path, "Second", "CFF42AE6-074F-48A5-94B6-7B9DA79D4622", "Notebook")

    with pytest.raises(ValueError, match="Duplicate config.logicalId") as error:
        validate_publish_metadata(tmp_path, ["DataPipeline", "Notebook"])

    assert str(first) in str(error.value)
    assert str(second) in str(error.value)


@pytest.mark.parametrize("content", ["{", "[]", '{"config": {}}', '{"metadata": {"type": "DataPipeline"}}'])
def test_metadata_rejects_malformed_system_files_with_source_path(tmp_path, content):
    path = tmp_path / ".platform"
    path.write_text(content, encoding="utf-8")

    with pytest.raises(ValueError) as error:
        validate_publish_metadata(tmp_path, ["DataPipeline"])

    assert str(path) in str(error.value)


def test_template_validates_all_batches_before_workspace_creation(publishing_context):
    manager, template, artifacts, events, _flags = publishing_context
    template.item_types_in_scope = ["DataPipeline", "Reflex"]
    template.unpublish_orphans = True
    _write_platform(artifacts, "InvalidLaterBatch", "invalid", "Reflex")

    with pytest.raises(ValueError, match="Invalid config.logicalId"):
        asyncio.run(manager._reconcile_artifacts("workspace-id", SimpleNamespace(), template, artifacts))

    assert events == []
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
    configure_publish_features([], bulk_publish=True)
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
    configure_publish_features([], bulk_publish=True)
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


@pytest.mark.parametrize(("existing", "schema"), [(True, None), (True, "dbo"), (True, "custom"), (False, None)])
def test_real_bulk_publisher_preserves_existing_lakehouse_schema_and_source(tmp_path, monkeypatch, existing, schema, caplog):
    monkeypatch.setattr(fabric_cicd.constants, "FEATURE_FLAG", set())
    configure_publish_features([], bulk_publish=True)
    item_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    platform_path = _write_platform(tmp_path, "Source", "11111111-1111-1111-1111-111111111111", "Lakehouse")
    metadata_path = platform_path.parent / "lakehouse.metadata.json"
    metadata_path.write_text('{"defaultSchema":"dbo"}', encoding="utf-8")
    original = metadata_path.read_bytes()
    (tmp_path / "parameter.yml").write_text("find_replace: []\n", encoding="utf-8")
    deployed_item = {"id": item_id, "type": "Lakehouse", "displayName": "Source", "description": ""}
    deployed = [deployed_item] if existing else []
    requests = []

    def invoke(method, url, **kwargs):
        response = {"header": {}, "body": {"value": [], "capacityId": "capacity-id"}}
        if method == "GET" and url.endswith("/items"):
            response["body"]["value"] = list(deployed)
        elif method == "GET" and url.endswith("/lakehouses"):
            response["body"]["value"] = [{"id": item_id, "properties": {"defaultSchema": schema}}]
        elif method == "POST" and url.endswith("/items/bulkImportDefinitions"):
            requests.append(kwargs["body"])
            if not deployed:
                deployed.append(deployed_item)
            response["body"]["importItemDefinitionsDetails"] = [{"itemType": "Lakehouse", "itemDisplayName": "Source", "itemId": item_id, "operationType": "Update" if existing else "Create"}]
        else:
            assert method == "GET", f"Unexpected request: {method} {url}"
        return response

    endpoint = MagicMock()
    endpoint.invoke.side_effect = invoke
    monkeypatch.setattr("fabric_cicd.fabric_workspace.FabricEndpoint", lambda **kwargs: endpoint)
    workspace = create_publish_workspace(workspace_id="12345678-1234-5678-abcd-1234567890ab", environment="ci", repository_directory=str(tmp_path), item_type_in_scope=["Lakehouse"], token_credential=AzureCliCredential(), bulk_publish=True)

    with caplog.at_level(logging.INFO, logger="fabric_workspace_deployment.manager.fabric.publishing"):
        fabric_cicd.publish_all_items(workspace)

    assert len(requests) == 1
    parts = requests[0]["definitionParts"]
    assert any(part["path"].endswith("/.platform") for part in parts)
    metadata_parts = [part for part in parts if part["path"].endswith("/lakehouse.metadata.json")]
    assert len(metadata_parts) == 1
    expected_metadata = {"defaultSchema": schema} if existing and schema else {} if existing else {"defaultSchema": "dbo"}
    assert json.loads(base64.b64decode(metadata_parts[0]["payload"])) == expected_metadata
    assert ("Preserving existing Lakehouse 'Source' default schema" in caplog.text) is existing
    assert metadata_path.read_bytes() == original
    lakehouse_reads = [call for call in endpoint.invoke.call_args_list if call.kwargs.get("url", "").endswith("/lakehouses")]
    assert len(lakehouse_reads) == int(existing)


@pytest.mark.parametrize("continuation_location", ["body", "header"])
def test_lakehouse_schema_snapshot_paginates_and_is_cached(monkeypatch, continuation_location):
    endpoint = MagicMock()
    first_page: dict[str, dict[str, object]] = {"header": {}, "body": {"value": [{"id": "first", "properties": {}}]}}
    first_page[continuation_location]["continuationUri"] = "https://api.fabric.microsoft.com/next"
    endpoint.invoke.side_effect = [
        first_page,
        {"header": {}, "body": {"value": [{"id": "second", "properties": {"defaultSchema": "custom"}}]}},
    ]

    class FakeWorkspace:
        def __init__(self, **kwargs):
            self.endpoint = endpoint
            self.base_api_url = "https://api.fabric.microsoft.com/v1/workspaces/workspace-id"

    monkeypatch.setattr(fabric_cicd, "FabricWorkspace", FakeWorkspace)
    workspace = create_publish_workspace(workspace_id="workspace-id", environment="ci", repository_directory="artifacts", item_type_in_scope=["Lakehouse"], token_credential=MagicMock())

    assert workspace._get_existing_lakehouse_schemas() == {"first": None, "second": "custom"}
    assert workspace._get_existing_lakehouse_schemas() == {"first": None, "second": "custom"}
    assert endpoint.invoke.call_count == 2
    assert endpoint.invoke.call_args.kwargs["url"] == "https://api.fabric.microsoft.com/next"


@pytest.mark.parametrize("response", [{"id": "first"}, {"id": "first", "properties": {"defaultSchema": 123}}])
def test_lakehouse_schema_snapshot_rejects_invalid_api_state(monkeypatch, response):
    endpoint = MagicMock()
    endpoint.invoke.return_value = {"header": {}, "body": {"value": [response]}}

    class FakeWorkspace:
        def __init__(self, **kwargs):
            self.endpoint = endpoint
            self.base_api_url = "https://api.fabric.microsoft.com/v1/workspaces/workspace-id"

    monkeypatch.setattr(fabric_cicd, "FabricWorkspace", FakeWorkspace)
    workspace = create_publish_workspace(workspace_id="workspace-id", environment="ci", repository_directory="artifacts", item_type_in_scope=["Lakehouse"], token_credential=MagicMock())

    with pytest.raises(ValueError, match="Lakehouse 'first' response"):
        workspace._get_existing_lakehouse_schemas()


def test_real_bulk_publisher_resolves_spark_job_logical_attachment_ids(tmp_path, monkeypatch):
    monkeypatch.setattr(fabric_cicd.constants, "FEATURE_FLAG", set())
    configure_publish_features([], bulk_publish=True)
    source_logical_id = "11111111-1111-1111-1111-111111111111"
    source_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    job_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    source_platform = _write_platform(tmp_path, "Source", source_logical_id, "Lakehouse")
    (source_platform.parent / "lakehouse.metadata.json").write_text("{}", encoding="utf-8")
    job_platform = _write_platform(tmp_path, "Job", "22222222-2222-2222-2222-222222222222", "SparkJobDefinition")
    job_path = job_platform.parent / "SparkJobDefinitionV1.json"
    job_path.write_text(json.dumps({"defaultLakehouseArtifactId": source_logical_id, "additionalLakehouseIds": [source_logical_id]}), encoding="utf-8")
    original = job_path.read_bytes()
    (tmp_path / "parameter.yml").write_text("find_replace: []\n", encoding="utf-8")
    deployed = [{"id": source_id, "type": "Lakehouse", "displayName": "Source", "description": ""}]
    requests = []

    def invoke(method, url, **kwargs):
        response = {"header": {}, "body": {"value": [], "capacityId": "capacity-id"}}
        if method == "GET" and url.endswith("/items"):
            response["body"]["value"] = list(deployed)
        elif method == "GET" and url.endswith("/lakehouses"):
            response["body"]["value"] = [{"id": source_id, "properties": {}}]
        elif method == "POST" and url.endswith("/items/bulkImportDefinitions"):
            requests.append(kwargs["body"])
            deployed.append({"id": job_id, "type": "SparkJobDefinition", "displayName": "Job", "description": ""})
            response["body"]["importItemDefinitionsDetails"] = [
                {"itemType": "Lakehouse", "itemDisplayName": "Source", "itemId": source_id, "operationType": "Update"},
                {"itemType": "SparkJobDefinition", "itemDisplayName": "Job", "itemId": job_id, "operationType": "Create"},
            ]
        else:
            assert method == "GET", f"Unexpected request: {method} {url}"
        return response

    endpoint = MagicMock()
    endpoint.invoke.side_effect = invoke
    monkeypatch.setattr("fabric_cicd.fabric_workspace.FabricEndpoint", lambda **kwargs: endpoint)
    workspace = create_publish_workspace(workspace_id="12345678-1234-5678-abcd-1234567890ab", environment="ci", repository_directory=str(tmp_path), item_type_in_scope=["Lakehouse", "SparkJobDefinition"], token_credential=AzureCliCredential(), bulk_publish=True)

    fabric_cicd.publish_all_items(workspace)

    assert len(requests) == 1
    job_part = next(part for part in requests[0]["definitionParts"] if part["path"].endswith("/SparkJobDefinitionV1.json"))
    assert json.loads(base64.b64decode(job_part["payload"])) == {"defaultLakehouseArtifactId": source_id, "additionalLakehouseIds": [source_id]}
    platform_part = next(part for part in requests[0]["definitionParts"] if part["path"].endswith("/Job.SparkJobDefinition/.platform"))
    assert json.loads(base64.b64decode(platform_part["payload"]))["config"]["logicalId"] == "22222222-2222-2222-2222-222222222222"
    assert job_path.read_bytes() == original
