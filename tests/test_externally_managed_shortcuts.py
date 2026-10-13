# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import json
import logging
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import fabric_cicd  # type: ignore[import-untyped]
import pytest
from fabric_cicd._items import _lakehouse  # type: ignore[import-untyped]

from fabric_workspace_deployment.manager.fabric.cicd import FabricCicdManager
from fabric_workspace_deployment.manager.fabric.publishing import create_publish_workspace, install_shortcut_preservation, validate_externally_managed_shortcuts
from fabric_workspace_deployment.operations.operation_interfaces import CommonParams, ExternallyManagedShortcuts, FabricParams, OperationParams

PUBLISHING_LOGGER = "fabric_workspace_deployment.manager.fabric.publishing"
WORKSPACE_ID = "12345678-1234-5678-abcd-1234567890ab"
LAKEHOUSE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
DEPLOYED = ["/Files/onelake", "/Tables/dbo/aks_a", "/Tables/dbo/aks_b", "/Tables/legacy/stale"]
GIT_SHORTCUTS = [{"name": "onelake", "path": "/Files", "target": {"type": "AdlsGen2", "adlsGen2": {"location": "https://account.dfs.core.windows.net", "subpath": "/onelake"}}}]


@pytest.fixture(autouse=True)
def restore_upstream_shortcut_listing(monkeypatch):
    # The preservation hook patches a process-global upstream function; monkeypatch restores it after each test.
    monkeypatch.setattr(_lakehouse, "list_deployed_shortcuts", _lakehouse.list_deployed_shortcuts)


def _params() -> OperationParams:
    params = OperationParams.__new__(OperationParams)
    params.logger = logging.getLogger(__name__)
    return params


def _write_platform(root: Path, name: str, item_type: str = "Lakehouse") -> Path:
    item_dir = root / f"{name}.{item_type}"
    item_dir.mkdir(parents=True, exist_ok=True)
    path = item_dir / ".platform"
    path.write_text(json.dumps({"metadata": {"type": item_type, "displayName": name}, "config": {"logicalId": str(uuid.uuid4())}}), encoding="utf-8")
    return path


def test_parser_defaults_to_no_rules():
    assert _params()._parse_externally_managed_shortcuts([]) == []


def test_parser_minimal_entry_exempts_the_whole_lakehouse():
    assert _params()._parse_externally_managed_shortcuts([{"lakehouse": "protobus_db_eh"}]) == [ExternallyManagedShortcuts("protobus_db_eh", ("/",), None)]


def test_parser_full_entry_dedupes_prefixes_and_keeps_order():
    rules = _params()._parse_externally_managed_shortcuts([{"lakehouse": "protobus_db_eh", "pathPrefixes": ["/Tables/dbo/", "/Tables/dbo/", "/Files/runtime/"], "reason": "created at runtime"}])

    assert rules == [ExternallyManagedShortcuts("protobus_db_eh", ("/Tables/dbo/", "/Files/runtime/"), "created at runtime")]


@pytest.mark.parametrize(
    ("data", "message"),
    [
        (None, "must be a list"),
        ({}, "must be a list"),
        ("protobus_db_eh", "must be a list"),
        (["protobus_db_eh"], r"\[0\] must be an object"),
        ([{}], "lakehouse must be a non-empty string"),
        ([{"lakehouse": ""}], "lakehouse must be a non-empty string"),
        ([{"lakehouse": "  "}], "lakehouse must be a non-empty string"),
        ([{"lakehouse": 1}], "lakehouse must be a non-empty string"),
        ([{"lakehouse": "a", "lakehous": "b"}], "unknown field"),
        ([{"lakehouse": "a"}, {"lakehouse": "a"}], "listed more than once"),
        ([{"lakehouse": "a", "pathPrefixes": []}], "pathPrefixes must be a non-empty list"),
        ([{"lakehouse": "a", "pathPrefixes": "/Tables/"}], "pathPrefixes must be a non-empty list"),
        ([{"lakehouse": "a", "pathPrefixes": ["Tables/"]}], "pathPrefixes must be a non-empty list"),
        ([{"lakehouse": "a", "pathPrefixes": [1]}], "pathPrefixes must be a non-empty list"),
        ([{"lakehouse": "a", "reason": 1}], "reason must be a string"),
    ],
)
def test_parser_rejects_invalid_configuration(data, message):
    with pytest.raises(ValueError, match=message):
        _params()._parse_externally_managed_shortcuts(data)


def test_template_parser_wires_rules_and_validates_them_first():
    with pytest.raises(ValueError, match="externallyManagedShortcuts must be a list"):
        _params()._parse_fabric_workspace_template_params({"externallyManagedShortcuts": "protobus_db_eh"}, ".")

    data = {"artifactsFolder": "artifacts", "parameterFilePath": "artifacts/parameter.yml", "itemTypesInScope": ["Lakehouse"], "environmentKey": "dev", "featureFlags": [], "unpublishOrphans": False, "customLibraries": [], "sparkJobDefinitions": [], "generator": {"monitoring": []}}
    assert _params()._parse_fabric_workspace_template_params(data, ".").externally_managed_shortcuts == []

    data["externallyManagedShortcuts"] = [{"lakehouse": "protobus_db_eh", "pathPrefixes": ["/Tables/dbo/"]}]
    assert _params()._parse_fabric_workspace_template_params(data, ".").externally_managed_shortcuts == [ExternallyManagedShortcuts("protobus_db_eh", ("/Tables/dbo/",))]


def test_validation_accepts_existing_lakehouse(tmp_path, caplog):
    _write_platform(tmp_path, "protobus_db_eh")

    with caplog.at_level(logging.INFO, logger=PUBLISHING_LOGGER):
        validate_externally_managed_shortcuts(tmp_path, ["Lakehouse"], ["enable_shortcut_publish"], [ExternallyManagedShortcuts("protobus_db_eh", ("/Tables/dbo/",))])

    assert "protobus_db_eh['/Tables/dbo/']" in caplog.text
    assert "has no effect" not in caplog.text


def test_validation_rejects_unknown_lakehouse_and_lists_the_available_ones(tmp_path):
    _write_platform(tmp_path, "arc_apps_db_eh")

    with pytest.raises(ValueError, match="'protobus_db_eh' which is not in") as error:
        validate_externally_managed_shortcuts(tmp_path, ["Lakehouse"], ["enable_shortcut_publish"], [ExternallyManagedShortcuts("protobus_db_eh")])

    assert "arc_apps_db_eh" in str(error.value)


@pytest.mark.parametrize(("platform_name", "item_type"), [("protobus_db_eh", "Notebook"), ("Protobus_DB_EH", "Lakehouse")])
def test_validation_requires_an_exact_lakehouse_match(tmp_path, platform_name, item_type):
    _write_platform(tmp_path, platform_name, item_type)

    with pytest.raises(ValueError, match="which is not in"):
        validate_externally_managed_shortcuts(tmp_path, ["Lakehouse"], ["enable_shortcut_publish"], [ExternallyManagedShortcuts("protobus_db_eh")])


def test_validation_reports_malformed_system_files_with_their_path(tmp_path):
    path = _write_platform(tmp_path, "protobus_db_eh")
    path.write_text("{", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid JSON") as error:
        validate_externally_managed_shortcuts(tmp_path, ["Lakehouse"], ["enable_shortcut_publish"], [ExternallyManagedShortcuts("protobus_db_eh")])

    assert str(path) in str(error.value)


@pytest.mark.parametrize(
    ("scope", "flags", "warning"),
    [
        (["DataPipeline"], ["enable_shortcut_publish"], "Lakehouse is not in template.itemTypesInScope"),
        (["Lakehouse"], [], "includes enable_shortcut_publish"),
    ],
)
def test_validation_warns_when_rules_cannot_take_effect(tmp_path, caplog, scope, flags, warning):
    _write_platform(tmp_path, "protobus_db_eh")

    with caplog.at_level(logging.WARNING, logger=PUBLISHING_LOGGER):
        validate_externally_managed_shortcuts(tmp_path, scope, flags, [ExternallyManagedShortcuts("protobus_db_eh")])

    assert warning in caplog.text


def test_validation_without_rules_does_not_read_the_artifacts(tmp_path):
    validate_externally_managed_shortcuts(tmp_path / "missing", [], [], [])


def _install_over_fake_listing(monkeypatch, deployed):
    monkeypatch.setattr(_lakehouse, "list_deployed_shortcuts", lambda workspace, item: list(deployed))
    install_shortcut_preservation()
    return _lakehouse.list_deployed_shortcuts


def test_listing_hides_only_paths_under_the_configured_prefixes(monkeypatch):
    listing = _install_over_fake_listing(monkeypatch, DEPLOYED)
    workspace = SimpleNamespace(externally_managed_shortcuts={"protobus_db_eh": ExternallyManagedShortcuts("protobus_db_eh", ("/Tables/dbo/",))})

    assert listing(workspace, SimpleNamespace(name="protobus_db_eh")) == ["/Files/onelake", "/Tables/legacy/stale"]


def test_listing_hides_everything_when_no_prefix_is_configured(monkeypatch):
    listing = _install_over_fake_listing(monkeypatch, DEPLOYED)
    workspace = SimpleNamespace(externally_managed_shortcuts={"protobus_db_eh": ExternallyManagedShortcuts("protobus_db_eh")})

    assert listing(workspace, SimpleNamespace(name="protobus_db_eh")) == []


def test_listing_is_unchanged_for_other_lakehouses_and_workspaces_without_rules(monkeypatch):
    listing = _install_over_fake_listing(monkeypatch, DEPLOYED)
    item = SimpleNamespace(name="protobus_db_eh")
    other_lakehouse = SimpleNamespace(externally_managed_shortcuts={"arc_apps_db_eh": ExternallyManagedShortcuts("arc_apps_db_eh")})

    assert listing(other_lakehouse, item) == DEPLOYED
    assert listing(SimpleNamespace(externally_managed_shortcuts=None), item) == DEPLOYED
    assert listing(SimpleNamespace(), item) == DEPLOYED


def test_installation_is_idempotent(monkeypatch):
    calls = []

    def upstream(workspace, item):
        calls.append(item.name)
        return []

    monkeypatch.setattr(_lakehouse, "list_deployed_shortcuts", upstream)
    install_shortcut_preservation()
    installed = _lakehouse.list_deployed_shortcuts
    install_shortcut_preservation()

    assert _lakehouse.list_deployed_shortcuts is installed
    installed(SimpleNamespace(), SimpleNamespace(name="protobus_db_eh"))
    assert calls == ["protobus_db_eh"]


class _CleanupResult(SimpleNamespace):
    deleted: list[str]
    published: list[str]


def _publish_shortcuts(monkeypatch, deployed, git_shortcuts, rules, lakehouse="protobus_db_eh") -> _CleanupResult:
    """Runs upstream's real ShortcutPublisher against a fake Fabric endpoint."""
    monkeypatch.setattr(fabric_cicd.FabricWorkspace, "__init__", lambda self, **kwargs: None)
    workspace = create_publish_workspace(workspace_id=WORKSPACE_ID, environment="ci", repository_directory="unused", item_type_in_scope=["Lakehouse"], token_credential=MagicMock(), externally_managed_shortcuts=rules)
    result = _CleanupResult(deleted=[], published=[])

    def invoke(method, url, **kwargs):
        if method == "GET":
            return {"header": {}, "body": {"value": [{"path": path.rsplit("/", 1)[0], "name": path.rsplit("/", 1)[1]} for path in deployed]}}
        if method == "DELETE":
            result.deleted.append("/" + url.split("/shortcuts/", 1)[1].lstrip("/"))
        if method == "POST":
            result.published.append(kwargs["body"]["name"])
        return {"header": {}, "body": {}}

    workspace.workspace_id = WORKSPACE_ID
    workspace._api_root_url = "https://api.fabric.microsoft.com"
    workspace.endpoint = SimpleNamespace(invoke=invoke)
    workspace.shortcut_exclude_regex = None
    workspace._replace_parameters = lambda file, item: file.contents
    workspace._replace_logical_ids = lambda contents, *_: contents
    workspace._replace_workspace_ids = lambda contents, *_: contents
    item = SimpleNamespace(guid=LAKEHOUSE_ID, name=lakehouse, item_files=[SimpleNamespace(name="shortcuts.metadata.json", contents=json.dumps(git_shortcuts))])

    _lakehouse.ShortcutPublisher(workspace, item).publish_all()
    return result


def test_upstream_cleanup_deletes_every_shortcut_missing_from_git_without_rules(monkeypatch):
    result = _publish_shortcuts(monkeypatch, DEPLOYED, GIT_SHORTCUTS, rules=[])

    assert result.deleted == ["/Tables/dbo/aks_a", "/Tables/dbo/aks_b", "/Tables/legacy/stale"]
    assert result.published == ["onelake"]


def test_prefix_rule_preserves_runtime_shortcuts_but_still_cleans_other_orphans(monkeypatch):
    result = _publish_shortcuts(monkeypatch, DEPLOYED, GIT_SHORTCUTS, rules=[ExternallyManagedShortcuts("protobus_db_eh", ("/Tables/dbo/",))])

    assert result.deleted == ["/Tables/legacy/stale"]
    assert result.published == ["onelake"]


def test_rule_without_prefixes_disables_cleanup_but_still_publishes_git_shortcuts(monkeypatch):
    result = _publish_shortcuts(monkeypatch, DEPLOYED, GIT_SHORTCUTS, rules=[ExternallyManagedShortcuts("protobus_db_eh")])

    assert result.deleted == []
    assert result.published == ["onelake"]


def test_rule_for_another_lakehouse_does_not_apply(monkeypatch):
    result = _publish_shortcuts(monkeypatch, DEPLOYED, GIT_SHORTCUTS, rules=[ExternallyManagedShortcuts("arc_apps_db_eh")])

    assert result.deleted == ["/Tables/dbo/aks_a", "/Tables/dbo/aks_b", "/Tables/legacy/stale"]


def test_git_defined_shortcut_under_a_preserved_prefix_is_still_published(monkeypatch):
    git = [*GIT_SHORTCUTS, {"name": "seed", "path": "/Tables/dbo", "target": GIT_SHORTCUTS[0]["target"]}]

    result = _publish_shortcuts(monkeypatch, [*DEPLOYED, "/Tables/dbo/seed"], git, rules=[ExternallyManagedShortcuts("protobus_db_eh", ("/Tables/dbo/",))])

    assert result.published == ["onelake", "seed"]
    assert result.deleted == ["/Tables/legacy/stale"]


@pytest.mark.parametrize("order", [("rules", "plain"), ("plain", "rules")])
def test_rules_do_not_leak_into_other_workspaces(monkeypatch, order):
    for kind in order:
        rules = [ExternallyManagedShortcuts("protobus_db_eh")] if kind == "rules" else []

        result = _publish_shortcuts(monkeypatch, DEPLOYED, GIT_SHORTCUTS, rules=rules)

        assert result.deleted == ([] if kind == "rules" else ["/Tables/dbo/aks_a", "/Tables/dbo/aks_b", "/Tables/legacy/stale"])


@pytest.fixture
def cicd_context(tmp_path: Path, monkeypatch):
    events = []
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    (artifacts / "parameter.yml").write_text("find_replace: []\n", encoding="utf-8")
    common = MagicMock(spec=CommonParams, local=SimpleNamespace(root_folder=str(tmp_path)), fabric=FabricParams([], []), endpoint=SimpleNamespace(cicd="https://api.fabric.microsoft.com"))
    manager = FabricCicdManager(common, MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
    monkeypatch.setattr(manager, "_post_process_environment_spark_settings", AsyncMock())
    monkeypatch.setattr(manager, "_update_spark_job_definition_configs", AsyncMock())

    class FakeWorkspace:
        def __init__(self, **kwargs):
            events.append(("workspace", kwargs["item_type_in_scope"]))

    monkeypatch.setattr(fabric_cicd, "disable_file_logging", lambda: None)
    monkeypatch.setattr(fabric_cicd, "append_feature_flag", lambda flag: None)
    monkeypatch.setattr(fabric_cicd, "FabricWorkspace", FakeWorkspace)
    monkeypatch.setattr(fabric_cicd, "publish_all_items", lambda workspace: events.append(("publish", dict(workspace.externally_managed_shortcuts or {}))))
    monkeypatch.setattr(fabric_cicd, "unpublish_all_orphan_items", lambda workspace: None)
    rules = [ExternallyManagedShortcuts("protobus_db_eh", ("/Tables/dbo/",))]
    template = SimpleNamespace(artifacts_folder="artifacts", parameter_file_path="artifacts/parameter.yml", environment_key="ci", feature_flags=["enable_shortcut_publish"], item_types_in_scope=["Lakehouse"], unpublish_orphans=False, spark_job_definitions=[], bulk_publish=False, externally_managed_shortcuts=rules)
    return manager, template, artifacts, events, rules


def test_template_attaches_the_rules_to_the_published_workspace_and_installs_the_hook(cicd_context):
    manager, template, artifacts, events, rules = cicd_context
    _write_platform(artifacts, "protobus_db_eh")

    asyncio.run(manager._reconcile_artifacts("workspace-id", SimpleNamespace(), template, artifacts))

    assert events == [("workspace", ["Lakehouse"]), ("publish", {"protobus_db_eh": rules[0]})]
    assert getattr(_lakehouse.list_deployed_shortcuts, "_preserves_externally_managed_shortcuts", False)


def test_template_fails_before_any_workspace_is_created_when_the_lakehouse_is_unknown(cicd_context):
    manager, template, artifacts, events, _rules = cicd_context
    _write_platform(artifacts, "arc_apps_db_eh")

    with pytest.raises(ValueError, match="which is not in"):
        asyncio.run(manager._reconcile_artifacts("workspace-id", SimpleNamespace(), template, artifacts))

    assert events == []


def test_template_without_rules_leaves_upstream_untouched(cicd_context):
    manager, template, artifacts, events, _rules = cicd_context
    template.externally_managed_shortcuts = []

    asyncio.run(manager._reconcile_artifacts("workspace-id", SimpleNamespace(), template, artifacts))

    assert events == [("workspace", ["Lakehouse"]), ("publish", {})]
    assert not hasattr(_lakehouse.list_deployed_shortcuts, "_preserves_externally_managed_shortcuts")
