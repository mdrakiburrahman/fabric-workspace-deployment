# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

from pathlib import Path
import logging

import pytest

from fabric_workspace_deployment.operations.operation_interfaces import OperationParams
from fabric_workspace_deployment.storage_namespace import DeploymentNamespace, SeedPathBinding, StorageNamespaceRewriter, normalize_storage_relative_path


def _binding(logical_path: str, prefix: str = "user/dev") -> SeedPathBinding:
    return SeedPathBinding(
        account="arcdatasynapsedogfood",
        container="onelake",
        logical_path=logical_path,
        path_prefix=prefix,
    )


def test_namespace_rewrites_exact_seed_references_only():
    rewriter = StorageNamespaceRewriter(
        [
            _binding("jars/sparkMsit.jar"),
            _binding("configs/sparkMsit-fabric.yaml"),
            _binding("pkgs/spark-dbt-bundle.tar.gz"),
        ]
    )

    content = "\n".join(
        [
            "abfss://onelake@arcdatasynapsedogfood.dfs.core.windows.net/jars/sparkMsit.jar",
            "Files/onelake/configs/sparkMsit-fabric.yaml arg",
            "/lakehouse/default/Files/onelake/pkgs/spark-dbt-bundle.tar.gz",
            "onelake/configs/sparkMsit-fabric.yaml",
            "Files/onelake/synapse/workspaces/prod/warehouse",
            "/lakehouse/default/Files/onelake/raw/dbt/events",
            "/lakehouse/default/Files/onelake/metrics/dbt/project",
        ]
    )

    rewritten = rewriter.rewrite_text(content)

    assert "abfss://onelake@arcdatasynapsedogfood.dfs.core.windows.net/user/dev/jars/sparkMsit.jar" in rewritten
    assert "Files/onelake/user/dev/configs/sparkMsit-fabric.yaml arg" in rewritten
    assert "/lakehouse/default/Files/onelake/user/dev/pkgs/spark-dbt-bundle.tar.gz" in rewritten
    assert "onelake/user/dev/configs/sparkMsit-fabric.yaml" in rewritten
    assert "Files/onelake/synapse/workspaces/prod/warehouse" in rewritten
    assert "/lakehouse/default/Files/onelake/raw/dbt/events" in rewritten
    assert "/lakehouse/default/Files/onelake/metrics/dbt/project" in rewritten
    assert rewriter.rewrite_text(rewritten) == rewritten


def test_namespace_effective_path_and_tree_copy_preserve_source(tmp_path: Path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    (source / "definition.json").write_text('{"path":"Files/onelake/configs/app.json"}', encoding="utf-8")
    binary = b"\x00\xffbinary"
    (source / "library.jar").write_bytes(binary)

    rewriter = StorageNamespaceRewriter([_binding("configs/app.json")])
    rewritten_count = rewriter.copy_tree(source, destination)

    assert rewritten_count == 1
    assert (source / "definition.json").read_text(encoding="utf-8") == '{"path":"Files/onelake/configs/app.json"}'
    assert (destination / "definition.json").read_text(encoding="utf-8") == '{"path":"Files/onelake/user/dev/configs/app.json"}'
    assert (destination / "library.jar").read_bytes() == binary
    assert rewriter.effective_path("arcdatasynapsedogfood", "onelake", "configs/app.json") == "user/dev/configs/app.json"
    assert rewriter.effective_path("other", "onelake", "configs/app.json") == "configs/app.json"


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        "/user/dev",
        "user\\dev",
        "user/../dev",
        "user/./dev",
        "user//dev",
    ],
)
def test_invalid_namespace_paths_are_rejected(value: str):
    with pytest.raises(ValueError):
        DeploymentNamespace(value)


def test_trailing_slash_is_normalized():
    assert normalize_storage_relative_path(" user/dev/ ", "path") == "user/dev"


def test_duplicate_and_ambiguous_bindings_are_rejected():
    with pytest.raises(ValueError, match="Duplicate seeded storage path"):
        StorageNamespaceRewriter([_binding("configs/app.json"), _binding("configs/app.json")])

    with pytest.raises(ValueError, match="Ambiguous seeded storage reference"):
        StorageNamespaceRewriter(
            [
                _binding("configs/app.json", "user/dev"),
                SeedPathBinding(
                    account="otheraccount",
                    container="onelake",
                    logical_path="configs/app.json",
                    path_prefix="user/other",
                ),
            ]
        )


def test_storage_parser_accepts_optional_namespace_and_preserves_legacy_shape():
    params = OperationParams.__new__(OperationParams)
    raw = {
        "account": "account",
        "container": "container",
        "location": "eastus",
        "resourceGroup": "resource-group",
        "subscriptionId": "subscription",
        "tenantId": "tenant",
        "shortcutDataConnectionId": "connection",
        "rbac": {"auth": {"mode": "cli"}},
        "seedFiles": [],
    }

    legacy = params._parse_fabric_storage_params(raw)
    namespaced = params._parse_fabric_storage_params({**raw, "deploymentNamespace": {"pathPrefix": "user/dev/"}})

    assert legacy.deployment_namespace is None
    assert namespaced.deployment_namespace == DeploymentNamespace("user/dev")


def test_unique_environment_placeholder_resolves_inside_namespace(monkeypatch):
    monkeypatch.setenv("UNIQUE_ENV_ID", "Dev.User-01")
    params = OperationParams.__new__(OperationParams)
    params.logger = logging.getLogger("namespace-placeholder-test")

    resolved = params._replace_placeholders_in_value({"deploymentNamespace": {"pathPrefix": "user/{unique-env-id}"}})

    assert resolved == {"deploymentNamespace": {"pathPrefix": "user/devuser01"}}


@pytest.mark.parametrize(
    "namespace",
    [
        None,
        "user/dev",
        {},
        {"pathPrefix": "user/dev", "unexpected": True},
    ],
)
def test_storage_parser_rejects_invalid_namespace_shape(namespace):
    params = OperationParams.__new__(OperationParams)
    raw = {
        "account": "account",
        "container": "container",
        "location": "eastus",
        "resourceGroup": "resource-group",
        "subscriptionId": "subscription",
        "tenantId": "tenant",
        "shortcutDataConnectionId": "connection",
        "rbac": {"auth": {"mode": "cli"}},
        "seedFiles": [],
        "deploymentNamespace": namespace,
    }

    with pytest.raises(ValueError, match="deploymentNamespace"):
        params._parse_fabric_storage_params(raw)
