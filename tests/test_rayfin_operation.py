# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import importlib
import json
import logging

from importlib import resources as importlib_resources
from pathlib import Path
from types import SimpleNamespace

import pytest

from fabric_workspace_deployment import resources as package_resources
from fabric_workspace_deployment.main import dump_env_vars, redact_environment_value
from fabric_workspace_deployment.operations import operators
from fabric_workspace_deployment.operations.operation_interfaces import Operation, OperationParams, RayfinSemanticModelParams

main_module = importlib.import_module("fabric_workspace_deployment.main")


def _write_app(app_root: Path, aliases: list[str] | None = None, data: dict | None = None, with_schema: bool = False) -> None:
    app_root.mkdir(parents=True)
    (app_root / "package.json").write_text(json.dumps({"devDependencies": {"@microsoft/rayfin-cli": "1.35.1"}}), encoding="utf-8")
    (app_root / "package-lock.json").write_text(
        json.dumps(
            {
                "lockfileVersion": 3,
                "packages": {
                    "node_modules/@microsoft/rayfin-cli": {
                        "version": "1.35.1",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    manifest = {
        "schemaVersion": "1.0",
        "kind": "rayfin",
        "app": {
            "id": "sales-insights",
            "name": "Sales Insights",
            "version": "1.0.0",
        },
        "rayfin": {
            "version": "1.35.1",
        },
        "connections": {
            "semanticModels": aliases or [],
        },
        "build": {
            "command": "npm run build",
            "outputPath": "dist",
            "indexDocument": "index.html",
        },
    }
    if data is not None:
        manifest["data"] = data
    (app_root / "fabric-workspace-deployment.json").write_text(json.dumps(manifest), encoding="utf-8")
    if with_schema:
        schema_path = app_root / "rayfin" / "data" / "schema.ts"
        schema_path.parent.mkdir(parents=True)
        schema_path.write_text("export const schema = {};\n", encoding="utf-8")


def _operation_params_for_private_methods(root: Path):
    params = OperationParams.__new__(OperationParams)
    params.logger = logging.getLogger("test-rayfin-operation-params")
    params.common = SimpleNamespace(local=SimpleNamespace(root_folder=str(root)))
    return params


def test_operation_enum_exposes_deploy_rayfin():
    assert Operation("deployRayfin") is Operation.DEPLOY_RAYFIN


def test_parse_absent_rayfin_config_is_empty_list(tmp_path):
    params = _operation_params_for_private_methods(tmp_path)

    assert params._parse_rayfin_params([]) == []


def test_parse_rayfin_binding(tmp_path):
    params = _operation_params_for_private_methods(tmp_path)

    result = params._parse_rayfin_params(
        [
            {
                "rootPath": "apps/sales",
                "workspaceName": "Analytics",
                "semanticModels": {
                    "sales": {
                        "workspaceName": "Semantic Models",
                        "itemName": "Sales Model",
                    },
                },
            }
        ]
    )

    assert result[0].root_path == "apps/sales"
    assert result[0].workspace_name == "Analytics"
    assert result[0].semantic_models == {
        "sales": RayfinSemanticModelParams(
            workspace_name="Semantic Models",
            item_name="Sales Model",
        )
    }


@pytest.mark.parametrize("value", [None, {}, "apps"])
def test_parse_rejects_non_list_rayfin_config(tmp_path, value):
    params = _operation_params_for_private_methods(tmp_path)

    with pytest.raises(ValueError, match="must be a list"):
        params._parse_rayfin_params(value)


def test_validate_rayfin_binding_and_manifest(tmp_path):
    _write_app(tmp_path / "apps" / "sales", aliases=["sales"])
    params = _operation_params_for_private_methods(tmp_path)
    params.rayfin = params._parse_rayfin_params(
        [
            {
                "rootPath": "apps/sales",
                "workspaceName": "Analytics",
                "semanticModels": {
                    "sales": {
                        "workspaceName": "Semantic Models",
                        "itemName": "Sales Model",
                    },
                },
            }
        ]
    )

    assert params._validate_rayfin_params() is True


def test_validate_rejects_binding_alias_mismatch(tmp_path, caplog):
    _write_app(tmp_path / "apps" / "sales", aliases=["sales"])
    params = _operation_params_for_private_methods(tmp_path)
    params.rayfin = params._parse_rayfin_params(
        [
            {
                "rootPath": "apps/sales",
                "workspaceName": "Analytics",
                "semanticModels": {
                    "inventory": {
                        "workspaceName": "Semantic Models",
                        "itemName": "Inventory Model",
                    },
                },
            }
        ]
    )
    caplog.set_level(logging.ERROR)

    assert params._validate_rayfin_params() is False
    assert "missing bindings=['sales']" in caplog.text
    assert "unexpected bindings=['inventory']" in caplog.text


def test_validate_requires_schema_when_managed_data_is_enabled(tmp_path, caplog):
    app_root = tmp_path / "apps" / "sales"
    _write_app(app_root, data={"enabled": True, "dialect": "mssql"})
    params = _operation_params_for_private_methods(tmp_path)
    params.rayfin = params._parse_rayfin_params(
        [
            {
                "rootPath": "apps/sales",
                "workspaceName": "Analytics",
                "semanticModels": {},
            }
        ]
    )
    caplog.set_level(logging.ERROR)

    assert params._validate_rayfin_params() is False
    assert "required schema file" in caplog.text

    schema_path = app_root / "rayfin" / "data" / "schema.ts"
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text("export const schema = {};\n", encoding="utf-8")
    assert params._validate_rayfin_params() is True


def test_parse_rejects_unknown_semantic_model_binding_fields(tmp_path):
    params = _operation_params_for_private_methods(tmp_path)

    with pytest.raises(ValueError, match="unexpected=\\['itemId'\\]"):
        params._parse_rayfin_params(
            [
                {
                    "rootPath": "apps/sales",
                    "workspaceName": "Analytics",
                    "semanticModels": {
                        "sales": {
                            "workspaceName": "Semantic Models",
                            "itemName": "Sales Model",
                            "itemId": "not-allowed",
                        }
                    },
                }
            ]
        )


def test_parse_rejects_duplicate_normalized_semantic_model_aliases(tmp_path):
    params = _operation_params_for_private_methods(tmp_path)

    with pytest.raises(ValueError, match="duplicate normalized alias 'sales'"):
        params._parse_rayfin_params(
            [
                {
                    "rootPath": "apps/sales",
                    "workspaceName": "Analytics",
                    "semanticModels": {
                        "sales": {
                            "workspaceName": "Semantic Models",
                            "itemName": "Sales Model",
                        },
                        " sales ": {
                            "workspaceName": "Semantic Models",
                            "itemName": "Alternate Sales Model",
                        },
                    },
                }
            ]
        )


@pytest.mark.parametrize("root_path", ["/absolute/app", "../outside"])
def test_validate_rejects_root_path_outside_common_root(tmp_path, root_path, caplog):
    params = _operation_params_for_private_methods(tmp_path)
    params.rayfin = params._parse_rayfin_params(
        [
            {
                "rootPath": root_path,
                "workspaceName": "Analytics",
                "semanticModels": {},
            }
        ]
    )
    caplog.set_level(logging.ERROR)

    assert params._validate_rayfin_params() is False
    assert "must be a relative path" in caplog.text


def test_environment_dump_redacts_tokens(monkeypatch, caplog):
    monkeypatch.setattr(
        main_module.os,
        "environ",
        {
            "RAYFIN_TOKEN": "rayfin-secret",
            "FAB_TOKEN": "fabric-secret",
            "NORMAL_VALUE": "visible",
        },
    )
    caplog.set_level(logging.DEBUG)

    dump_env_vars()

    assert "rayfin-secret" not in caplog.text
    assert "fabric-secret" not in caplog.text
    assert "RAYFIN_TOKEN=******" in caplog.text
    assert "NORMAL_VALUE=visible" in caplog.text
    assert redact_environment_value("API_PASSWORD", "secret") == "******"


def test_packaged_compose_resource_uses_pinned_node_image():
    compose_text = importlib_resources.files(package_resources).joinpath("Compose.rayfin.yaml").read_text(encoding="utf-8")

    assert "image: mcr.microsoft.com/azurelinux/base/nodejs:24" in compose_text
    assert "RAYFIN_TOKEN" in compose_text
    assert "RAYFIN_WORKSPACE_ID" in compose_text
    assert "RAYFIN_TENANT_ID" in compose_text


def test_central_operator_dispatches_deploy_rayfin(monkeypatch):
    class DummyManager:
        def __init__(self):
            self.execute_count = 0

        async def execute(self):
            self.execute_count += 1

    class DummyFabricCli:
        def run_command(self, command):
            assert command == "version"
            return "1.0.0"

    rayfin_manager = DummyManager()
    other_manager = DummyManager()

    class FakeFactory:
        def __init__(self, operation_params):
            self.operation_params = operation_params

        def create_fabric_cli(self):
            return DummyFabricCli()

        def create_rayfin_manager(self):
            return rayfin_manager

        def __getattr__(self, name):
            if name.startswith("create_"):
                return lambda: other_manager
            raise AttributeError(name)

    monkeypatch.setattr(operators, "ContainerizedManagementFactory", FakeFactory)
    operation_params = SimpleNamespace(common=SimpleNamespace(), operation=Operation.DEPLOY_RAYFIN, rayfin=[SimpleNamespace()])

    asyncio.run(operators.CentralOperator(operation_params).execute())

    assert rayfin_manager.execute_count == 1
    assert other_manager.execute_count == 0


def test_central_operator_empty_rayfin_config_is_noop_without_fabric_cli(monkeypatch):
    class DummyManager:
        def __init__(self):
            self.execute_count = 0

        async def execute(self):
            self.execute_count += 1

    class FailingFabricCli:
        def run_command(self, command):
            raise AssertionError("Fabric CLI must not run for an empty Rayfin configuration")

    rayfin_manager = DummyManager()
    other_manager = DummyManager()

    class FakeFactory:
        def __init__(self, operation_params):
            self.operation_params = operation_params

        def create_fabric_cli(self):
            return FailingFabricCli()

        def create_rayfin_manager(self):
            return rayfin_manager

        def __getattr__(self, name):
            if name.startswith("create_"):
                return lambda: other_manager
            raise AttributeError(name)

    monkeypatch.setattr(operators, "ContainerizedManagementFactory", FakeFactory)
    operation_params = SimpleNamespace(common=SimpleNamespace(), operation=Operation.DEPLOY_RAYFIN, rayfin=[])

    asyncio.run(operators.CentralOperator(operation_params).execute())

    assert rayfin_manager.execute_count == 1
    assert other_manager.execute_count == 0
