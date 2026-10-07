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
from fabric_workspace_deployment.factories.management_factory import ContainerizedManagementFactory
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
    params.common = SimpleNamespace(
        local=SimpleNamespace(root_folder=str(root)),
        fabric=SimpleNamespace(workspaces=[]),
    )
    return params


def _workspace(name: str = "Analytics", *, rayfins=None, skip_deploy: bool = False):
    return SimpleNamespace(
        name=name,
        rayfins=[] if rayfins is None else rayfins,
        skip_deploy=skip_deploy,
    )


def _set_workspace_rayfins(params, data, *, workspace_index: int = 0, workspace_name: str = "Analytics", skip_deploy: bool = False):
    rayfins = params._parse_rayfin_params(data, workspace_index)
    params.common.fabric.workspaces = [_workspace(workspace_name, rayfins=rayfins, skip_deploy=skip_deploy)]
    return rayfins


def test_operation_enum_exposes_deploy_rayfin():
    assert Operation("deployRayfin") is Operation.DEPLOY_RAYFIN


def test_parse_absent_rayfins_config_is_empty_list(tmp_path):
    params = _operation_params_for_private_methods(tmp_path)

    assert params._parse_rayfin_params([], 3) == []


def test_parse_workspace_defaults_rayfins_to_empty_list(tmp_path, monkeypatch):
    params = _operation_params_for_private_methods(tmp_path)
    monkeypatch.setattr(params, "_parse_fabric_workspace_template_params", lambda data, root_folder: SimpleNamespace())
    monkeypatch.setattr(params, "_parse_fabric_capacity_params", lambda data: SimpleNamespace())
    monkeypatch.setattr(params, "_parse_rbac_params", lambda data: SimpleNamespace())
    monkeypatch.setattr(params, "_parse_model_params", lambda data: [])
    monkeypatch.setattr(params, "_parse_spark_params", lambda data: SimpleNamespace())
    monkeypatch.setattr(params, "_parse_monitoring_params", lambda data: SimpleNamespace())

    result = params._parse_fabric_workspace_params(
        {
            "name": "Analytics",
            "description": "Analytics workspace",
            "iconPath": "icon.png",
            "datasetStorageMode": 1,
            "template": {},
            "capacity": {},
            "rbac": {},
            "model": [],
            "shortcutAuthZRoleName": "Storage Blob Data Reader",
            "skipDeploy": False,
            "spark": {},
            "monitoring": {},
        },
        str(tmp_path),
        2,
    )

    assert result.rayfins == []


def test_parse_rayfin_binding(tmp_path):
    params = _operation_params_for_private_methods(tmp_path)

    result = params._parse_rayfin_params(
        [
            {
                "rootPath": "apps/sales",
                "force": True,
                "semanticModels": {
                    "sales": {
                        "workspaceName": "Semantic Models",
                        "itemName": "Sales Model",
                    },
                    "local": {
                        "itemName": "Local Model",
                    },
                },
            }
        ],
        2,
    )

    assert result[0].root_path == "apps/sales"
    assert result[0].force is True
    assert result[0].semantic_models == {
        "sales": RayfinSemanticModelParams(
            workspace_name="Semantic Models",
            item_name="Sales Model",
        ),
        "local": RayfinSemanticModelParams(
            item_name="Local Model",
        ),
    }


@pytest.mark.parametrize("value", [None, {}, "apps"])
def test_parse_rejects_non_list_rayfins_config(tmp_path, value):
    params = _operation_params_for_private_methods(tmp_path)

    with pytest.raises(ValueError, match=r"common\.fabric\.workspaces\[4\]\.rayfins must be a list"):
        params._parse_rayfin_params(value, 4)


def test_parse_rejects_non_boolean_force(tmp_path):
    params = _operation_params_for_private_methods(tmp_path)

    with pytest.raises(ValueError, match=r"common\.fabric\.workspaces\[0\]\.rayfins\[0\]\.force must be a boolean"):
        params._parse_rayfin_params(
            [
                {
                    "rootPath": "apps/sales",
                    "force": "true",
                    "semanticModels": {},
                }
            ],
            0,
        )


def test_rejects_obsolete_common_fabric_rayfins(tmp_path):
    params = _operation_params_for_private_methods(tmp_path)

    with pytest.raises(ValueError, match=r"common\.fabric\.workspaces\[\]\.rayfins"):
        params._parse_fabric_params({"workspaces": [], "storages": [], "rayfins": []}, str(tmp_path))


def test_rejects_obsolete_top_level_rayfin(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"common": {}, "rayfin": []}), encoding="utf-8")

    with pytest.raises(ValueError, match=r"common\.fabric\.workspaces\[\]\.rayfins"):
        OperationParams(str(config_path), Operation.DEPLOY_RAYFIN.value)


def test_validate_rayfin_binding_and_manifest(tmp_path):
    _write_app(tmp_path / "apps" / "sales", aliases=["sales"])
    params = _operation_params_for_private_methods(tmp_path)
    _set_workspace_rayfins(
        params,
        [
            {
                "rootPath": "apps/sales",
                "semanticModels": {
                    "sales": {
                        "itemName": "Sales Model",
                    },
                },
            }
        ],
    )

    assert params._validate_rayfin_params() is True


def test_validate_rejects_binding_alias_mismatch(tmp_path, caplog):
    _write_app(tmp_path / "apps" / "sales", aliases=["sales"])
    params = _operation_params_for_private_methods(tmp_path)
    _set_workspace_rayfins(
        params,
        [
            {
                "rootPath": "apps/sales",
                "semanticModels": {
                    "inventory": {
                        "workspaceName": "Semantic Models",
                        "itemName": "Inventory Model",
                    },
                },
            }
        ],
    )
    caplog.set_level(logging.ERROR)

    assert params._validate_rayfin_params() is False
    assert "missing bindings=['sales']" in caplog.text
    assert "unexpected bindings=['inventory']" in caplog.text


def test_validate_requires_schema_when_managed_data_is_enabled(tmp_path, caplog):
    app_root = tmp_path / "apps" / "sales"
    _write_app(app_root, data={"enabled": True, "dialect": "mssql"})
    params = _operation_params_for_private_methods(tmp_path)
    _set_workspace_rayfins(
        params,
        [
            {
                "rootPath": "apps/sales",
                "semanticModels": {},
            }
        ],
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
                    "semanticModels": {
                        "sales": {
                            "workspaceName": "Semantic Models",
                            "itemName": "Sales Model",
                            "itemId": "not-allowed",
                        }
                    },
                }
            ],
            0,
        )


@pytest.mark.parametrize("workspace_name", [None, 123])
def test_parse_rejects_non_string_explicit_semantic_model_workspace(tmp_path, workspace_name):
    params = _operation_params_for_private_methods(tmp_path)

    with pytest.raises(ValueError, match=r"common\.fabric\.workspaces\[0\]\.rayfins\[0\]\.semanticModels\['sales'\]\.workspaceName must be a string"):
        params._parse_rayfin_params(
            [
                {
                    "rootPath": "apps/sales",
                    "semanticModels": {
                        "sales": {
                            "workspaceName": workspace_name,
                            "itemName": "Sales Model",
                        }
                    },
                }
            ],
            0,
        )


def test_parse_rejects_duplicate_normalized_semantic_model_aliases(tmp_path):
    params = _operation_params_for_private_methods(tmp_path)

    with pytest.raises(ValueError, match="duplicate normalized alias 'sales'"):
        params._parse_rayfin_params(
            [
                {
                    "rootPath": "apps/sales",
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
            ],
            0,
        )


def test_parse_rejects_removed_app_workspace_name(tmp_path):
    params = _operation_params_for_private_methods(tmp_path)

    with pytest.raises(ValueError, match=r"common\.fabric\.workspaces\[1\]\.rayfins\[0\].*unexpected=\['workspaceName'\]"):
        params._parse_rayfin_params(
            [
                {
                    "rootPath": "apps/sales",
                    "workspaceName": "Analytics",
                    "semanticModels": {},
                }
            ],
            1,
        )


@pytest.mark.parametrize("root_path", ["/absolute/app", "../outside"])
def test_validate_rejects_root_path_outside_common_root(tmp_path, root_path, caplog):
    params = _operation_params_for_private_methods(tmp_path)
    _set_workspace_rayfins(
        params,
        [
            {
                "rootPath": root_path,
                "semanticModels": {},
            }
        ],
    )
    caplog.set_level(logging.ERROR)

    assert params._validate_rayfin_params() is False
    assert "common.fabric.workspaces[0].rayfins[0].rootPath" in caplog.text
    assert "must be a relative path" in caplog.text


def test_validate_rejects_duplicate_root_paths_globally(tmp_path, caplog):
    _write_app(tmp_path / "apps" / "sales")
    params = _operation_params_for_private_methods(tmp_path)
    binding = [{"rootPath": "apps/sales", "semanticModels": {}}]
    params.common.fabric.workspaces = [
        _workspace("Analytics", rayfins=params._parse_rayfin_params(binding, 0)),
        _workspace("Operations", rayfins=params._parse_rayfin_params(binding, 1)),
    ]
    caplog.set_level(logging.ERROR)

    assert params._validate_rayfin_params() is False
    assert "common.fabric.workspaces[1].rayfins[0].rootPath duplicates common.fabric.workspaces[0].rayfins[0].rootPath" in caplog.text


def test_validate_rejects_duplicate_app_ids_within_parent_workspace(tmp_path, caplog):
    _write_app(tmp_path / "apps" / "sales")
    _write_app(tmp_path / "apps" / "inventory")
    params = _operation_params_for_private_methods(tmp_path)
    _set_workspace_rayfins(
        params,
        [
            {"rootPath": "apps/sales", "semanticModels": {}},
            {"rootPath": "apps/inventory", "semanticModels": {}},
        ],
    )
    caplog.set_level(logging.ERROR)

    assert params._validate_rayfin_params() is False
    assert "common.fabric.workspaces[0].rayfins[1] duplicates Rayfin app ID 'sales-insights'" in caplog.text


def test_validate_allows_same_app_id_in_different_parent_workspaces(tmp_path):
    _write_app(tmp_path / "apps" / "sales")
    _write_app(tmp_path / "apps" / "inventory")
    params = _operation_params_for_private_methods(tmp_path)
    params.common.fabric.workspaces = [
        _workspace(
            "Analytics",
            rayfins=params._parse_rayfin_params([{"rootPath": "apps/sales", "semanticModels": {}}], 0),
        ),
        _workspace(
            "Operations",
            rayfins=params._parse_rayfin_params([{"rootPath": "apps/inventory", "semanticModels": {}}], 1),
        ),
    ]

    assert params._validate_rayfin_params() is True


def test_validate_rejects_blank_explicit_semantic_model_workspace(tmp_path, caplog):
    _write_app(tmp_path / "apps" / "sales", aliases=["sales"])
    params = _operation_params_for_private_methods(tmp_path)
    _set_workspace_rayfins(
        params,
        [
            {
                "rootPath": "apps/sales",
                "semanticModels": {
                    "sales": {
                        "workspaceName": " ",
                        "itemName": "Sales Model",
                    }
                },
            }
        ],
    )
    caplog.set_level(logging.ERROR)

    assert params._validate_rayfin_params() is False
    assert "common.fabric.workspaces[0].rayfins[0].semanticModels['sales'].workspaceName" in caplog.text


def test_environment_dump_redacts_tokens(monkeypatch, caplog):
    monkeypatch.setattr(
        main_module.os,
        "environ",
        {
            "RAYFIN_TOKEN": "rayfin-secret",
            "FAB_TOKEN": "fabric-secret",
            "FAB_TOKEN_SQL": "sql-env-secret",
            "NORMAL_VALUE": "visible",
        },
    )
    caplog.set_level(logging.DEBUG)

    dump_env_vars()

    assert "rayfin-secret" not in caplog.text
    assert "fabric-secret" not in caplog.text
    assert "sql-env-secret" not in caplog.text
    assert "RAYFIN_TOKEN=******" in caplog.text
    assert "FAB_TOKEN_SQL=******" in caplog.text
    assert "NORMAL_VALUE=visible" in caplog.text
    assert redact_environment_value("API_PASSWORD", "secret") == "******"


def test_packaged_compose_resource_uses_pinned_node_image():
    compose_text = importlib_resources.files(package_resources).joinpath("Compose.rayfin.yaml").read_text(encoding="utf-8")

    assert "image: mcr.microsoft.com/azurelinux/base/nodejs:24" in compose_text
    assert "RAYFIN_TOKEN" in compose_text
    assert "RAYFIN_WORKSPACE_ID" in compose_text
    assert "RAYFIN_TENANT_ID" in compose_text
    assert "FAB_TOKEN_SQL" not in compose_text
    assert "FWD_RAYFIN_SQL_ACCESS_TOKEN: ${FWD_RAYFIN_SQL_ACCESS_TOKEN-}" in compose_text


def test_factory_passes_workspace_scoped_rayfins_to_manager(tmp_path, monkeypatch):
    workspaces = [_workspace(rayfins=[SimpleNamespace()])]
    common = SimpleNamespace(
        local=SimpleNamespace(root_folder=str(tmp_path)),
        fabric=SimpleNamespace(workspaces=workspaces),
    )
    factory = ContainerizedManagementFactory.__new__(ContainerizedManagementFactory)
    factory.operation_params = SimpleNamespace(common=common)
    factory.logger = logging.getLogger("test-rayfin-factory")
    az_cli = object()
    fabric_cli = object()
    docker_cli = object()
    database_client = object()
    monkeypatch.setattr(factory, "create_azure_cli", lambda: az_cli)
    monkeypatch.setattr(factory, "create_fabric_cli", lambda: fabric_cli)
    monkeypatch.setattr(factory, "create_docker_cli", lambda: docker_cli)
    monkeypatch.setattr(factory, "create_rayfin_database_client", lambda: database_client)

    manager = factory.create_rayfin_manager()

    assert manager.workspace_params is workspaces
    assert manager.az_cli is az_cli
    assert manager.fabric_cli is fabric_cli
    assert manager.docker_cli is docker_cli
    assert manager.database_client is database_client


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
    operation_params = SimpleNamespace(
        common=SimpleNamespace(
            fabric=SimpleNamespace(
                workspaces=[
                    _workspace(rayfins=[SimpleNamespace()]),
                ]
            )
        ),
        operation=Operation.DEPLOY_RAYFIN,
    )

    asyncio.run(operators.CentralOperator(operation_params).execute())

    assert rayfin_manager.execute_count == 1
    assert other_manager.execute_count == 0


def test_central_operator_dry_run_reports_rayfin_plan_and_checks_entitlements(monkeypatch):
    calls = []

    class DummyManager:
        async def execute(self):
            calls.append("entitlements")

        def report_plan(self):
            calls.append("rayfin-plan")

    class DummyFabricCli:
        def run_command(self, command):
            assert command == "version"
            return "1.0.0"

    class FakeFactory:
        def __init__(self, operation_params):
            pass

        def create_fabric_cli(self):
            return DummyFabricCli()

        def __getattr__(self, name):
            if name.startswith("create_"):
                return DummyManager
            raise AttributeError(name)

    monkeypatch.setattr(operators, "ContainerizedManagementFactory", FakeFactory)
    operation_params = SimpleNamespace(common=SimpleNamespace(fabric=SimpleNamespace(workspaces=[])), operation=Operation.DRY_RUN)

    asyncio.run(operators.CentralOperator(operation_params).execute())

    assert calls == ["rayfin-plan", "entitlements"]


@pytest.mark.parametrize(
    "workspaces",
    [
        [_workspace(rayfins=[])],
        [_workspace(rayfins=[SimpleNamespace()], skip_deploy=True)],
    ],
)
def test_central_operator_without_deployable_rayfins_is_noop_without_fabric_cli(monkeypatch, workspaces):
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
    operation_params = SimpleNamespace(
        common=SimpleNamespace(fabric=SimpleNamespace(workspaces=workspaces)),
        operation=Operation.DEPLOY_RAYFIN,
    )

    asyncio.run(operators.CentralOperator(operation_params).execute())

    assert rayfin_manager.execute_count == 1
    assert other_manager.execute_count == 0
