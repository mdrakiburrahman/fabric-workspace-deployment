# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import json
import traceback

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from fabric_workspace_deployment.client.rayfin_database import FabricRayfinDatabaseClient
from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.docker.cli import DockerCliError
from fabric_workspace_deployment.manager.rayfin.deployment import RayfinDeploymentManager
from fabric_workspace_deployment.environment_variables import FWD_RAYFIN_APP_BACKEND_ID_ENV_VAR, FWD_RAYFIN_SQL_ACCESS_TOKEN_ENV_VAR, FWD_RAYFIN_SQL_DATABASE_ID_ENV_VAR, FWD_RAYFIN_SQL_DATABASE_NAME_ENV_VAR, FWD_RAYFIN_SQL_SERVER_ENV_VAR, FWD_RAYFIN_WORKSPACE_ID_ENV_VAR
from fabric_workspace_deployment.operations.operation_interfaces import RayfinParams, RayfinSemanticModelParams
from fabric_workspace_deployment.rayfin.manifest import RayfinDeploymentRecord, RayfinManagedSqlDatabase, RayfinManifestLoader

WORKSPACE_ID = "11111111-1111-1111-1111-111111111111"
MODEL_WORKSPACE_ID = "55555555-5555-5555-5555-555555555555"
MODEL_ID = "22222222-2222-2222-2222-222222222222"
ITEM_ID = "33333333-3333-3333-3333-333333333333"
TENANT_ID = "44444444-4444-4444-4444-444444444444"
DATABASE_ID = "66666666-6666-6666-6666-666666666666"
SQL_SERVER = "managed.database.fabric.microsoft.com"
DATABASE_NAME = "managed-app-db"


def _write_app(root: Path) -> Path:
    app_root = root / "apps" / "sales"
    (app_root / "rayfin" / "data").mkdir(parents=True)
    (app_root / "rayfin" / "data" / "schema.ts").write_text("export const schema = {};\n", encoding="utf-8")
    (app_root / "package.json").write_text(
        json.dumps(
            {
                "name": "sales-insights",
                "private": True,
                "devDependencies": {
                    "@microsoft/rayfin-cli": "1.35.1",
                },
            }
        ),
        encoding="utf-8",
    )
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
    (app_root / "fabric-workspace-deployment.json").write_text(
        json.dumps(
            {
                "schemaVersion": "1.0",
                "kind": "rayfin",
                "app": {
                    "id": "sales-insights",
                    "name": "Sales Insights",
                    "version": "2.4.0",
                },
                "rayfin": {
                    "version": "1.35.1",
                },
                "connections": {
                    "semanticModels": ["sales"],
                },
                "build": {
                    "command": "npm run build",
                    "outputPath": "dist",
                    "indexDocument": "index.html",
                },
                "data": {
                    "enabled": True,
                    "dialect": "mssql",
                },
            }
        ),
        encoding="utf-8",
    )
    return app_root


def _enable_functions(app_root: Path) -> None:
    functions_root = app_root / "rayfin" / "functions"
    (functions_root / "src").mkdir(parents=True)
    sdk = "@microsoft/fabric-user-data-functions"
    (functions_root / "package.json").write_text(json.dumps({"name": "sales-functions", "scripts": {"build": "tsc --build"}, "dependencies": {sdk: "1.35.1"}}), encoding="utf-8")
    (functions_root / "host.json").write_text('{"version":"2.0"}', encoding="utf-8")
    (functions_root / "tsconfig.json").write_text('{"include":["src"]}', encoding="utf-8")
    (functions_root / "src" / "function_app.ts").write_text("export {};\n", encoding="utf-8")
    manifest_path = app_root / "fabric-workspace-deployment.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["functions"] = {"enabled": True, "path": "rayfin/functions", "buildCommand": "npm run build"}
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    package_path = app_root / "package.json"
    package = json.loads(package_path.read_text())
    package["workspaces"] = ["rayfin/functions"]
    package_path.write_text(json.dumps(package), encoding="utf-8")
    lock_path = app_root / "package-lock.json"
    lock = json.loads(lock_path.read_text())
    lock["packages"].update(
        {
            "rayfin/functions": {"dependencies": {sdk: "1.35.1"}},
            "node_modules/sales-functions": {"link": True, "resolved": "rayfin/functions"},
            f"node_modules/{sdk}": {"version": "1.35.1", "dependencies": {"@microsoft/rayfin-client": "1.35.1"}},
        }
    )
    lock_path.write_text(json.dumps(lock), encoding="utf-8")


def _enable_migrations(app_root: Path) -> None:
    manifest_path = app_root / "fabric-workspace-deployment.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["data"]["migrations"] = {"command": "npm run data:migrate"}
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    package_path = app_root / "package.json"
    package = json.loads(package_path.read_text())
    package["scripts"] = {"data:migrate": "node migrations/run.mjs"}
    package_path.write_text(json.dumps(package), encoding="utf-8")


class FakeDatabaseClient:
    def __init__(self, database=None, error=None):
        self.calls = []
        self.database = database or RayfinManagedSqlDatabase(WORKSPACE_ID, ITEM_ID, DATABASE_ID, SQL_SERVER, DATABASE_NAME)
        self.error = error

    def resolve_managed_database(self, workspace_id, app_backend_id):
        self.calls.append((workspace_id, app_backend_id))
        if self.error is not None:
            raise RuntimeError(self.error)
        return self.database


def _common(root: Path):
    return SimpleNamespace(
        local=SimpleNamespace(root_folder=str(root)),
        scope=SimpleNamespace(analysis_service="https://analysis.windows.net/powerbi/api"),
        arm=SimpleNamespace(tenant_id=TENANT_ID),
    )


class FakeAzCli:
    def __init__(self):
        self.calls = []

    def get_access_token(self, scope):
        self.calls.append(scope)
        return "az-token"

    def get_sql_access_token(self, tenant_id=None):
        assert tenant_id == TENANT_ID
        self.calls.append("https://database.windows.net")
        return "sql-short-lived-token"


class FakeFabricCli:
    def __init__(self, *, fail_model: bool = False, app_item_id: str = ITEM_ID, app_workspace_id: str = WORKSPACE_ID, app_display_name: str = "sales-insights", wrap_api_response: bool = True, app_type: str = "AppBackend"):
        self.calls = []
        self.fail_model = fail_model
        self.app_item_id = app_item_id
        self.app_workspace_id = app_workspace_id
        self.app_display_name = app_display_name
        self.wrap_api_response = wrap_api_response
        self.app_type = app_type

    def run(self, command, timeout=None):
        self.calls.append((list(command), timeout))
        if command[0] == "api":
            assert command == ["api", f"workspaces/{WORKSPACE_ID}/items/{ITEM_ID}", "-X", "get"]
            item_data = {
                "id": self.app_item_id,
                "workspaceId": self.app_workspace_id,
                "displayName": self.app_display_name,
                "type": self.app_type,
            }
            response_data = (
                {
                    "status_code": 200,
                    "text": item_data,
                }
                if self.wrap_api_response
                else item_data
            )
            return (
                json.dumps(response_data),
                "",
            )

        resource_path = command[1]
        if resource_path == "Analytics.Workspace":
            return WORKSPACE_ID, ""
        if resource_path == "Semantic Models.Workspace":
            return MODEL_WORKSPACE_ID, ""
        if resource_path == "Semantic Models.Workspace/Sales Model.SemanticModel":
            if self.fail_model:
                raise RuntimeError("not found")
            return f'"{MODEL_ID}"', ""
        if resource_path == "Analytics.Workspace/Sales Model.SemanticModel":
            if self.fail_model:
                raise RuntimeError("not found")
            return f'"{MODEL_ID}"', ""
        raise AssertionError(f"Unexpected Fabric CLI path: {resource_path}")


class FakeDockerCli:
    def __init__(self, *, fail_command: list[str] | None = None, version: str = "1.35.1", status: object | None = None, registry_data_enabled: bool | None = True, migration_stdout: str = "", migration_stderr: str = ""):
        self.calls = []
        self.fail_command = fail_command
        self.version = version
        self.status = status if status is not None else {"status": "Running", "healthy": True, "services": {"data": {"enabled": True}}}
        self.registry_data_enabled = registry_data_enabled
        self.generated_fabric_yaml = None
        self.generated_rayfin_yaml = None
        self.compose_text = None
        self.seeded_registry_seen = False
        self.migration_stdout = migration_stdout
        self.migration_stderr = migration_stderr

    def resolve_daemon_path(self, path):
        return Path(path)

    def compose_run(self, compose_file, project_name, service, command, *, timeout=None, env=None, container_name=None):
        command = list(command)
        env = dict(env or {})
        self.calls.append(
            {
                "compose_file": Path(compose_file),
                "project_name": project_name,
                "service": service,
                "command": command,
                "timeout": timeout,
                "env": env,
                "container_name": container_name,
            }
        )
        staging_root = Path(env["RAYFIN_APP_ROOT"])

        if command == ["npm", "ci"]:
            self.generated_fabric_yaml = yaml.safe_load((staging_root / "fabric.yaml").read_text(encoding="utf-8"))
            self.generated_rayfin_yaml = yaml.safe_load((staging_root / "rayfin" / "rayfin.yml").read_text(encoding="utf-8"))
            self.compose_text = Path(compose_file).read_text(encoding="utf-8")
            self.seeded_registry_seen = (staging_root / "rayfin" / ".deployments.json").exists()

        if self.fail_command == command:
            raise RuntimeError("simulated Docker failure")

        if command == ["./node_modules/.bin/rayfin", "--version"]:
            return f"@microsoft/rayfin-cli {self.version}\n", ""
        if command in (["./node_modules/.bin/rayfin", "up", "--yes"], ["./node_modules/.bin/rayfin", "up", "--yes", "--force"]):
            registry_path = staging_root / "rayfin" / ".deployments.json"
            registry_path.parent.mkdir(parents=True, exist_ok=True)
            deployment_record = {
                "workspaceId": WORKSPACE_ID,
                "fabricItemId": ITEM_ID,
                "hostingUrl": "https://example.invalid",
                "publishableKey": "pk-example",
            }
            if self.registry_data_enabled is not None:
                deployment_record["services"] = {
                    "data": {
                        "enabled": self.registry_data_enabled,
                    }
                }
            registry_path.write_text(
                json.dumps({"deployments": [deployment_record]}),
                encoding="utf-8",
            )
            return "deployed", ""
        if command == ["./node_modules/.bin/rayfin", "up", "status", "--json"]:
            return json.dumps(self.status), ""
        if command[:2] == ["sh", "-c"]:
            return self.migration_stdout, self.migration_stderr
        return "", ""


def _manager(root: Path, staging_root: Path | None, state_root: Path | None, az_cli=None, fabric_cli=None, docker_cli=None, rayfin_params=None, workspace_params=None, database_client=None):
    configured_rayfins = (
        rayfin_params
        if rayfin_params is not None
        else [
            RayfinParams(
                root_path="apps/sales",
                semantic_models={
                    "sales": RayfinSemanticModelParams(
                        workspace_name="Semantic Models",
                        item_name="Sales Model",
                    )
                },
            )
        ]
    )
    configured_workspaces = (
        workspace_params
        if workspace_params is not None
        else [
            SimpleNamespace(
                name="Analytics",
                skip_deploy=False,
                rayfins=configured_rayfins,
            )
        ]
    )
    return RayfinDeploymentManager(
        _common(root),
        configured_workspaces,
        az_cli or FakeAzCli(),
        fabric_cli or FakeFabricCli(),
        docker_cli or FakeDockerCli(),
        staging_root=staging_root,
        state_root=state_root,
        database_client=database_client,
    )


def test_successful_deployment_generates_configs_verifies_and_cleans_staging(tmp_path, monkeypatch):
    app_root = _write_app(tmp_path)
    staging_root = tmp_path / ".fabric-workspace-deployment" / "rayfin-staging"
    stale_staging = staging_root / "stale-run"
    stale_staging.mkdir(parents=True)
    (stale_staging / "diagnostic.txt").write_text("old failure", encoding="utf-8")
    state_root = tmp_path / "state"
    az_cli = FakeAzCli()
    fabric_cli = FakeFabricCli()
    docker_cli = FakeDockerCli()
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    manager = _manager(tmp_path, None, state_root, az_cli=az_cli, fabric_cli=fabric_cli, docker_cli=docker_cli)

    asyncio.run(manager.execute())

    assert az_cli.calls == []
    assert [call["command"] for call in docker_cli.calls] == [
        ["npm", "ci"],
        ["./node_modules/.bin/rayfin", "--version"],
        ["./node_modules/.bin/rayfin", "up", "--yes"],
        ["./node_modules/.bin/rayfin", "up", "status", "--json"],
    ]
    assert all(call["env"]["RAYFIN_TOKEN"] == "configured-token" for call in docker_cli.calls)
    assert all(call["env"]["RAYFIN_WORKSPACE_ID"] == WORKSPACE_ID for call in docker_cli.calls)
    assert all(call["env"]["RAYFIN_TENANT_ID"] == TENANT_ID for call in docker_cli.calls)
    assert all(Path(call["env"]["RAYFIN_APP_ROOT"]).parent == staging_root for call in docker_cli.calls)
    assert all(call["compose_file"].parent == Path(call["env"]["RAYFIN_APP_ROOT"]) for call in docker_cli.calls)
    assert not stale_staging.exists()
    assert docker_cli.generated_fabric_yaml["profiles"]["deployment"]["semanticModels"]["sales"] == {
        "workspaceId": MODEL_WORKSPACE_ID,
        "itemId": MODEL_ID,
    }
    assert docker_cli.generated_rayfin_yaml["services"]["auth"] == {
        "enabled": True,
        "password": {
            "enabled": False,
        },
        "fabric": {
            "enabled": True,
        },
    }
    assert docker_cli.generated_rayfin_yaml["services"]["data"] == {
        "enabled": True,
        "dialect": "mssql",
    }
    assert "allowedRedirectUris" not in docker_cli.generated_rayfin_yaml["services"]["auth"]
    assert docker_cli.generated_rayfin_yaml["services"]["staticHosting"]["buildCommand"] == "npm run build"
    assert "image: mcr.microsoft.com/azurelinux/base/nodejs:24" in docker_cli.compose_text
    assert docker_cli.seeded_registry_seen is False
    assert not (app_root / "fabric.yaml").exists()
    assert not (app_root / "rayfin" / "rayfin.yml").exists()
    assert staging_root.exists()
    assert list(staging_root.iterdir()) == []
    assert not state_root.exists()
    assert fabric_cli.calls[0][0][1] == "Analytics.Workspace"
    assert fabric_cli.calls[1][0][1] == "Semantic Models.Workspace"
    assert fabric_cli.calls[2][0][1] == "Semantic Models.Workspace/Sales Model.SemanticModel"
    assert fabric_cli.calls[-1][0] == ["api", f"workspaces/{WORKSPACE_ID}/items/{ITEM_ID}", "-X", "get"]


def test_omitted_semantic_model_workspace_inherits_parent_workspace(tmp_path, monkeypatch):
    _write_app(tmp_path)
    fabric_cli = FakeFabricCli()
    docker_cli = FakeDockerCli()
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    manager = _manager(
        tmp_path,
        tmp_path / "staging",
        tmp_path / "state",
        fabric_cli=fabric_cli,
        docker_cli=docker_cli,
        rayfin_params=[
            RayfinParams(
                root_path="apps/sales",
                semantic_models={
                    "sales": RayfinSemanticModelParams(
                        item_name="Sales Model",
                    )
                },
            )
        ],
    )

    asyncio.run(manager.execute())

    assert docker_cli.generated_fabric_yaml["profiles"]["deployment"]["semanticModels"]["sales"] == {
        "workspaceId": WORKSPACE_ID,
        "itemId": MODEL_ID,
    }
    resolved_paths = [call[0][1] for call in fabric_cli.calls if call[0][0] == "get"]
    assert resolved_paths == [
        "Analytics.Workspace",
        "Analytics.Workspace/Sales Model.SemanticModel",
    ]


def test_functions_clean_checkout_uses_root_install_and_canonical_deployment(tmp_path, monkeypatch):
    app_root = _write_app(tmp_path)
    _enable_functions(app_root)
    docker_cli = FakeDockerCli()
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")

    asyncio.run(_manager(tmp_path, tmp_path / "staging", None, docker_cli=docker_cli).execute())

    assert not (app_root / "node_modules").exists()
    assert [call["command"] for call in docker_cli.calls] == [
        ["npm", "ci"],
        ["npm", "exec", "--workspace", "rayfin/functions", "--no", "--", "tsc", "--showConfig", "--project", "tsconfig.json"],
        ["./node_modules/.bin/rayfin", "--version"],
        ["./node_modules/.bin/rayfin", "up", "--yes"],
        ["./node_modules/.bin/rayfin", "up", "status", "--json"],
    ]
    assert docker_cli.generated_rayfin_yaml["services"]["functions"] == {
        "enabled": True,
        "auth": {"type": "application"},
        "path": "rayfin/functions",
        "buildCommand": "npm run build",
    }
    assert list((tmp_path / "staging").iterdir()) == []


def test_functions_typescript_configuration_failure_prevents_deployment(tmp_path, monkeypatch):
    app_root = _write_app(tmp_path)
    _enable_functions(app_root)
    config_command = ["npm", "exec", "--workspace", "rayfin/functions", "--no", "--", "tsc", "--showConfig", "--project", "tsconfig.json"]
    docker_cli = FakeDockerCli(fail_command=config_command)
    fabric_cli = FakeFabricCli()
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")

    with pytest.raises(RuntimeError, match="simulated Docker failure"):
        asyncio.run(_manager(tmp_path, tmp_path / "staging", None, docker_cli=docker_cli, fabric_cli=fabric_cli).execute())

    assert [call["command"] for call in docker_cli.calls] == [["npm", "ci"], config_command]
    assert not any(call[0][0] == "api" for call in fabric_cli.calls)
    assert len(list((tmp_path / "staging").iterdir())) == 1


@pytest.mark.parametrize("functions_enabled", [False, True])
def test_rayfin_plan_reports_functions_without_external_side_effects(tmp_path, caplog, functions_enabled):
    app_root = _write_app(tmp_path)
    if functions_enabled:
        _enable_functions(app_root)
    az_cli = FakeAzCli()
    fabric_cli = FakeFabricCli()
    docker_cli = FakeDockerCli()
    manager = _manager(tmp_path, tmp_path / "staging", None, az_cli=az_cli, fabric_cli=fabric_cli, docker_cli=docker_cli)
    caplog.set_level("INFO")

    manager.report_plan()

    assert f"Functions enabled={functions_enabled}" in caplog.text
    assert "rayfin up --yes" in caplog.text
    if functions_enabled:
        assert "auth.type=application" in caplog.text
    assert az_cli.calls == fabric_cli.calls == docker_cli.calls == []
    assert not (tmp_path / "staging").exists()
    assert not (app_root / "rayfin" / "rayfin.yml").exists()


def test_rayfin_plan_skips_workspace_before_reading_app(tmp_path, caplog):
    manager = _manager(tmp_path, tmp_path / "staging", None, workspace_params=[SimpleNamespace(name="Skipped", skip_deploy=True, rayfins=[RayfinParams(root_path="missing", semantic_models={})])])
    caplog.set_level("INFO")

    manager.report_plan()

    assert "skipDeploy=true" in caplog.text
    assert not (tmp_path / "staging").exists()


def test_migration_uses_configured_sql_identity_instead_of_azure_cli_identity(tmp_path, monkeypatch):
    app_root = _write_app(tmp_path)
    _enable_functions(app_root)
    _enable_migrations(app_root)
    monkeypatch.setenv("RAYFIN_TOKEN", "fabric-token-principal-a")
    monkeypatch.setenv("FAB_TOKEN_SQL", " \n sql-token-principal-a \t")
    azure_commands = []

    def popen(command, **kwargs):
        azure_commands.append(list(command))
        return SimpleNamespace(returncode=0, communicate=lambda **kwargs: (b"sql-token-principal-b", b""))

    monkeypatch.setattr("fabric_workspace_deployment.manager.azure.cli.Popen", popen)
    docker_cli = FakeDockerCli()
    database_client = FakeDatabaseClient()
    manager = _manager(tmp_path, tmp_path / "staging", None, az_cli=AzCli(), docker_cli=docker_cli, database_client=database_client)

    asyncio.run(manager.execute())

    migration_call = next(call for call in docker_cli.calls if call["command"][:2] == ["sh", "-c"])
    assert migration_call["env"][FWD_RAYFIN_SQL_ACCESS_TOKEN_ENV_VAR] == "sql-token-principal-a"
    assert azure_commands == []
    assert database_client.calls == [(WORKSPACE_ID, ITEM_ID)]
    assert all(call["env"]["RAYFIN_TOKEN"] == "fabric-token-principal-a" for call in docker_cli.calls)
    assert all(call["env"][FWD_RAYFIN_SQL_ACCESS_TOKEN_ENV_VAR] == "" for call in docker_cli.calls if call is not migration_call)
    container_environment = yaml.safe_load(docker_cli.compose_text)["services"]["rayfin"]["environment"]
    assert "FAB_TOKEN_SQL" not in container_environment
    assert container_environment[FWD_RAYFIN_SQL_ACCESS_TOKEN_ENV_VAR] == "${FWD_RAYFIN_SQL_ACCESS_TOKEN-}"
    assert any(call["command"][:3] == ["npm", "exec", "--workspace"] for call in docker_cli.calls)


@pytest.mark.parametrize("sql_item_type", ["SQLDbNative", "SQLDatabase"])
def test_live_relation_graph_reaches_environment_sql_token_migration(tmp_path, monkeypatch, sql_item_type):
    _enable_migrations(_write_app(tmp_path))
    monkeypatch.setenv("FAB_TOKEN", "fabric-token-principal-a")
    monkeypatch.setenv("RAYFIN_TOKEN", "fabric-token-principal-a")
    monkeypatch.setenv("FAB_TOKEN_SQL", " \n sql-token-principal-a \t")
    azure_commands = []

    def popen(command, **kwargs):
        azure_commands.append(list(command))
        return SimpleNamespace(returncode=0, communicate=lambda **kwargs: (b"sql-token-principal-b", b""))

    monkeypatch.setattr("fabric_workspace_deployment.manager.azure.cli.Popen", popen)
    common = _common(tmp_path)
    common.endpoint = SimpleNamespace(cicd="https://fabric.example.invalid")
    relation_path = f"{common.endpoint.cicd}/v1/workspaces/{WORKSPACE_ID}/items/{ITEM_ID}/relations"
    database_path = f"{common.endpoint.cicd}/v1/workspaces/{WORKSPACE_ID}/sqlDatabases/{DATABASE_ID}"
    function_id = "77777777-7777-7777-7777-777777777777"
    endpoint_id = "88888888-8888-8888-8888-888888888888"
    items = [
        {"id": ITEM_ID, "workspaceId": WORKSPACE_ID, "type": "AppBackend"},
        {"id": DATABASE_ID, "workspaceId": WORKSPACE_ID, "type": sql_item_type},
        {"id": function_id, "workspaceId": WORKSPACE_ID, "type": "FunctionSet"},
        {"id": endpoint_id, "workspaceId": WORKSPACE_ID, "type": "SqlAnalyticsEndpoint"},
    ]
    relations = [
        {"itemId": DATABASE_ID, "dependentOnItemId": ITEM_ID, "relationType": "CascadeDelete"},
        {"itemId": function_id, "dependentOnItemId": ITEM_ID, "relationType": "CascadeDelete"},
        {"itemId": endpoint_id, "dependentOnItemId": DATABASE_ID, "relationType": "CascadeDelete"},
    ]
    public_types = {DATABASE_ID: "SQLDatabase", function_id: "UserDataFunction", endpoint_id: "SQLEndpoint"}
    responses = {
        f"{relation_path}/upstream?beta=true": {"items": items, "relations": relations},
        f"{relation_path}/downstream?beta=true": {"items": [{**item, "type": public_types.get(item["id"], item["type"])} for item in items], "relations": relations},
        database_path: {"id": DATABASE_ID, "workspaceId": WORKSPACE_ID, "type": "SQLDatabase", "properties": {"serverFqdn": f"tcp:{SQL_SERVER},1433", "databaseName": DATABASE_NAME, "connectionString": f"Server={SQL_SERVER};Database={DATABASE_NAME}"}},
    }
    http_calls = []

    def execute(method, url, **kwargs):
        http_calls.append(url)
        assert kwargs["safe_log_context"]
        assert kwargs["timeout"] == 60
        return SimpleNamespace(json=lambda: responses[url])

    azure = AzCli()
    original_select_sql_token = azure.get_sql_access_token
    selected_tenants = []

    def select_sql_token(tenant_id=None):
        assert http_calls == list(responses)
        selected_tenants.append(tenant_id)
        return original_select_sql_token(tenant_id)

    monkeypatch.setattr(azure, "get_sql_access_token", select_sql_token)
    database_client = FabricRayfinDatabaseClient(common, azure, SimpleNamespace(execute=execute))
    docker_cli = FakeDockerCli()
    manager = _manager(tmp_path, tmp_path / "staging", None, az_cli=azure, docker_cli=docker_cli, database_client=database_client)

    asyncio.run(manager.execute())

    migration_call = next(call for call in docker_cli.calls if call["command"][:2] == ["sh", "-c"])
    assert selected_tenants == [TENANT_ID]
    assert migration_call["env"][FWD_RAYFIN_SQL_ACCESS_TOKEN_ENV_VAR] == "sql-token-principal-a"
    assert migration_call["env"][FWD_RAYFIN_SQL_DATABASE_ID_ENV_VAR] == DATABASE_ID
    assert migration_call["env"][FWD_RAYFIN_SQL_SERVER_ENV_VAR] == SQL_SERVER
    assert migration_call["env"][FWD_RAYFIN_SQL_DATABASE_NAME_ENV_VAR] == DATABASE_NAME
    assert azure_commands == []
    assert all(call["env"][FWD_RAYFIN_SQL_ACCESS_TOKEN_ENV_VAR] == "" for call in docker_cli.calls if call is not migration_call)
    assert docker_cli.calls[-1]["command"] == ["./node_modules/.bin/rayfin", "up", "status", "--json"]
    assert list((tmp_path / "staging").iterdir()) == []


def test_migration_rerun_reads_sql_environment_immediately_before_each_invocation(tmp_path, monkeypatch):
    _enable_migrations(_write_app(tmp_path))
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    monkeypatch.setenv("FAB_TOKEN_SQL", "superseded-sql-token")
    monkeypatch.setattr("fabric_workspace_deployment.manager.azure.cli.Popen", lambda *args, **kwargs: pytest.fail("A supplied SQL credential must not fall back to Azure CLI"))
    database_client = FakeDatabaseClient()
    original_resolve = database_client.resolve_managed_database
    tokens = iter(["sql-env-first", "sql-env-second"])

    def resolve(workspace_id, app_backend_id):
        database = original_resolve(workspace_id, app_backend_id)
        monkeypatch.setenv("FAB_TOKEN_SQL", f" \n {next(tokens)} \t")
        return database

    monkeypatch.setattr(database_client, "resolve_managed_database", resolve)
    docker_cli = FakeDockerCli()
    manager = _manager(tmp_path, tmp_path / "staging", None, az_cli=AzCli(), docker_cli=docker_cli, database_client=database_client)

    asyncio.run(manager.execute())
    asyncio.run(manager.execute())

    migrations = [call for call in docker_cli.calls if call["command"][:2] == ["sh", "-c"]]
    assert [call["env"][FWD_RAYFIN_SQL_ACCESS_TOKEN_ENV_VAR] for call in migrations] == ["sql-env-first", "sql-env-second"]
    assert database_client.calls == [(WORKSPACE_ID, ITEM_ID), (WORKSPACE_ID, ITEM_ID)]
    assert list((tmp_path / "staging").iterdir()) == []


def test_migrations_run_after_guarded_deployment_before_status_with_ephemeral_environment(tmp_path, monkeypatch):
    app_root = _write_app(tmp_path)
    _enable_migrations(app_root)
    az_cli = FakeAzCli()
    fabric_cli = FakeFabricCli()
    docker_cli = FakeDockerCli(migration_stdout='{"status":"Succeeded","applied":["0001"]}')
    database_client = FakeDatabaseClient()
    original_resolve = database_client.resolve_managed_database

    def resolve(workspace_id, app_backend_id):
        assert fabric_cli.calls[-1][0] == ["api", f"workspaces/{WORKSPACE_ID}/items/{ITEM_ID}", "-X", "get"]
        assert docker_cli.calls[-1]["command"] == ["./node_modules/.bin/rayfin", "up", "--yes"]
        assert az_cli.calls == []
        return original_resolve(workspace_id, app_backend_id)

    database_client.resolve_managed_database = resolve
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    manager = _manager(tmp_path, tmp_path / "staging", None, az_cli=az_cli, fabric_cli=fabric_cli, docker_cli=docker_cli, database_client=database_client)

    asyncio.run(manager.execute())

    assert database_client.calls == [(WORKSPACE_ID, ITEM_ID)]
    assert az_cli.calls == ["https://database.windows.net"]
    assert [call["command"] for call in docker_cli.calls][-2:] == [["sh", "-c", "npm run data:migrate"], ["./node_modules/.bin/rayfin", "up", "status", "--json"]]
    migration_env = docker_cli.calls[-2]["env"]
    assert migration_env[FWD_RAYFIN_WORKSPACE_ID_ENV_VAR] == WORKSPACE_ID
    assert migration_env[FWD_RAYFIN_APP_BACKEND_ID_ENV_VAR] == ITEM_ID
    assert migration_env[FWD_RAYFIN_SQL_DATABASE_ID_ENV_VAR] == DATABASE_ID
    assert migration_env[FWD_RAYFIN_SQL_SERVER_ENV_VAR] == SQL_SERVER
    assert migration_env[FWD_RAYFIN_SQL_DATABASE_NAME_ENV_VAR] == DATABASE_NAME
    assert migration_env[FWD_RAYFIN_SQL_ACCESS_TOKEN_ENV_VAR] == "sql-short-lived-token"
    assert docker_cli.calls[-2]["container_name"].endswith("-data-migrate")
    assert all(call["env"][FWD_RAYFIN_SQL_ACCESS_TOKEN_ENV_VAR] == "" for call in docker_cli.calls if call["command"][:2] != ["sh", "-c"])
    assert list((tmp_path / "staging").iterdir()) == []
    assert not (app_root / "rayfin" / ".deployments.json").exists()


@pytest.mark.parametrize("failure", ["No managed SQL Database relationship", "Ambiguous managed SQL Database relationship"])
def test_migration_target_failure_prevents_sql_token_and_application_command(tmp_path, monkeypatch, failure):
    _enable_migrations(_write_app(tmp_path))
    az_cli = FakeAzCli()
    docker_cli = FakeDockerCli()
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    manager = _manager(tmp_path, tmp_path / "staging", None, az_cli=az_cli, docker_cli=docker_cli, database_client=FakeDatabaseClient(error=failure))

    with pytest.raises(RuntimeError, match=failure):
        asyncio.run(manager.execute())

    assert az_cli.calls == []
    assert not any(call["command"][:2] == ["sh", "-c"] for call in docker_cli.calls)
    assert len(list((tmp_path / "staging").iterdir())) == 1


def test_migrations_reject_wrong_database_workspace_before_token(tmp_path, monkeypatch):
    _enable_migrations(_write_app(tmp_path))
    az_cli = FakeAzCli()
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    database = RayfinManagedSqlDatabase(MODEL_WORKSPACE_ID, ITEM_ID, DATABASE_ID, SQL_SERVER, DATABASE_NAME)

    with pytest.raises(RuntimeError, match="guarded workspace"):
        asyncio.run(_manager(tmp_path, tmp_path / "staging", None, az_cli=az_cli, database_client=FakeDatabaseClient(database=database)).execute())

    assert az_cli.calls == []


def test_migrations_reject_non_appbackend_before_target_resolution(tmp_path, monkeypatch):
    _enable_migrations(_write_app(tmp_path))
    database_client = FakeDatabaseClient()
    az_cli = FakeAzCli()
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")

    with pytest.raises(RuntimeError, match="must be an AppBackend"):
        asyncio.run(_manager(tmp_path, tmp_path / "staging", None, az_cli=az_cli, fabric_cli=FakeFabricCli(app_type="SQLDatabase"), database_client=database_client).execute())

    assert database_client.calls == az_cli.calls == []


def test_absent_migrations_do_not_resolve_database_or_acquire_sql_token(tmp_path, monkeypatch):
    _write_app(tmp_path)
    database_client = FakeDatabaseClient(error="must not resolve")
    az_cli = FakeAzCli()
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")

    asyncio.run(_manager(tmp_path, tmp_path / "staging", None, az_cli=az_cli, database_client=database_client).execute())

    assert database_client.calls == az_cli.calls == []


def test_migration_failure_retains_staging_and_prevents_status(tmp_path, monkeypatch):
    _enable_migrations(_write_app(tmp_path))
    docker_cli = FakeDockerCli(fail_command=["sh", "-c", "npm run data:migrate"])
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")

    with pytest.raises(RuntimeError, match="migration command failed"):
        asyncio.run(_manager(tmp_path, tmp_path / "staging", None, docker_cli=docker_cli, database_client=FakeDatabaseClient()).execute())

    assert docker_cli.calls[-1]["command"] == ["sh", "-c", "npm run data:migrate"]
    assert len(list((tmp_path / "staging").iterdir())) == 1


@pytest.mark.parametrize("structured", [False, True])
@pytest.mark.parametrize("environment_token", [False, True])
def test_migration_output_redacts_sql_credentials_connection_identity_and_inherited_secrets(tmp_path, monkeypatch, caplog, structured, environment_token):
    _enable_migrations(_write_app(tmp_path))
    sql_token = "sql-env-secret" if environment_token else "sql-short-lived-token"
    az_cli = AzCli() if environment_token else FakeAzCli()
    if environment_token:
        monkeypatch.setenv("FAB_TOKEN_SQL", f" \n {sql_token} \t")
        monkeypatch.setattr("fabric_workspace_deployment.manager.azure.cli.Popen", lambda *args, **kwargs: pytest.fail("A supplied SQL credential must not fall back to Azure CLI"))
    content = f"{sql_token} {SQL_SERVER} {DATABASE_NAME} inherited-password"
    stdout = json.dumps({"status": "Succeeded", "detail": content}) if structured else content
    docker_cli = FakeDockerCli(migration_stdout=stdout, migration_stderr=content)
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    monkeypatch.setenv("API_PASSWORD", "inherited-password")
    caplog.set_level("DEBUG")

    asyncio.run(_manager(tmp_path, tmp_path / "staging", None, az_cli=az_cli, docker_cli=docker_cli, database_client=FakeDatabaseClient()).execute())

    for secret in (sql_token, SQL_SERVER, DATABASE_NAME, "inherited-password"):
        assert secret not in caplog.text
    assert "******" in caplog.text


@pytest.mark.parametrize("failure_mode", ["command", "timeout", "structured-stdout", "structured-stderr"])
def test_sql_environment_token_failure_is_redacted_and_never_switches_identity(tmp_path, monkeypatch, caplog, failure_mode):
    _enable_migrations(_write_app(tmp_path))
    sql_token = "rejected-sql-env-token"
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    monkeypatch.setenv("FAB_TOKEN_SQL", f" \n {sql_token} \t")
    monkeypatch.setattr("fabric_workspace_deployment.manager.azure.cli.Popen", lambda *args, **kwargs: pytest.fail("A rejected supplied SQL token must not switch identities"))
    diagnostics = json.dumps({"status": "Failed", "detail": sql_token})
    docker_cli = FakeDockerCli(migration_stdout=diagnostics if failure_mode == "structured-stdout" else "", migration_stderr=diagnostics if failure_mode == "structured-stderr" else "")
    original_compose_run = docker_cli.compose_run
    migration_environments = []

    def compose_run(compose_file, project_name, service, command, **kwargs):
        result = original_compose_run(compose_file, project_name, service, command, **kwargs)
        if command[:2] == ["sh", "-c"]:
            migration_environments.append(kwargs["env"])
            if failure_mode == "command":
                raise RuntimeError(f"SQL rejected {sql_token}") from ValueError(sql_token)
            if failure_mode == "timeout":
                raise DockerCliError(f"Migration timed out: {sql_token}", stdout=sql_token, stderr=sql_token, timed_out=True)
        return result

    monkeypatch.setattr(docker_cli, "compose_run", compose_run)
    caplog.set_level("DEBUG")
    expected_error = "migration command failed" if failure_mode in ("command", "timeout") else "unsuccessful diagnostics"

    with pytest.raises(RuntimeError, match=expected_error) as failure:
        asyncio.run(_manager(tmp_path, tmp_path / "staging", None, az_cli=AzCli(), docker_cli=docker_cli, database_client=FakeDatabaseClient()).execute())

    assert sql_token not in caplog.text + "".join(traceback.format_exception(failure.type, failure.value, failure.tb))
    assert migration_environments[0][FWD_RAYFIN_SQL_ACCESS_TOKEN_ENV_VAR] == ""
    assert docker_cli.calls[-1]["command"] == ["sh", "-c", "npm run data:migrate"]
    retained_staging = list((tmp_path / "staging").iterdir())
    assert len(retained_staging) == 1
    persisted = "\n".join(path.read_text() for path in retained_staging[0].rglob("*") if path.is_file())
    assert sql_token not in persisted
    assert SQL_SERVER not in persisted
    assert DATABASE_NAME not in persisted


@pytest.mark.parametrize("banner", ["", "> app data:migrate\n> node migrations/run.mjs\n"])
def test_migration_structured_failure_fails_even_when_process_succeeded(tmp_path, monkeypatch, banner):
    _enable_migrations(_write_app(tmp_path))
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    docker_cli = FakeDockerCli(migration_stdout=banner + '{"success":false}')

    with pytest.raises(RuntimeError, match="unsuccessful diagnostics"):
        asyncio.run(_manager(tmp_path, tmp_path / "staging", None, docker_cli=docker_cli, database_client=FakeDatabaseClient()).execute())

    assert docker_cli.calls[-1]["command"] == ["sh", "-c", "npm run data:migrate"]


def test_structured_stderr_migration_failure_is_not_treated_as_success(tmp_path, monkeypatch):
    _enable_migrations(_write_app(tmp_path))
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    docker_cli = FakeDockerCli(migration_stderr='{"status":"Failed"}')

    with pytest.raises(RuntimeError, match="unsuccessful diagnostics"):
        asyncio.run(_manager(tmp_path, tmp_path / "staging", None, docker_cli=docker_cli, database_client=FakeDatabaseClient()).execute())


def test_migration_rerun_re_resolves_target_and_acquires_fresh_token(tmp_path, monkeypatch):
    _enable_migrations(_write_app(tmp_path))
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    az_cli = FakeAzCli()
    database_client = FakeDatabaseClient()
    docker_cli = FakeDockerCli()
    manager = _manager(tmp_path, tmp_path / "staging", None, az_cli=az_cli, docker_cli=docker_cli, database_client=database_client)

    asyncio.run(manager.execute())
    asyncio.run(manager.execute())

    assert database_client.calls == [(WORKSPACE_ID, ITEM_ID), (WORKSPACE_ID, ITEM_ID)]
    assert az_cli.calls == ["https://database.windows.net", "https://database.windows.net"]
    assert sum(call["command"] == ["sh", "-c", "npm run data:migrate"] for call in docker_cli.calls) == 2


def test_failed_migration_rerun_keeps_diagnostics_credential_free_and_reconciles_fresh_staging(tmp_path, monkeypatch):
    _enable_migrations(_write_app(tmp_path))
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    az_cli = FakeAzCli()
    database_client = FakeDatabaseClient()
    docker_cli = FakeDockerCli(fail_command=["sh", "-c", "npm run data:migrate"])
    manager = _manager(tmp_path, tmp_path / "staging", None, az_cli=az_cli, docker_cli=docker_cli, database_client=database_client)

    with pytest.raises(RuntimeError, match="migration command failed"):
        asyncio.run(manager.execute())

    failed_staging = next((tmp_path / "staging").iterdir())
    persisted = "\n".join(path.read_text() for path in failed_staging.rglob("*") if path.is_file())
    assert "sql-short-lived-token" not in persisted
    assert SQL_SERVER not in persisted
    assert DATABASE_NAME not in persisted
    docker_cli.fail_command = None

    asyncio.run(manager.execute())

    assert not failed_staging.exists()
    assert database_client.calls == [(WORKSPACE_ID, ITEM_ID), (WORKSPACE_ID, ITEM_ID)]
    assert az_cli.calls == ["https://database.windows.net", "https://database.windows.net"]
    assert list((tmp_path / "staging").iterdir()) == []


def test_migration_dry_run_is_local_only_and_reports_repeatable_execution(tmp_path, caplog):
    _enable_migrations(_write_app(tmp_path))
    az_cli = FakeAzCli()
    database_client = FakeDatabaseClient(error="must not resolve")
    docker_cli = FakeDockerCli()
    caplog.set_level("INFO")

    _manager(tmp_path, tmp_path / "staging", None, az_cli=az_cli, docker_cli=docker_cli, database_client=database_client).report_plan()

    assert "data.migrations.command" in caplog.text
    assert "migration ledger" in caplog.text
    assert database_client.calls == az_cli.calls == docker_cli.calls == []
    assert not (tmp_path / "staging").exists()


def test_force_binding_enables_destructive_schema_migrations(tmp_path, monkeypatch):
    _write_app(tmp_path)
    docker_cli = FakeDockerCli()
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    manager = _manager(
        tmp_path,
        tmp_path / "staging",
        tmp_path / "state",
        docker_cli=docker_cli,
        rayfin_params=[
            RayfinParams(
                root_path="apps/sales",
                semantic_models={
                    "sales": RayfinSemanticModelParams(item_name="Sales Model"),
                },
                force=True,
            )
        ],
    )

    asyncio.run(manager.execute())

    assert ["./node_modules/.bin/rayfin", "up", "--yes", "--force"] in [call["command"] for call in docker_cli.calls]


def test_default_staging_root_is_beneath_common_local_root(tmp_path):
    manager = _manager(tmp_path, None, tmp_path / "state", rayfin_params=[])

    assert manager.staging_root == tmp_path / ".fabric-workspace-deployment" / "rayfin-staging"


def test_staging_copy_excludes_internal_fwd_directory_when_source_is_common_root(tmp_path):
    app_root = _write_app(tmp_path)
    manifest = RayfinManifestLoader().load(app_root)
    manager = _manager(tmp_path, None, tmp_path / "state", rayfin_params=[])
    internal_marker = tmp_path / ".fabric-workspace-deployment" / "internal-marker.txt"
    internal_marker.parent.mkdir(parents=True)
    internal_marker.write_text("must not be copied", encoding="utf-8")

    staging_path = manager._create_staging_tree(tmp_path, manifest)

    assert not (staging_path / ".fabric-workspace-deployment").exists()


def test_uses_azure_cli_token_when_rayfin_token_is_absent(tmp_path, monkeypatch):
    _write_app(tmp_path)
    az_cli = FakeAzCli()
    docker_cli = FakeDockerCli()
    monkeypatch.delenv("RAYFIN_TOKEN", raising=False)
    manager = _manager(tmp_path, tmp_path / "staging", tmp_path / "state", az_cli=az_cli, docker_cli=docker_cli)

    asyncio.run(manager.execute())

    assert az_cli.calls == ["https://analysis.windows.net/powerbi/api"]
    assert docker_cli.calls[0]["env"]["RAYFIN_TOKEN"] == "az-token"


def test_ignores_persisted_registry_before_deployment(tmp_path, monkeypatch):
    _write_app(tmp_path)
    state_root = tmp_path / "state"
    persisted_registry = state_root / "sales-insights" / WORKSPACE_ID / ".deployments.json"
    persisted_registry.parent.mkdir(parents=True)
    persisted_registry.write_text(json.dumps({"deployments": [{"workspaceId": WORKSPACE_ID, "fabricItemId": ITEM_ID}]}), encoding="utf-8")
    docker_cli = FakeDockerCli()
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    manager = _manager(tmp_path, tmp_path / "staging", state_root, docker_cli=docker_cli)

    asyncio.run(manager.execute())

    assert docker_cli.seeded_registry_seen is False


def test_failure_retains_staging_with_generated_files_and_without_token(tmp_path, monkeypatch):
    app_root = _write_app(tmp_path)
    staging_root = tmp_path / "staging"
    docker_cli = FakeDockerCli(fail_command=["./node_modules/.bin/rayfin", "up", "--yes"])
    monkeypatch.setenv("RAYFIN_TOKEN", "do-not-write-this-token")
    manager = _manager(tmp_path, staging_root, tmp_path / "state", docker_cli=docker_cli)

    with pytest.raises(RuntimeError, match="simulated Docker failure"):
        asyncio.run(manager.execute())

    retained = list(staging_root.iterdir())
    assert len(retained) == 1
    assert (retained[0] / "fabric.yaml").is_file()
    assert (retained[0] / "rayfin" / "rayfin.yml").is_file()
    assert not (app_root / "fabric.yaml").exists()
    for file_path in retained[0].rglob("*"):
        if file_path.is_file():
            assert "do-not-write-this-token" not in file_path.read_text(encoding="utf-8")


def test_version_mismatch_fails_before_rayfin_up_and_retains_staging(tmp_path, monkeypatch):
    _write_app(tmp_path)
    staging_root = tmp_path / "staging"
    docker_cli = FakeDockerCli(version="1.36.0")
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    manager = _manager(tmp_path, staging_root, tmp_path / "state", docker_cli=docker_cli)

    with pytest.raises(RuntimeError, match="version mismatch"):
        asyncio.run(manager.execute())

    assert [call["command"] for call in docker_cli.calls] == [
        ["npm", "ci"],
        ["./node_modules/.bin/rayfin", "--version"],
    ]
    assert len(list(staging_root.iterdir())) == 1


def test_unhealthy_status_fails_without_persisting_registry(tmp_path, monkeypatch):
    _write_app(tmp_path)
    staging_root = tmp_path / "staging"
    state_root = tmp_path / "state"
    docker_cli = FakeDockerCli(status={"health": "unhealthy"})
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    manager = _manager(tmp_path, staging_root, state_root, docker_cli=docker_cli)

    with pytest.raises(RuntimeError, match="unhealthy deployment state"):
        asyncio.run(manager.execute())

    assert not state_root.exists()
    assert len(list(staging_root.iterdir())) == 1


def test_registry_reported_data_disabled_fails_validation(tmp_path, monkeypatch):
    _write_app(tmp_path)
    staging_root = tmp_path / "staging"
    docker_cli = FakeDockerCli(registry_data_enabled=False)
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    manager = _manager(tmp_path, staging_root, tmp_path / "state", docker_cli=docker_cli)

    with pytest.raises(RuntimeError, match="deployment registry reports managed data enabled=False"):
        asyncio.run(manager.execute())

    assert len(list(staging_root.iterdir())) == 1


def test_status_reported_data_disabled_fails_validation(tmp_path, monkeypatch):
    _write_app(tmp_path)
    staging_root = tmp_path / "staging"
    state_root = tmp_path / "state"
    docker_cli = FakeDockerCli(status={"status": "Running", "healthy": True, "services": {"data": {"enabled": False}}})
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    manager = _manager(tmp_path, staging_root, state_root, docker_cli=docker_cli)

    with pytest.raises(RuntimeError, match="up status reports managed data enabled=False"):
        asyncio.run(manager.execute())

    assert not state_root.exists()
    assert len(list(staging_root.iterdir())) == 1


def test_missing_status_data_report_is_accepted_when_command_succeeds(tmp_path, monkeypatch):
    _write_app(tmp_path)
    docker_cli = FakeDockerCli(status={"status": "Running", "healthy": True})
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    manager = _manager(tmp_path, tmp_path / "staging", tmp_path / "state", docker_cli=docker_cli)

    asyncio.run(manager.execute())


def test_fabric_item_id_mismatch_fails_assertion(tmp_path, monkeypatch):
    _write_app(tmp_path)
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    fabric_cli = FakeFabricCli(app_item_id="55555555-5555-5555-5555-555555555555")
    state_root = tmp_path / "state"
    manager = _manager(tmp_path, tmp_path / "staging", state_root, fabric_cli=fabric_cli)

    with pytest.raises(RuntimeError, match="registry item ID"):
        asyncio.run(manager.execute())

    assert not (state_root / "sales-insights" / WORKSPACE_ID / ".deployments.json").exists()


def test_fabric_item_assertion_uses_ids_when_display_name_has_spaces_or_canonical_mismatch(tmp_path):
    fabric_cli = FakeFabricCli(app_display_name="sql-modernization-navigator")
    manager = _manager(tmp_path, tmp_path / "staging", tmp_path / "state", fabric_cli=fabric_cli)

    manager._assert_fabric_item(
        WORKSPACE_ID,
        RayfinDeploymentRecord(
            workspace_id=WORKSPACE_ID,
            fabric_item_id=ITEM_ID,
        ),
    )

    assert fabric_cli.calls == [(["api", f"workspaces/{WORKSPACE_ID}/items/{ITEM_ID}", "-X", "get"], 60)]
    assert all("Sales Insights" not in token for token in fabric_cli.calls[0][0])


def test_fabric_item_assertion_accepts_unwrapped_api_response(tmp_path):
    fabric_cli = FakeFabricCli(wrap_api_response=False)
    manager = _manager(tmp_path, tmp_path / "staging", tmp_path / "state", fabric_cli=fabric_cli)

    manager._assert_fabric_item(
        WORKSPACE_ID,
        RayfinDeploymentRecord(
            workspace_id=WORKSPACE_ID,
            fabric_item_id=ITEM_ID,
        ),
    )


def test_fabric_item_assertion_rejects_item_from_different_workspace(tmp_path):
    fabric_cli = FakeFabricCli(app_workspace_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    manager = _manager(tmp_path, tmp_path / "staging", tmp_path / "state", fabric_cli=fabric_cli)

    with pytest.raises(RuntimeError, match="belongs to workspace"):
        manager._assert_fabric_item(
            WORKSPACE_ID,
            RayfinDeploymentRecord(
                workspace_id=WORKSPACE_ID,
                fabric_item_id=ITEM_ID,
            ),
        )


def test_friendly_semantic_model_resolution_error_stops_before_docker(tmp_path, monkeypatch):
    _write_app(tmp_path)
    staging_root = tmp_path / "staging"
    docker_cli = FakeDockerCli()
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    manager = _manager(tmp_path, staging_root, tmp_path / "state", fabric_cli=FakeFabricCli(fail_model=True), docker_cli=docker_cli)

    with pytest.raises(ValueError, match=r"Unable to resolve semantic model 'Sales Model' for alias 'sales' at common\.fabric\.workspaces\[0\]\.rayfins\[0\]\.semanticModels\['sales'\] in workspace 'Semantic Models'"):
        asyncio.run(manager.execute())

    assert docker_cli.calls == []
    assert not staging_root.exists()


def test_parent_skip_deploy_skips_nested_apps(tmp_path, caplog):
    docker_cli = FakeDockerCli()
    fabric_cli = FakeFabricCli()
    az_cli = FakeAzCli()
    manager = _manager(
        tmp_path,
        tmp_path / "staging",
        tmp_path / "state",
        az_cli=az_cli,
        fabric_cli=fabric_cli,
        docker_cli=docker_cli,
        workspace_params=[
            SimpleNamespace(
                name="Analytics",
                skip_deploy=True,
                rayfins=[
                    RayfinParams(
                        root_path="does-not-need-to-exist",
                        semantic_models={},
                    )
                ],
            )
        ],
    )
    caplog.set_level("INFO")

    asyncio.run(manager.execute())

    assert az_cli.calls == []
    assert fabric_cli.calls == []
    assert docker_cli.calls == []
    assert "common.fabric.workspaces[0]" in caplog.text
    assert "skipDeploy=true" in caplog.text


def test_absent_rayfin_config_is_noop(tmp_path):
    docker_cli = FakeDockerCli()
    fabric_cli = FakeFabricCli()
    manager = _manager(tmp_path, tmp_path / "staging", tmp_path / "state", fabric_cli=fabric_cli, docker_cli=docker_cli, rayfin_params=[])

    asyncio.run(manager.execute())

    assert docker_cli.calls == []
    assert fabric_cli.calls == []
