# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import json

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from fabric_workspace_deployment.manager.rayfin.deployment import RayfinDeploymentManager
from fabric_workspace_deployment.operations.operation_interfaces import RayfinParams, RayfinSemanticModelParams
from fabric_workspace_deployment.rayfin.manifest import RayfinDeploymentRecord, RayfinManifestLoader

WORKSPACE_ID = "11111111-1111-1111-1111-111111111111"
MODEL_WORKSPACE_ID = "55555555-5555-5555-5555-555555555555"
MODEL_ID = "22222222-2222-2222-2222-222222222222"
ITEM_ID = "33333333-3333-3333-3333-333333333333"
TENANT_ID = "44444444-4444-4444-4444-444444444444"


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


class FakeFabricCli:
    def __init__(self, *, fail_model: bool = False, app_item_id: str = ITEM_ID, app_workspace_id: str = WORKSPACE_ID, app_display_name: str = "sales-insights", wrap_api_response: bool = True):
        self.calls = []
        self.fail_model = fail_model
        self.app_item_id = app_item_id
        self.app_workspace_id = app_workspace_id
        self.app_display_name = app_display_name
        self.wrap_api_response = wrap_api_response

    def run(self, command, timeout=None):
        self.calls.append((list(command), timeout))
        if command[0] == "api":
            assert command == ["api", f"workspaces/{WORKSPACE_ID}/items/{ITEM_ID}", "-X", "get"]
            item_data = {
                "id": self.app_item_id,
                "workspaceId": self.app_workspace_id,
                "displayName": self.app_display_name,
                "type": "AppBackend",
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
    def __init__(self, *, fail_command: list[str] | None = None, version: str = "1.35.1", status: object | None = None, registry_data_enabled: bool | None = True):
        self.calls = []
        self.fail_command = fail_command
        self.version = version
        self.status = status if status is not None else {"status": "Running", "healthy": True, "services": {"data": {"enabled": True}}}
        self.registry_data_enabled = registry_data_enabled
        self.generated_fabric_yaml = None
        self.generated_rayfin_yaml = None
        self.compose_text = None
        self.seeded_registry_seen = False

    def compose_run(self, compose_file, project_name, service, command, *, timeout=None, env=None):
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
        if command == ["./node_modules/.bin/rayfin", "up", "--yes"]:
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
        return "", ""


def _manager(root: Path, staging_root: Path | None, state_root: Path | None, az_cli=None, fabric_cli=None, docker_cli=None, rayfin_params=None, workspace_params=None):
    kwargs = {}
    if staging_root is not None:
        kwargs["staging_root"] = staging_root
    if state_root is not None:
        kwargs["state_root"] = state_root
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
        **kwargs,
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
    assert not (app_root / "fabric.yaml").exists()
    assert not (app_root / "rayfin" / "rayfin.yml").exists()
    assert staging_root.exists()
    assert list(staging_root.iterdir()) == []
    assert (state_root / "sales-insights" / WORKSPACE_ID / ".deployments.json").is_file()
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

    staging_path = manager._create_staging_tree(tmp_path, manifest, WORKSPACE_ID)

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


def test_seeds_persisted_registry_before_deployment(tmp_path, monkeypatch):
    _write_app(tmp_path)
    state_root = tmp_path / "state"
    persisted_registry = state_root / "sales-insights" / WORKSPACE_ID / ".deployments.json"
    persisted_registry.parent.mkdir(parents=True)
    persisted_registry.write_text(json.dumps({"deployments": [{"workspaceId": WORKSPACE_ID, "fabricItemId": ITEM_ID}]}), encoding="utf-8")
    docker_cli = FakeDockerCli()
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    manager = _manager(tmp_path, tmp_path / "staging", state_root, docker_cli=docker_cli)

    asyncio.run(manager.execute())

    assert docker_cli.seeded_registry_seen is True


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


def test_unhealthy_status_fails_after_persisting_registry(tmp_path, monkeypatch):
    _write_app(tmp_path)
    staging_root = tmp_path / "staging"
    state_root = tmp_path / "state"
    docker_cli = FakeDockerCli(status={"health": "unhealthy"})
    monkeypatch.setenv("RAYFIN_TOKEN", "configured-token")
    manager = _manager(tmp_path, staging_root, state_root, docker_cli=docker_cli)

    with pytest.raises(RuntimeError, match="unhealthy deployment state"):
        asyncio.run(manager.execute())

    assert (state_root / "sales-insights" / WORKSPACE_ID / ".deployments.json").is_file()
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

    assert (state_root / "sales-insights" / WORKSPACE_ID / ".deployments.json").is_file()
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
