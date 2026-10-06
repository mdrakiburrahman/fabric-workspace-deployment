# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import json

from pathlib import Path

import pytest
import yaml

from fabric_workspace_deployment.rayfin.manifest import RayfinManifestLoader, RayfinManifestRenderer, ResolvedSemanticModel


def _manifest_data() -> dict:
    return {
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
            "semanticModels": ["sales", "inventory"],
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


def _write_manifest(app_root: Path, data: dict | None = None) -> Path:
    app_root.mkdir(parents=True, exist_ok=True)
    manifest_path = app_root / "fabric-workspace-deployment.json"
    manifest_path.write_text(json.dumps(data or _manifest_data()), encoding="utf-8")
    return manifest_path


def test_loads_valid_manifest(tmp_path):
    app_root = tmp_path / "app"
    _write_manifest(app_root)

    manifest = RayfinManifestLoader().load(app_root)

    assert manifest.schema_version == "1.0"
    assert manifest.kind == "rayfin"
    assert manifest.app.id == "sales-insights"
    assert manifest.app.name == "Sales Insights"
    assert manifest.app.version == "2.4.0"
    assert manifest.rayfin.version == "1.35.1"
    assert manifest.connections.semantic_models == ["sales", "inventory"]
    assert manifest.build.output_path == "dist"
    assert manifest.data.enabled is True
    assert manifest.data.dialect == "mssql"


def _write_functions_app(app_root: Path, *, workspace_pattern: str = "rayfin/*", nested_sdk: bool = False) -> Path:
    data = _manifest_data()
    data["functions"] = {"enabled": True, "path": "rayfin/functions", "buildCommand": "npm run build"}
    _write_manifest(app_root, data)
    functions_root = app_root / "rayfin" / "functions"
    (functions_root / "src").mkdir(parents=True)
    sdk = "@microsoft/fabric-user-data-functions"
    (functions_root / "package.json").write_text(json.dumps({"name": "test-functions", "private": True, "type": "module", "main": "dist/src/function_app.js", "scripts": {"build": "tsc --build"}, "dependencies": {sdk: "1.35.1"}}), encoding="utf-8")
    (functions_root / "host.json").write_text(json.dumps({"version": "2.0"}), encoding="utf-8")
    (functions_root / "tsconfig.json").write_text(json.dumps({"compilerOptions": {"outDir": "dist"}, "include": ["src"]}), encoding="utf-8")
    (functions_root / "src" / "function_app.ts").write_text("export {};\n", encoding="utf-8")
    (app_root / "package.json").write_text(json.dumps({"name": "test-app", "private": True, "workspaces": [workspace_pattern], "devDependencies": {"@microsoft/rayfin-cli": "1.35.1"}}), encoding="utf-8")
    sdk_prefix = "rayfin/functions/" if nested_sdk else ""
    (app_root / "package-lock.json").write_text(
        json.dumps(
            {
                "lockfileVersion": 3,
                "packages": {
                    "node_modules/@microsoft/rayfin-cli": {"version": "1.35.1"},
                    "rayfin/functions": {"dependencies": {sdk: "1.35.1"}},
                    "node_modules/test-functions": {"resolved": "rayfin/functions", "link": True},
                    f"{sdk_prefix}node_modules/{sdk}": {"version": "1.35.1", "dependencies": {"@microsoft/rayfin-client": "1.35.1"}},
                },
            }
        ),
        encoding="utf-8",
    )
    return functions_root


def test_functions_default_disabled_and_omitted_from_yaml(tmp_path):
    _write_manifest(tmp_path)
    manifest = RayfinManifestLoader().load(tmp_path)

    assert manifest.functions.enabled is False
    assert "functions" not in yaml.safe_load(RayfinManifestRenderer().render_rayfin_yaml(manifest))["services"]


def test_explicitly_disabled_functions_do_not_require_project_or_change_yaml(tmp_path):
    data = _manifest_data()
    data["functions"] = {"enabled": False}
    _write_manifest(tmp_path, data)
    manifest = RayfinManifestLoader().load(tmp_path)

    assert manifest.functions.enabled is False
    assert "functions" not in yaml.safe_load(RayfinManifestRenderer().render_rayfin_yaml(manifest))["services"]


@pytest.mark.parametrize("nested_sdk", [False, True])
def test_functions_workspace_is_valid_without_installed_dependencies(tmp_path, nested_sdk):
    _write_functions_app(tmp_path, nested_sdk=nested_sdk)
    loader = RayfinManifestLoader()
    manifest = loader.load(tmp_path)

    loader.validate_node_package(tmp_path, manifest)

    assert not (tmp_path / "node_modules").exists()
    assert yaml.safe_load(RayfinManifestRenderer().render_rayfin_yaml(manifest))["services"]["functions"] == {
        "enabled": True,
        "auth": {"type": "application"},
        "path": "rayfin/functions",
        "buildCommand": "npm run build",
    }


@pytest.mark.parametrize(
    ("functions", "message"),
    [
        ({"enabled": "true"}, "boolean"),
        ({"enabled": True}, "missing"),
        ({"enabled": False, "unexpected": True}, "unexpected"),
        ({"enabled": True, "path": "../functions", "buildCommand": "npm run build"}, "relative path"),
        ({"enabled": True, "path": "/functions", "buildCommand": "npm run build"}, "relative path"),
        ({"enabled": True, "path": "rayfin/functions", "buildCommand": " "}, "non-empty"),
    ],
)
def test_rejects_invalid_functions_contract(tmp_path, functions, message):
    data = _manifest_data()
    data["functions"] = functions
    _write_manifest(tmp_path, data)

    with pytest.raises(ValueError, match=message):
        RayfinManifestLoader().load(tmp_path)


def test_rejects_functions_root_symlink_escape(tmp_path):
    app_root = tmp_path / "app"
    _write_functions_app(app_root)
    external = tmp_path / "external"
    external.mkdir()
    (app_root / "functions-link").symlink_to(external, target_is_directory=True)
    data = json.loads((app_root / "fabric-workspace-deployment.json").read_text())
    data["functions"]["path"] = "functions-link"
    _write_manifest(app_root, data)

    with pytest.raises(ValueError, match="symlinks"):
        RayfinManifestLoader().load(app_root)


@pytest.mark.parametrize("name", ["package.json", "host.json", "tsconfig.json", "src/function_app.ts"])
def test_requires_functions_contract_files(tmp_path, name):
    functions_root = _write_functions_app(tmp_path)
    (functions_root / name).unlink()
    loader = RayfinManifestLoader()

    with pytest.raises(ValueError, match="requires a non-empty"):
        loader.validate_node_package(tmp_path, loader.load(tmp_path))


def test_rejects_symlink_escape_in_functions_contract_files(tmp_path):
    app_root = tmp_path / "app"
    functions_root = _write_functions_app(app_root)
    external = tmp_path / "external.json"
    external.write_text('{"version":"2.0"}', encoding="utf-8")
    (functions_root / "host.json").unlink()
    (functions_root / "host.json").symlink_to(external)
    loader = RayfinManifestLoader()

    with pytest.raises(ValueError, match="inside the Functions project"):
        loader.validate_node_package(app_root, loader.load(app_root))


@pytest.mark.parametrize("version", ["experimental", "^1.35.1", "1.36.0", None])
def test_rejects_functions_sdk_pin_mismatch(tmp_path, version):
    functions_root = _write_functions_app(tmp_path)
    package = json.loads((functions_root / "package.json").read_text())
    package["dependencies"]["@microsoft/fabric-user-data-functions"] = version
    (functions_root / "package.json").write_text(json.dumps(package), encoding="utf-8")
    loader = RayfinManifestLoader()

    with pytest.raises(ValueError, match="@microsoft/fabric-user-data-functions"):
        loader.validate_node_package(tmp_path, loader.load(tmp_path))


@pytest.mark.parametrize("workspace_pattern", ["other/*", "../*", "/rayfin/functions", "", "!rayfin/functions"])
def test_rejects_missing_or_invalid_functions_workspace(tmp_path, workspace_pattern):
    _write_functions_app(tmp_path, workspace_pattern=workspace_pattern)
    loader = RayfinManifestLoader()

    with pytest.raises(ValueError, match="workspace"):
        loader.validate_node_package(tmp_path, loader.load(tmp_path))


@pytest.mark.parametrize("entry", ["rayfin/functions", "node_modules/test-functions", "node_modules/@microsoft/fabric-user-data-functions"])
def test_rejects_incomplete_functions_workspace_lock(tmp_path, entry):
    _write_functions_app(tmp_path)
    lock_path = tmp_path / "package-lock.json"
    lock = json.loads(lock_path.read_text())
    del lock["packages"][entry]
    lock_path.write_text(json.dumps(lock), encoding="utf-8")
    loader = RayfinManifestLoader()

    with pytest.raises(ValueError, match="package-lock.json"):
        loader.validate_node_package(tmp_path, loader.load(tmp_path))


def test_rejects_missing_functions_build_script(tmp_path):
    functions_root = _write_functions_app(tmp_path)
    package_path = functions_root / "package.json"
    package = json.loads(package_path.read_text())
    package["scripts"] = {}
    package_path.write_text(json.dumps(package), encoding="utf-8")
    loader = RayfinManifestLoader()

    with pytest.raises(ValueError, match="non-empty Functions package script"):
        loader.validate_node_package(tmp_path, loader.load(tmp_path))


@pytest.mark.parametrize("spec", ["file:../../shared", "file:/tmp/shared", "file:missing.tgz"])
def test_rejects_nonportable_functions_dependencies(tmp_path, spec):
    functions_root = _write_functions_app(tmp_path)
    package_path = functions_root / "package.json"
    package = json.loads(package_path.read_text())
    package["dependencies"]["shared"] = spec
    package_path.write_text(json.dumps(package), encoding="utf-8")
    loader = RayfinManifestLoader()

    with pytest.raises(ValueError, match="Functions dependency"):
        loader.validate_node_package(tmp_path, loader.load(tmp_path))


@pytest.mark.parametrize(
    ("entry", "replacement", "message"),
    [
        ("node_modules/@microsoft/fabric-user-data-functions", {"version": "1.36.0"}, "resolve Functions"),
        ("node_modules/@microsoft/fabric-user-data-functions", {"version": "1.35.1", "dependencies": {"@microsoft/rayfin-client": "1.36.0"}}, "same declared Rayfin client"),
        ("node_modules/test-functions", {"resolved": "other/functions", "link": True}, "link the Functions workspace"),
    ],
)
def test_rejects_conflicting_functions_lock_identity(tmp_path, entry, replacement, message):
    _write_functions_app(tmp_path)
    lock_path = tmp_path / "package-lock.json"
    lock = json.loads(lock_path.read_text())
    lock["packages"][entry] = replacement
    lock_path.write_text(json.dumps(lock), encoding="utf-8")
    loader = RayfinManifestLoader()

    with pytest.raises(ValueError, match=message):
        loader.validate_node_package(tmp_path, loader.load(tmp_path))


def test_rejects_functions_workspace_excluded_after_positive_pattern(tmp_path):
    _write_functions_app(tmp_path)
    package_path = tmp_path / "package.json"
    package = json.loads(package_path.read_text())
    package["workspaces"].append("!rayfin/functions")
    package_path.write_text(json.dumps(package), encoding="utf-8")
    loader = RayfinManifestLoader()

    with pytest.raises(ValueError, match="without exclusions"):
        loader.validate_node_package(tmp_path, loader.load(tmp_path))


@pytest.mark.parametrize("content", ['{"version":"1.0"}', "not-json", "[]"])
def test_rejects_invalid_functions_host_configuration(tmp_path, content):
    functions_root = _write_functions_app(tmp_path)
    (functions_root / "host.json").write_text(content, encoding="utf-8")
    loader = RayfinManifestLoader()

    with pytest.raises(ValueError, match="Functions host.json"):
        loader.validate_node_package(tmp_path, loader.load(tmp_path))


def test_functions_workspace_object_syntax_is_supported(tmp_path):
    _write_functions_app(tmp_path)
    package_path = tmp_path / "package.json"
    package = json.loads(package_path.read_text())
    package["workspaces"] = {"packages": ["rayfin/functions"]}
    package_path.write_text(json.dumps(package), encoding="utf-8")
    loader = RayfinManifestLoader()

    loader.validate_node_package(tmp_path, loader.load(tmp_path))


def test_absent_data_block_defaults_to_disabled_mssql(tmp_path):
    data = _manifest_data()
    del data["data"]
    app_root = tmp_path / "app"
    _write_manifest(app_root, data)

    manifest = RayfinManifestLoader().load(app_root)

    assert manifest.data.enabled is False
    assert manifest.data.dialect == "mssql"
    assert yaml.safe_load(RayfinManifestRenderer().render_rayfin_yaml(manifest))["services"]["data"] == {
        "enabled": False,
        "dialect": "mssql",
    }


def test_accepts_post_schema_migrations_without_rendering_fwd_command_into_rayfin_yaml(tmp_path):
    data = _manifest_data()
    data["data"]["migrations"] = {"command": "npm run data:migrate"}
    _write_manifest(tmp_path, data)

    manifest = RayfinManifestLoader().load(tmp_path)

    assert manifest.data.migrations is not None
    assert manifest.data.migrations.command == "npm run data:migrate"
    assert yaml.safe_load(RayfinManifestRenderer().render_rayfin_yaml(manifest))["services"]["data"] == {"enabled": True, "dialect": "mssql"}


@pytest.mark.parametrize(
    ("migrations", "message"),
    [
        (None, "JSON object"),
        ("npm run migrate", "JSON object"),
        ({}, "missing"),
        ({"command": ""}, "non-empty"),
        ({"command": True}, "non-empty"),
        ({"command": "npm run migrate", "extra": True}, "unexpected"),
        ({"command": "npm run 'migrate"}, "valid shell command"),
        ({"command": "npm\x00run migrate"}, "NUL"),
    ],
)
def test_rejects_invalid_migration_contract(tmp_path, migrations, message):
    data = _manifest_data()
    data["data"]["migrations"] = migrations
    _write_manifest(tmp_path, data)

    with pytest.raises(ValueError, match=message):
        RayfinManifestLoader().load(tmp_path)


def test_migrations_require_enabled_managed_data(tmp_path):
    data = _manifest_data()
    data["data"] = {"enabled": False, "dialect": "mssql", "migrations": {"command": "npm run data:migrate"}}
    _write_manifest(tmp_path, data)

    with pytest.raises(ValueError, match="requires data.enabled=true"):
        RayfinManifestLoader().load(tmp_path)


def test_migration_npm_script_is_validated_at_app_root(tmp_path):
    _write_functions_app(tmp_path)
    manifest_path = tmp_path / "fabric-workspace-deployment.json"
    data = json.loads(manifest_path.read_text())
    data["data"]["migrations"] = {"command": "npm run data:migrate"}
    manifest_path.write_text(json.dumps(data), encoding="utf-8")
    loader = RayfinManifestLoader()
    manifest = loader.load(tmp_path)

    with pytest.raises(ValueError, match="root package script"):
        loader.validate_node_package(tmp_path, manifest)

    package_path = tmp_path / "package.json"
    package = json.loads(package_path.read_text())
    package["scripts"] = {"data:migrate": "node migrations/run.mjs"}
    package_path.write_text(json.dumps(package), encoding="utf-8")
    loader.validate_node_package(tmp_path, manifest)


def test_migration_registry_requires_explicit_workspace_and_accepts_canonical_rayfin_key(tmp_path):
    workspace_id = "11111111-1111-1111-1111-111111111111"
    item_id = "22222222-2222-2222-2222-222222222222"
    registry_path = tmp_path / ".deployments.json"
    registry_path.write_text(json.dumps({"deployments": [{"fabricItemId": item_id}]}), encoding="utf-8")
    loader = RayfinManifestLoader()

    with pytest.raises(ValueError, match="does not contain a deployment"):
        loader.load_deployment_record(registry_path, workspace_id, require_workspace=True)

    registry_path.write_text(json.dumps({"deployments": {"Friendly workspace": {"fabricItemId": item_id, "fabricWorkspaceId": workspace_id}}}), encoding="utf-8")
    assert loader.load_deployment_record(registry_path, workspace_id, require_workspace=True).fabric_item_id == item_id


def test_registry_rejects_conflicting_explicit_workspace_keys(tmp_path):
    registry_path = tmp_path / ".deployments.json"
    registry_path.write_text(json.dumps({"deployments": [{"fabricItemId": "22222222-2222-2222-2222-222222222222", "workspaceId": "11111111-1111-1111-1111-111111111111", "fabricWorkspaceId": "33333333-3333-3333-3333-333333333333"}]}), encoding="utf-8")

    with pytest.raises(ValueError, match="conflicting workspace"):
        RayfinManifestLoader().load_deployment_record(registry_path, "11111111-1111-1111-1111-111111111111")


@pytest.mark.parametrize(
    ("data_config", "message"),
    [
        ({"enabled": True}, "missing"),
        ({"enabled": True, "dialect": "mssql", "unexpected": True}, "unexpected"),
        ({"enabled": "true", "dialect": "mssql"}, "data.enabled must be a boolean"),
        ({"enabled": True, "dialect": "postgresql"}, "exactly 'mssql'"),
        ({"enabled": True, "dialect": "MSSQL"}, "exactly 'mssql'"),
    ],
)
def test_rejects_invalid_data_contract(tmp_path, data_config, message):
    data = _manifest_data()
    data["data"] = data_config
    app_root = tmp_path / "app"
    _write_manifest(app_root, data)

    with pytest.raises(ValueError, match=message):
        RayfinManifestLoader().load(app_root)


def test_validates_exact_rayfin_pin_in_node_package_files(tmp_path):
    app_root = tmp_path / "app"
    _write_manifest(app_root)
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
    loader = RayfinManifestLoader()
    manifest = loader.load(app_root)

    loader.validate_node_package(app_root, manifest)


@pytest.mark.parametrize(("package_version", "locked_version"), [("^1.35.1", "1.35.1"), ("1.35.1", "1.36.0"), (None, "1.35.1")])
def test_rejects_node_package_version_mismatch(tmp_path, package_version, locked_version):
    app_root = tmp_path / "app"
    _write_manifest(app_root)
    dependencies = {} if package_version is None else {"@microsoft/rayfin-cli": package_version}
    (app_root / "package.json").write_text(json.dumps({"devDependencies": dependencies}), encoding="utf-8")
    (app_root / "package-lock.json").write_text(
        json.dumps(
            {
                "lockfileVersion": 3,
                "packages": {
                    "node_modules/@microsoft/rayfin-cli": {
                        "version": locked_version,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    loader = RayfinManifestLoader()
    manifest = loader.load(app_root)

    with pytest.raises(ValueError, match="@microsoft/rayfin-cli"):
        loader.validate_node_package(app_root, manifest)


def test_enabled_data_requires_non_empty_schema_file(tmp_path):
    app_root = tmp_path / "app"
    _write_manifest(app_root)
    loader = RayfinManifestLoader()
    manifest = loader.load(app_root)

    with pytest.raises(ValueError, match="required schema file"):
        loader.validate_data_schema(app_root, manifest)

    schema_path = app_root / "rayfin" / "data" / "schema.ts"
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text("export const schema = {};\n", encoding="utf-8")
    loader.validate_data_schema(app_root, manifest)


def test_disabled_data_does_not_require_schema_file(tmp_path):
    data = _manifest_data()
    del data["data"]
    app_root = tmp_path / "app"
    _write_manifest(app_root, data)
    loader = RayfinManifestLoader()

    loader.validate_data_schema(app_root, loader.load(app_root))


@pytest.mark.parametrize("version", ["latest", "^1.35.1", "~1.35.1", "1.35", "v1.35.1", ""])
def test_rejects_non_exact_rayfin_version(tmp_path, version):
    data = _manifest_data()
    data["rayfin"]["version"] = version
    app_root = tmp_path / "app"
    _write_manifest(app_root, data)

    with pytest.raises(ValueError, match="exact semantic version"):
        RayfinManifestLoader().load(app_root)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("schemaVersion", "2.0", "Unsupported Rayfin manifest schemaVersion"),
        ("kind", "other", "Unsupported Rayfin manifest kind"),
    ],
)
def test_rejects_unsupported_contract_values(tmp_path, field, value, message):
    data = _manifest_data()
    data[field] = value
    app_root = tmp_path / "app"
    _write_manifest(app_root, data)

    with pytest.raises(ValueError, match=message):
        RayfinManifestLoader().load(app_root)


def test_rejects_unknown_manifest_fields(tmp_path):
    data = _manifest_data()
    data["unexpected"] = True
    app_root = tmp_path / "app"
    _write_manifest(app_root, data)

    with pytest.raises(ValueError, match="unexpected"):
        RayfinManifestLoader().load(app_root)


@pytest.mark.parametrize("path", ["/dist", "../dist", "assets/../../dist"])
def test_rejects_build_paths_outside_app_root(tmp_path, path):
    data = _manifest_data()
    data["build"]["outputPath"] = path
    app_root = tmp_path / "app"
    _write_manifest(app_root, data)

    with pytest.raises(ValueError, match="relative path"):
        RayfinManifestLoader().load(app_root)


def test_renders_fabric_and_rayfin_yaml(tmp_path):
    app_root = tmp_path / "app"
    _write_manifest(app_root)
    manifest = RayfinManifestLoader().load(app_root)
    renderer = RayfinManifestRenderer()

    fabric_yaml = yaml.safe_load(
        renderer.render_fabric_yaml(
            {
                "sales": ResolvedSemanticModel(workspace_id="11111111-1111-1111-1111-111111111111", item_id="22222222-2222-2222-2222-222222222222"),
            }
        )
    )
    rayfin_yaml = yaml.safe_load(renderer.render_rayfin_yaml(manifest))

    assert fabric_yaml == {
        "activeProfile": "deployment",
        "profiles": {
            "deployment": {
                "semanticModels": {
                    "sales": {
                        "workspaceId": "11111111-1111-1111-1111-111111111111",
                        "itemId": "22222222-2222-2222-2222-222222222222",
                    }
                }
            }
        },
    }
    assert rayfin_yaml == {
        "id": "sales-insights",
        "name": "Sales Insights",
        "version": "2.4.0",
        "services": {
            "auth": {
                "enabled": True,
                "password": {
                    "enabled": False,
                },
                "fabric": {
                    "enabled": True,
                },
            },
            "data": {
                "enabled": True,
                "dialect": "mssql",
            },
            "staticHosting": {
                "enabled": True,
                "folder": "dist",
                "buildCommand": "npm run build",
                "indexDocument": "index.html",
            },
        },
    }
    assert "allowedRedirectUris" not in rayfin_yaml["services"]["auth"]


def test_loads_registry_record_from_workspace_key(tmp_path):
    workspace_id = "11111111-1111-1111-1111-111111111111"
    item_id = "22222222-2222-2222-2222-222222222222"
    registry_path = tmp_path / ".deployments.json"
    registry_path.write_text(
        json.dumps(
            {
                "deployments": {
                    workspace_id: {
                        "fabricItemId": item_id,
                        "hostingUrl": "https://example.invalid",
                        "services": {
                            "data": {
                                "enabled": True,
                            }
                        },
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    record = RayfinManifestLoader().load_deployment_record(registry_path, workspace_id)

    assert record.workspace_id == workspace_id
    assert record.fabric_item_id == item_id
    assert record.data_enabled is True


def test_reports_data_enabled_from_status_shapes():
    loader = RayfinManifestLoader()

    assert loader.get_reported_data_enabled({"services": {"data": {"enabled": True}}}) is True
    assert loader.get_reported_data_enabled({"dataServiceEnabled": False}) is False
    assert loader.get_reported_data_enabled({"services": {"staticHosting": {"enabled": True}}}) is None


def test_rejects_conflicting_reported_data_states():
    with pytest.raises(ValueError, match="conflicting"):
        RayfinManifestLoader().get_reported_data_enabled(
            {
                "services": {
                    "data": {
                        "enabled": True,
                    }
                },
                "dataEnabled": False,
            }
        )


def test_registry_requires_matching_deployment(tmp_path):
    registry_path = tmp_path / ".deployments.json"
    registry_path.write_text(json.dumps({"deployments": []}), encoding="utf-8")

    with pytest.raises(ValueError, match="does not contain a deployment"):
        RayfinManifestLoader().load_deployment_record(registry_path, "11111111-1111-1111-1111-111111111111")


def test_registry_rejects_single_record_for_different_workspace(tmp_path):
    registry_path = tmp_path / ".deployments.json"
    registry_path.write_text(
        json.dumps(
            {
                "deployments": [
                    {
                        "workspaceId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                        "fabricItemId": "22222222-2222-2222-2222-222222222222",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="does not contain a deployment"):
        RayfinManifestLoader().load_deployment_record(registry_path, "11111111-1111-1111-1111-111111111111")
