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
