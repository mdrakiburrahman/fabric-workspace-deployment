# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import json
import re
import shlex

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

RAYFIN_MANIFEST_FILE_NAME = "fabric-workspace-deployment.json"
RAYFIN_DEPLOYMENT_REGISTRY_FILE_NAME = ".deployments.json"
RAYFIN_MANIFEST_SCHEMA_VERSION = "1.0"
RAYFIN_MANIFEST_KIND = "rayfin"

_EXACT_SEMVER_PATTERN = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$")
_APP_ID_PATTERN = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")
_CONNECTION_ALIAS_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")
_GUID_PATTERN = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


@dataclass(frozen=True)
class RayfinApp:
    """Rayfin application identity rendered into ``rayfin.yml``."""

    id: str
    name: str
    version: str


@dataclass(frozen=True)
class RayfinTool:
    """Pinned Rayfin CLI requirement."""

    version: str


@dataclass(frozen=True)
class RayfinConnections:
    """Logical connections required by a Rayfin application."""

    semantic_models: list[str]


@dataclass(frozen=True)
class RayfinBuild:
    """Static application build settings."""

    command: str
    output_path: str
    index_document: str


@dataclass(frozen=True)
class RayfinData:
    """Managed Rayfin data-service settings."""

    enabled: bool
    dialect: str


@dataclass(frozen=True)
class RayfinFunctions:
    """Optional application-authenticated Functions service."""

    enabled: bool = False
    path: str = "rayfin/functions"
    build_command: str = "npm run build"


@dataclass(frozen=True)
class RayfinAppManifest:
    """Validated app-root ``fabric-workspace-deployment.json`` manifest."""

    schema_version: str
    kind: str
    app: RayfinApp
    rayfin: RayfinTool
    connections: RayfinConnections
    build: RayfinBuild
    data: RayfinData
    functions: RayfinFunctions = field(default_factory=RayfinFunctions)


@dataclass(frozen=True)
class ResolvedSemanticModel:
    """A semantic-model connection resolved to Fabric identifiers."""

    workspace_id: str
    item_id: str


@dataclass(frozen=True)
class RayfinDeploymentRecord:
    """Deployment registry record selected for a Fabric workspace."""

    workspace_id: str
    fabric_item_id: str
    data_enabled: bool | None = None


class RayfinManifestLoader:
    """Load and validate Rayfin app manifests and deployment registries."""

    def load(self, app_root: Path) -> RayfinAppManifest:
        """
        Load the app-root deployment manifest.

        Args:
            app_root: Root folder of the Rayfin application.

        Returns:
            RayfinAppManifest: The validated manifest.
        """
        manifest_path = app_root / RAYFIN_MANIFEST_FILE_NAME
        try:
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
        except FileNotFoundError as e:
            raise ValueError(f"Rayfin manifest not found: {manifest_path}") from e
        except json.JSONDecodeError as e:
            raise ValueError(f"Rayfin manifest contains invalid JSON at {manifest_path}: {e}") from e

        if not isinstance(data, dict):
            raise ValueError(f"Rayfin manifest must contain a JSON object: {manifest_path}")

        self._require_keys(data, {"schemaVersion", "kind", "app", "rayfin", "connections", "build"}, {"data", "functions"}, "manifest")
        app_data = self._require_dict(data["app"], "app")
        rayfin_data = self._require_dict(data["rayfin"], "rayfin")
        connections_data = self._require_dict(data["connections"], "connections")
        build_data = self._require_dict(data["build"], "build")
        data_service_data = self._require_dict(data["data"], "data") if "data" in data else None

        self._require_exact_keys(app_data, {"id", "name", "version"}, "app")
        self._require_exact_keys(rayfin_data, {"version"}, "rayfin")
        self._require_exact_keys(connections_data, {"semanticModels"}, "connections")
        self._require_exact_keys(build_data, {"command", "outputPath", "indexDocument"}, "build")
        if data_service_data is not None:
            self._require_exact_keys(data_service_data, {"enabled", "dialect"}, "data")

        schema_version = self._require_string(data["schemaVersion"], "schemaVersion")
        if schema_version != RAYFIN_MANIFEST_SCHEMA_VERSION:
            raise ValueError(f"Unsupported Rayfin manifest schemaVersion {schema_version!r}; expected {RAYFIN_MANIFEST_SCHEMA_VERSION!r}")

        kind = self._require_string(data["kind"], "kind")
        if kind != RAYFIN_MANIFEST_KIND:
            raise ValueError(f"Unsupported Rayfin manifest kind {kind!r}; expected {RAYFIN_MANIFEST_KIND!r}")

        app_id = self._require_string(app_data["id"], "app.id")
        if _APP_ID_PATTERN.fullmatch(app_id) is None:
            raise ValueError("app.id must start with a lowercase letter and contain only lowercase letters, digits, and single hyphens")
        if len(app_id) > 63:
            raise ValueError("app.id cannot exceed 63 characters")

        app_name = self._require_string(app_data["name"], "app.name")
        if "/" in app_name:
            raise ValueError("app.name cannot contain '/'")

        app_version = self._require_exact_semver(app_data["version"], "app.version")
        rayfin_version = self._require_exact_semver(rayfin_data["version"], "rayfin.version")

        semantic_models_data = connections_data["semanticModels"]
        if not isinstance(semantic_models_data, list):
            raise ValueError("connections.semanticModels must be a list of connection aliases")

        semantic_models: list[str] = []
        for index, alias_value in enumerate(semantic_models_data):
            alias = self._require_string(alias_value, f"connections.semanticModels[{index}]")
            if _CONNECTION_ALIAS_PATTERN.fullmatch(alias) is None:
                raise ValueError(f"connections.semanticModels[{index}] contains invalid alias {alias!r}")
            if alias in semantic_models:
                raise ValueError(f"connections.semanticModels contains duplicate alias {alias!r}")
            semantic_models.append(alias)

        command = self._require_string(build_data["command"], "build.command")
        output_path = self._require_relative_path(build_data["outputPath"], "build.outputPath")
        index_document = self._require_relative_path(build_data["indexDocument"], "build.indexDocument")
        data_service = RayfinData(enabled=False, dialect="mssql")
        if data_service_data is not None:
            enabled = data_service_data["enabled"]
            if not isinstance(enabled, bool):
                raise ValueError("data.enabled must be a boolean")
            dialect = self._require_string(data_service_data["dialect"], "data.dialect")
            if dialect != "mssql":
                raise ValueError("data.dialect must be exactly 'mssql' for Fabric deployment")
            data_service = RayfinData(enabled=enabled, dialect=dialect)

        functions = RayfinFunctions()
        if "functions" in data:
            functions_data = self._require_dict(data["functions"], "functions")
            self._require_keys(functions_data, {"enabled"}, {"path", "buildCommand"}, "functions")
            functions_enabled = functions_data["enabled"]
            if not isinstance(functions_enabled, bool):
                raise ValueError("functions.enabled must be a boolean")
            if functions_enabled:
                self._require_exact_keys(functions_data, {"enabled", "path", "buildCommand"}, "functions")
            functions_path = self._require_relative_path(functions_data.get("path", functions.path), "functions.path")
            functions_command = self._require_string(functions_data.get("buildCommand", functions.build_command), "functions.buildCommand")
            functions_root = (app_root / functions_path).resolve()
            try:
                functions_root.relative_to(app_root.resolve())
            except ValueError:
                raise ValueError("functions.path must remain inside the application root, including through symlinks") from None
            functions = RayfinFunctions(enabled=functions_enabled, path=functions_path, build_command=functions_command)

        return RayfinAppManifest(
            schema_version=schema_version,
            kind=kind,
            app=RayfinApp(id=app_id, name=app_name, version=app_version),
            rayfin=RayfinTool(version=rayfin_version),
            connections=RayfinConnections(semantic_models=semantic_models),
            build=RayfinBuild(command=command, output_path=output_path, index_document=index_document),
            data=data_service,
            functions=functions,
        )

    def validate_node_package(self, app_root: Path, manifest: RayfinAppManifest) -> None:
        """Validate the exact local Rayfin CLI pin in package metadata."""
        package_json = self._load_json_object(app_root / "package.json", "package.json")
        package_lock = self._load_json_object(app_root / "package-lock.json", "package-lock.json")

        declared_version = None
        for dependency_group in ("devDependencies", "dependencies"):
            dependencies = package_json.get(dependency_group, {})
            if isinstance(dependencies, dict) and "@microsoft/rayfin-cli" in dependencies:
                declared_version = dependencies["@microsoft/rayfin-cli"]
                break

        if declared_version != manifest.rayfin.version:
            raise ValueError(f"package.json must pin @microsoft/rayfin-cli exactly to {manifest.rayfin.version!r}; found {declared_version!r}")

        locked_version = None
        packages = package_lock.get("packages")
        if isinstance(packages, dict):
            locked_package = packages.get("node_modules/@microsoft/rayfin-cli")
            if isinstance(locked_package, dict):
                locked_version = locked_package.get("version")

        if locked_version is None:
            dependencies = package_lock.get("dependencies")
            if isinstance(dependencies, dict):
                locked_package = dependencies.get("@microsoft/rayfin-cli")
                if isinstance(locked_package, dict):
                    locked_version = locked_package.get("version")

        if locked_version != manifest.rayfin.version:
            raise ValueError(f"package-lock.json must resolve @microsoft/rayfin-cli exactly to {manifest.rayfin.version!r}; found {locked_version!r}")

        if manifest.functions.enabled:
            self._validate_functions_package(app_root, manifest, package_json, package_lock)

    def _validate_functions_package(self, app_root: Path, manifest: RayfinAppManifest, root_package: dict[str, Any], package_lock: dict[str, Any]) -> None:
        """Require a portable Functions project installed by the root npm workspace."""
        app_root = app_root.resolve()
        functions_root = (app_root / manifest.functions.path).resolve()
        try:
            functions_root.relative_to(app_root)
        except ValueError:
            raise ValueError("functions.path must remain inside the application root") from None
        if functions_root == app_root or not functions_root.is_dir():
            raise ValueError("functions.path must identify an existing Functions subdirectory")

        for name in ("package.json", "host.json", "tsconfig.json", "src/function_app.ts"):
            required_path = functions_root / name
            try:
                required_path.resolve().relative_to(functions_root)
            except ValueError:
                raise ValueError(f"Functions file must remain inside the Functions project: {name}") from None
            if not required_path.is_file() or required_path.stat().st_size == 0:
                raise ValueError(f"Functions project requires a non-empty {name}")
        functions_package = self._load_json_object(functions_root / "package.json", "Functions package.json")
        host = self._load_json_object(functions_root / "host.json", "Functions host.json")
        if host.get("version") != "2.0":
            raise ValueError("Functions host.json must declare version '2.0'")
        for group in ("dependencies", "devDependencies", "peerDependencies", "optionalDependencies"):
            dependency_group = functions_package.get(group, {})
            if not isinstance(dependency_group, dict):
                raise ValueError(f"Functions package.json {group} must be an object")
            for dependency, spec in dependency_group.items():
                if not isinstance(spec, str):
                    raise ValueError(f"Functions dependency {dependency!r} must have a string version")
                if spec.startswith("file:"):
                    local_path = self._require_relative_path(spec[5:], f"Functions dependency {dependency!r}")
                    local_target = (functions_root / local_path).resolve()
                    try:
                        local_target.relative_to(functions_root)
                    except ValueError:
                        raise ValueError(f"Functions dependency {dependency!r} must remain inside the Functions project") from None
                    if not local_target.exists():
                        raise ValueError(f"Functions dependency {dependency!r} references a missing local file")

        try:
            build_argv = shlex.split(manifest.functions.build_command)
        except ValueError:
            raise ValueError("functions.buildCommand must be a valid command") from None
        scripts = functions_package.get("scripts", {})
        if not build_argv:
            raise ValueError("functions.buildCommand must be a non-empty command")
        if build_argv[:2] == ["npm", "run"]:
            if len(build_argv) < 3 or not isinstance(scripts, dict) or not isinstance(scripts.get(build_argv[2]), str) or not scripts[build_argv[2]].strip():
                raise ValueError("functions.buildCommand must reference a non-empty Functions package script")

        sdk = "@microsoft/fabric-user-data-functions"
        dependencies = functions_package.get("dependencies")
        if not isinstance(dependencies, dict) or dependencies.get(sdk) != manifest.rayfin.version:
            raise ValueError(f"Functions package.json must pin {sdk} exactly to the declared Rayfin release {manifest.rayfin.version!r}")
        for group in ("devDependencies", "peerDependencies", "optionalDependencies"):
            other_dependencies = functions_package.get(group, {})
            if isinstance(other_dependencies, dict) and sdk in other_dependencies and other_dependencies[sdk] != manifest.rayfin.version:
                raise ValueError(f"Functions package.json contains a conflicting {sdk} pin in {group}")

        workspaces = root_package.get("workspaces")
        if isinstance(workspaces, dict):
            workspaces = workspaces.get("packages")
        if not isinstance(workspaces, list) or not workspaces:
            raise ValueError("Enabled Functions require root npm workspaces so npm ci installs the Functions project")
        workspace_match = False
        for workspace in workspaces:
            if not isinstance(workspace, str) or not workspace.strip() or workspace.startswith("!") or Path(workspace).is_absolute() or ".." in Path(workspace).parts:
                raise ValueError("Root npm workspace patterns must be non-empty app-relative paths without exclusions")
            if any(candidate.resolve() == functions_root for candidate in app_root.glob(workspace)):
                workspace_match = True
        if not workspace_match:
            raise ValueError("Root npm workspaces must include functions.path")

        functions_path = functions_root.relative_to(app_root).as_posix()
        packages = package_lock.get("packages")
        if not isinstance(packages, dict):
            raise ValueError("Functions require an npm workspace package-lock.json with a packages map")
        locked_workspace = packages.get(functions_path)
        if not isinstance(locked_workspace, dict) or not isinstance(locked_workspace.get("dependencies"), dict) or locked_workspace["dependencies"].get(sdk) != manifest.rayfin.version:
            raise ValueError("package-lock.json must include the Functions workspace with its exact SDK pin")
        package_name = self._require_string(functions_package.get("name"), "Functions package.json name")
        workspace_link = packages.get(f"node_modules/{package_name}")
        if not isinstance(workspace_link, dict) or workspace_link.get("link") is not True or workspace_link.get("resolved") != functions_path:
            raise ValueError("package-lock.json must link the Functions workspace from node_modules")
        sdk_package = None
        for parent in (functions_root, *functions_root.parents):
            if parent == app_root.parent:
                break
            prefix = parent.relative_to(app_root).as_posix()
            key = f"{prefix}/node_modules/{sdk}" if prefix != "." else f"node_modules/{sdk}"
            candidate = packages.get(key)
            if candidate is not None:
                sdk_package = candidate
                break
        if not isinstance(sdk_package, dict) or sdk_package.get("version") != manifest.rayfin.version:
            raise ValueError(f"package-lock.json must resolve Functions {sdk} exactly to {manifest.rayfin.version!r}")
        sdk_dependencies = sdk_package.get("dependencies")
        if not isinstance(sdk_dependencies, dict) or sdk_dependencies.get("@microsoft/rayfin-client") != manifest.rayfin.version:
            raise ValueError("The Functions SDK must depend on the same declared Rayfin client release")

    def validate_data_schema(self, app_root: Path, manifest: RayfinAppManifest) -> None:
        """Require a Rayfin entity schema whenever the managed data service is enabled."""
        if not manifest.data.enabled:
            return
        schema_path = app_root / "rayfin" / "data" / "schema.ts"
        if not schema_path.is_file() or schema_path.stat().st_size == 0:
            raise ValueError(f"Managed Rayfin data is enabled, but the required schema file is missing or empty: {schema_path}")

    def load_deployment_record(self, registry_path: Path, workspace_id: str) -> RayfinDeploymentRecord:
        """
        Select a deployment registry record for a workspace.

        The Rayfin registry is owned by the Rayfin CLI. The loader intentionally
        accepts both list- and map-backed registries while requiring the canonical
        ``workspaceId`` and ``fabricItemId`` values on the selected record.
        """
        try:
            data = json.loads(registry_path.read_text(encoding="utf-8"))
        except FileNotFoundError as e:
            raise ValueError(f"Rayfin deployment registry not found after deployment: {registry_path}") from e
        except json.JSONDecodeError as e:
            raise ValueError(f"Rayfin deployment registry contains invalid JSON at {registry_path}: {e}") from e

        matching_records: list[tuple[RayfinDeploymentRecord, dict[str, Any]]] = []
        fallback_records: list[tuple[RayfinDeploymentRecord, dict[str, Any]]] = []
        for candidate, inferred_workspace_id in self._walk_objects(data):
            item_id = candidate.get("fabricItemId")
            if not isinstance(item_id, str) or _GUID_PATTERN.fullmatch(item_id.strip()) is None:
                continue

            candidate_workspace_id = candidate.get("workspaceId", inferred_workspace_id)
            if isinstance(candidate_workspace_id, str) and _GUID_PATTERN.fullmatch(candidate_workspace_id.strip()):
                record = RayfinDeploymentRecord(workspace_id=candidate_workspace_id.strip(), fabric_item_id=item_id.strip())
                if candidate_workspace_id.strip().lower() == workspace_id.lower():
                    matching_records.append((record, candidate))
            elif candidate_workspace_id is None:
                fallback_records.append((RayfinDeploymentRecord(workspace_id=workspace_id, fabric_item_id=item_id.strip()), candidate))

        selected_records = matching_records
        if not selected_records and len(fallback_records) == 1:
            selected_records = fallback_records

        unique_records = {(record.workspace_id.lower(), record.fabric_item_id.lower()): (record, candidate) for record, candidate in selected_records}
        if len(unique_records) == 0:
            raise ValueError(f"Rayfin deployment registry {registry_path} does not contain a deployment for workspace {workspace_id}")
        if len(unique_records) > 1:
            raise ValueError(f"Rayfin deployment registry {registry_path} contains multiple deployments for workspace {workspace_id}")
        record, candidate = next(iter(unique_records.values()))
        return RayfinDeploymentRecord(
            workspace_id=record.workspace_id,
            fabric_item_id=record.fabric_item_id,
            data_enabled=self.get_reported_data_enabled(candidate),
        )

    def get_reported_data_enabled(self, value: Any) -> bool | None:
        """Read a reported data-service enabled flag from registry or status JSON."""
        observations: list[bool] = []

        def collect(candidate: Any) -> None:
            if isinstance(candidate, dict):
                services = candidate.get("services")
                if isinstance(services, dict):
                    data_service = services.get("data")
                    if isinstance(data_service, dict) and isinstance(data_service.get("enabled"), bool):
                        observations.append(data_service["enabled"])

                data_service = candidate.get("data")
                if isinstance(data_service, dict) and isinstance(data_service.get("enabled"), bool):
                    observations.append(data_service["enabled"])

                for key in ("dataEnabled", "dataServiceEnabled"):
                    if isinstance(candidate.get(key), bool):
                        observations.append(candidate[key])

                service_name = candidate.get("service", candidate.get("name", candidate.get("type")))
                if isinstance(service_name, str) and service_name.replace(" ", "").lower() in {"data", "dataservice"} and isinstance(candidate.get("enabled"), bool):
                    observations.append(candidate["enabled"])

                for child in candidate.values():
                    collect(child)
            elif isinstance(candidate, list):
                for child in candidate:
                    collect(child)

        collect(value)
        unique_observations = set(observations)
        if len(unique_observations) > 1:
            raise ValueError("Rayfin deployment output reports conflicting managed data enabled states")
        if not unique_observations:
            return None
        return next(iter(unique_observations))

    def _walk_objects(self, value: Any, inferred_workspace_id: str | None = None):
        if isinstance(value, dict):
            yield value, inferred_workspace_id
            for key, child in value.items():
                child_workspace_id = key if isinstance(key, str) and _GUID_PATTERN.fullmatch(key) else inferred_workspace_id
                yield from self._walk_objects(child, child_workspace_id)
        elif isinstance(value, list):
            for child in value:
                yield from self._walk_objects(child, inferred_workspace_id)

    def _require_dict(self, value: Any, path: str) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise ValueError(f"{path} must be a JSON object")
        return value

    def _load_json_object(self, path: Path, description: str) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as e:
            raise ValueError(f"{description} not found: {path}") from e
        except json.JSONDecodeError as e:
            raise ValueError(f"{description} contains invalid JSON at {path}: {e}") from e
        if not isinstance(value, dict):
            raise ValueError(f"{description} must contain a JSON object: {path}")
        return value

    def _require_string(self, value: Any, path: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{path} must be a non-empty string")
        return value.strip()

    def _require_exact_semver(self, value: Any, path: str) -> str:
        if not isinstance(value, str):
            raise ValueError(f"{path} must be an exact semantic version such as '1.35.1'; ranges and tags are not allowed")
        version = value.strip()
        if _EXACT_SEMVER_PATTERN.fullmatch(version) is None:
            raise ValueError(f"{path} must be an exact semantic version such as '1.35.1'; ranges and tags are not allowed")
        return version

    def _require_relative_path(self, value: Any, path: str) -> str:
        relative_path = self._require_string(value, path)
        parsed_path = Path(relative_path)
        if parsed_path.is_absolute() or ".." in parsed_path.parts:
            raise ValueError(f"{path} must be a relative path contained by the app root")
        return relative_path

    def _require_exact_keys(self, value: dict[str, Any], expected: set[str], path: str) -> None:
        self._require_keys(value, expected, set(), path)

    def _require_keys(self, value: dict[str, Any], required: set[str], optional: set[str], path: str) -> None:
        actual = set(value)
        missing = sorted(required - actual)
        unexpected = sorted(actual - required - optional)
        if missing or unexpected:
            details = []
            if missing:
                details.append(f"missing {missing}")
            if unexpected:
                details.append(f"unexpected {unexpected}")
            raise ValueError(f"{path} has invalid fields: {', '.join(details)}")


class RayfinManifestRenderer:
    """Render generated Rayfin and Fabric configuration files."""

    def render_fabric_yaml(self, semantic_models: dict[str, ResolvedSemanticModel]) -> str:
        """Render app-root ``fabric.yaml`` with a single deployment profile."""
        data = {
            "activeProfile": "deployment",
            "profiles": {
                "deployment": {
                    "semanticModels": {
                        alias: {
                            "workspaceId": model.workspace_id,
                            "itemId": model.item_id,
                        }
                        for alias, model in semantic_models.items()
                    }
                }
            },
        }
        return yaml.safe_dump(data, sort_keys=False, allow_unicode=True)

    def render_rayfin_yaml(self, manifest: RayfinAppManifest) -> str:
        """Render ``rayfin/rayfin.yml`` for static-hosting deployment."""
        data: dict[str, Any] = {
            "id": manifest.app.id,
            "name": manifest.app.name,
            "version": manifest.app.version,
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
                    "enabled": manifest.data.enabled,
                    "dialect": manifest.data.dialect,
                },
                "staticHosting": {
                    "enabled": True,
                    "folder": manifest.build.output_path,
                    "buildCommand": manifest.build.command,
                    "indexDocument": manifest.build.index_document,
                },
            },
        }
        if manifest.functions.enabled:
            data["services"]["functions"] = {
                "enabled": True,
                "auth": {"type": "application"},
                "path": manifest.functions.path,
                "buildCommand": manifest.functions.build_command,
            }
        return yaml.safe_dump(data, sort_keys=False, allow_unicode=True)

    def write_generated_files(self, staging_root: Path, manifest: RayfinAppManifest, semantic_models: dict[str, ResolvedSemanticModel]) -> None:
        """Write generated configuration files into the isolated staging tree."""
        staging_root.mkdir(parents=True, exist_ok=True)
        rayfin_root = staging_root / "rayfin"
        rayfin_root.mkdir(parents=True, exist_ok=True)

        fabric_yaml_path = staging_root / "fabric.yaml"
        rayfin_yaml_path = rayfin_root / "rayfin.yml"
        fabric_yaml_path.write_text(self.render_fabric_yaml(semantic_models), encoding="utf-8")
        rayfin_yaml_path.write_text(self.render_rayfin_yaml(manifest), encoding="utf-8")
        fabric_yaml_path.chmod(0o600)
        rayfin_yaml_path.chmod(0o600)
