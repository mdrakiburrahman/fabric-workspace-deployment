# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import json
import logging
import os
import re
import shutil
import uuid

from importlib import resources as importlib_resources
from pathlib import Path
from typing import Any

from fabric_workspace_deployment import resources as package_resources
from fabric_workspace_deployment.environment_variables import RAYFIN_APP_ROOT_ENV_VAR, RAYFIN_GID_ENV_VAR, RAYFIN_TENANT_ID_ENV_VAR, RAYFIN_TOKEN_ENV_VAR, RAYFIN_UID_ENV_VAR, RAYFIN_WORKSPACE_ID_ENV_VAR
from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.docker.cli import DockerCli
from fabric_workspace_deployment.manager.fabric.cli import FabricCli
from fabric_workspace_deployment.operations.operation_interfaces import CommonParams, FabricWorkspaceParams, RayfinManager, RayfinParams
from fabric_workspace_deployment.rayfin.manifest import RAYFIN_DEPLOYMENT_REGISTRY_FILE_NAME, RayfinAppManifest, RayfinDeploymentRecord, RayfinManifestLoader, RayfinManifestRenderer, ResolvedSemanticModel

NPM_INSTALL_TIMEOUT_SECONDS = 15 * 60
RAYFIN_DEPLOY_TIMEOUT_SECONDS = 45 * 60
RAYFIN_STATUS_TIMEOUT_SECONDS = 5 * 60
RAYFIN_COMPOSE_RESOURCE_NAME = "Compose.rayfin.yaml"
RAYFIN_COMPOSE_SERVICE_NAME = "rayfin"

_EXACT_SEMVER_SEARCH_PATTERN = re.compile(r"(?<![0-9A-Za-z])((?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?)(?![0-9A-Za-z])")
_GUID_PATTERN = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_FAILED_STATUS_VALUES = frozenset({"error", "failed", "failure", "unhealthy", "notready", "not_ready"})


class RayfinDeploymentManager(RayfinManager):
    """Deploy Rayfin applications through an isolated Node.js Compose service."""

    def __init__(
        self,
        common_params: CommonParams,
        workspace_params: list[FabricWorkspaceParams],
        az_cli: AzCli,
        fabric_cli: FabricCli,
        docker_cli: DockerCli,
        *,
        manifest_loader: RayfinManifestLoader | None = None,
        manifest_renderer: RayfinManifestRenderer | None = None,
        staging_root: Path | None = None,
        state_root: Path | None = None,
        logger: logging.Logger | None = None,
    ):
        super().__init__(common_params, logger=logger)
        self.workspace_params = workspace_params
        self.az_cli = az_cli
        self.fabric_cli = fabric_cli
        self.docker_cli = docker_cli
        self.manifest_loader = manifest_loader or RayfinManifestLoader()
        self.manifest_renderer = manifest_renderer or RayfinManifestRenderer()
        del state_root
        configured_root = Path(common_params.local.root_folder).expanduser()
        if not configured_root.is_absolute():
            configured_root = Path.cwd() / configured_root
        self.staging_root = staging_root or configured_root / ".fabric-workspace-deployment" / "rayfin-staging"

    async def _execute(self) -> None:
        configured_workspaces = [(workspace_index, workspace) for workspace_index, workspace in enumerate(self.workspace_params) if workspace.rayfins]
        if not configured_workspaces:
            self.logger.info("No common.fabric.workspaces[].rayfins configuration found; deployRayfin is a no-op.")
            return

        for workspace_index, workspace in configured_workspaces:
            workspace_path = f"common.fabric.workspaces[{workspace_index}]"
            if workspace.skip_deploy:
                self.logger.info(f"Skipping {len(workspace.rayfins)} Rayfin app(s) for workspace '{workspace.name}' at {workspace_path} because skipDeploy=true")
                continue
            for rayfin_index, params in enumerate(workspace.rayfins):
                self._deploy(workspace, params, workspace_index, rayfin_index)

    def _deploy(self, workspace: FabricWorkspaceParams, params: RayfinParams, workspace_index: int, rayfin_index: int) -> None:
        config_path = f"common.fabric.workspaces[{workspace_index}].rayfins[{rayfin_index}]"
        source_root = (Path(self.common_params.local.root_folder).resolve() / params.root_path).resolve()
        manifest = self.manifest_loader.load(source_root)
        self.manifest_loader.validate_node_package(source_root, manifest)
        self.manifest_loader.validate_data_schema(source_root, manifest)
        workspace_id = self._resolve_workspace_id(workspace.name, config_path)
        semantic_models = self._resolve_semantic_models(params, workspace.name, workspace_id, config_path)
        token = self._get_rayfin_token()

        staging_path: Path | None = None
        try:
            staging_path = self._create_staging_tree(source_root, manifest)
            self.manifest_renderer.write_generated_files(staging_path, manifest, semantic_models)
            compose_path = self._write_compose_resource(staging_path)
            docker_env = self._build_docker_environment(staging_path, workspace_id, token)
            project_name = self._build_compose_project_name(manifest)

            self.logger.info(f"Installing Rayfin app dependencies for '{manifest.app.name}' configured at {config_path} with npm ci")
            self.docker_cli.compose_run(
                compose_path,
                project_name,
                RAYFIN_COMPOSE_SERVICE_NAME,
                ["npm", "ci"],
                timeout=NPM_INSTALL_TIMEOUT_SECONDS,
                env=docker_env,
            )

            self._assert_local_rayfin_version(compose_path, project_name, docker_env, manifest)

            self.logger.info(f"Deploying Rayfin app '{manifest.app.name}' from {config_path} to parent workspace '{workspace.name}'")
            self.docker_cli.compose_run(
                compose_path,
                project_name,
                RAYFIN_COMPOSE_SERVICE_NAME,
                ["./node_modules/.bin/rayfin", "up", "--yes"],
                timeout=RAYFIN_DEPLOY_TIMEOUT_SECONDS,
                env=docker_env,
            )

            registry_path = staging_path / "rayfin" / RAYFIN_DEPLOYMENT_REGISTRY_FILE_NAME
            deployment_record = self.manifest_loader.load_deployment_record(registry_path, workspace_id)
            self._assert_reported_data_state("Rayfin deployment registry", deployment_record.data_enabled, manifest.data.enabled)
            self._assert_fabric_item(workspace_id, deployment_record)
            self._assert_rayfin_status(compose_path, project_name, docker_env, manifest)

            shutil.rmtree(staging_path)
            self.logger.info(f"Successfully deployed Rayfin app '{manifest.app.name}' and removed staging directory {staging_path}")
        except Exception:
            if staging_path is not None:
                self.logger.error(f"Rayfin deployment failed for {config_path}. Staging directory retained for diagnostics: {staging_path}")
            raise

    def _resolve_workspace_id(self, workspace_name: str, config_path: str) -> str:
        resource_path = f"{workspace_name}.Workspace"
        try:
            stdout, _ = self.fabric_cli.run(["get", resource_path, "-q", "id"], timeout=60)
            return self._parse_guid(stdout, f"workspace '{workspace_name}'")
        except Exception as e:
            raise ValueError(f"Unable to resolve Fabric workspace '{workspace_name}' for {config_path}. Verify the display name and the caller's workspace access.") from e

    def _resolve_semantic_models(self, params: RayfinParams, parent_workspace_name: str, parent_workspace_id: str, config_path: str) -> dict[str, ResolvedSemanticModel]:
        resolved: dict[str, ResolvedSemanticModel] = {}
        resolved_workspace_ids = {parent_workspace_name.casefold(): parent_workspace_id}
        for alias, semantic_model in params.semantic_models.items():
            binding_path = f"{config_path}.semanticModels[{alias!r}]"
            model_workspace_name = semantic_model.workspace_name or parent_workspace_name
            try:
                model_workspace_id = resolved_workspace_ids.get(model_workspace_name.casefold())
                if model_workspace_id is None:
                    model_workspace_id = self._resolve_workspace_id(model_workspace_name, binding_path)
                    resolved_workspace_ids[model_workspace_name.casefold()] = model_workspace_id
            except Exception as e:
                raise ValueError(f"Unable to resolve semantic-model workspace '{model_workspace_name}' for alias '{alias}' at {binding_path}. Verify the friendly workspace name and access.") from e

            resource_path = f"{model_workspace_name}.Workspace/{semantic_model.item_name}.SemanticModel"
            try:
                stdout, _ = self.fabric_cli.run(["get", resource_path, "-q", "id"], timeout=60)
                model_id = self._parse_guid(stdout, f"semantic model '{semantic_model.item_name}' in workspace '{model_workspace_name}'")
            except Exception as e:
                raise ValueError(f"Unable to resolve semantic model '{semantic_model.item_name}' for alias '{alias}' at {binding_path} in workspace '{model_workspace_name}'. Verify the friendly item name and access.") from e
            resolved[alias] = ResolvedSemanticModel(workspace_id=model_workspace_id, item_id=model_id)
        return resolved

    def _get_rayfin_token(self) -> str:
        configured_token = os.getenv(RAYFIN_TOKEN_ENV_VAR, "").strip()
        if configured_token:
            self.logger.debug(f"Using Rayfin token from environment variable '{RAYFIN_TOKEN_ENV_VAR}'")
            return configured_token
        self.logger.debug(f"Environment variable '{RAYFIN_TOKEN_ENV_VAR}' is not set; acquiring a Fabric token through Azure CLI")
        return self.az_cli.get_access_token(self.common_params.scope.analysis_service)

    def _create_staging_tree(self, source_root: Path, manifest: RayfinAppManifest) -> Path:
        self._remove_stale_staging()
        self.staging_root.mkdir(parents=True, exist_ok=True)
        self.staging_root.chmod(0o700)
        staging_path = self.staging_root / f"{manifest.app.id}-{uuid.uuid4().hex}"
        ignore = shutil.ignore_patterns(".git", ".fabric-workspace-deployment", "node_modules", ".home", ".temp", ".env", RAYFIN_DEPLOYMENT_REGISTRY_FILE_NAME, "fabric.yaml", "rayfin.yml")
        shutil.copytree(source_root, staging_path, ignore=ignore)
        staging_path.chmod(0o700)

        output_path = staging_path / manifest.build.output_path
        if output_path.exists():
            if output_path.is_dir():
                shutil.rmtree(output_path)
            else:
                output_path.unlink()

        return staging_path

    def _remove_stale_staging(self) -> None:
        if not self.staging_root.exists():
            return

        removed = 0
        for path in self.staging_root.iterdir():
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
            removed += 1

        if removed:
            self.logger.info(f"Removed {removed} stale Rayfin staging entr{'y' if removed == 1 else 'ies'} from {self.staging_root}")

    def _write_compose_resource(self, staging_path: Path) -> Path:
        compose_text = importlib_resources.files(package_resources).joinpath(RAYFIN_COMPOSE_RESOURCE_NAME).read_text(encoding="utf-8")
        compose_path = staging_path / RAYFIN_COMPOSE_RESOURCE_NAME
        compose_path.write_text(compose_text, encoding="utf-8")
        compose_path.chmod(0o600)
        return compose_path

    def _build_docker_environment(self, staging_path: Path, workspace_id: str, token: str) -> dict[str, str]:
        return {
            RAYFIN_APP_ROOT_ENV_VAR: str(staging_path),
            RAYFIN_TOKEN_ENV_VAR: token,
            RAYFIN_WORKSPACE_ID_ENV_VAR: workspace_id,
            RAYFIN_TENANT_ID_ENV_VAR: self.common_params.arm.tenant_id,
            RAYFIN_UID_ENV_VAR: str(getattr(os, "getuid", lambda: 1000)()),
            RAYFIN_GID_ENV_VAR: str(getattr(os, "getgid", lambda: 1000)()),
        }

    def _build_compose_project_name(self, manifest: RayfinAppManifest) -> str:
        return f"fwd-rayfin-{manifest.app.id}-{uuid.uuid4().hex[:8]}"

    def _assert_local_rayfin_version(self, compose_path: Path, project_name: str, docker_env: dict[str, str], manifest: RayfinAppManifest) -> None:
        stdout, _ = self.docker_cli.compose_run(
            compose_path,
            project_name,
            RAYFIN_COMPOSE_SERVICE_NAME,
            ["./node_modules/.bin/rayfin", "--version"],
            timeout=60,
            env=docker_env,
        )
        match = _EXACT_SEMVER_SEARCH_PATTERN.search(stdout)
        if match is None:
            raise RuntimeError("The local Rayfin CLI did not report a semantic version after npm ci")
        installed_version = match.group(1)
        if installed_version != manifest.rayfin.version:
            raise RuntimeError(f"Rayfin CLI version mismatch: app manifest requires {manifest.rayfin.version}, but npm ci installed {installed_version}. Pin @microsoft/rayfin-cli exactly in package.json and package-lock.json.")

    def _assert_rayfin_status(self, compose_path: Path, project_name: str, docker_env: dict[str, str], manifest: RayfinAppManifest) -> None:
        stdout, _ = self.docker_cli.compose_run(
            compose_path,
            project_name,
            RAYFIN_COMPOSE_SERVICE_NAME,
            ["./node_modules/.bin/rayfin", "up", "status", "--json"],
            timeout=RAYFIN_STATUS_TIMEOUT_SECONDS,
            env=docker_env,
        )
        if not stdout.strip():
            raise RuntimeError("rayfin up status returned no output")

        try:
            status_data = json.loads(stdout)
        except json.JSONDecodeError:
            self.logger.warning("rayfin up status succeeded but did not return JSON; relying on the command exit code")
            return

        failed_value = self._find_failed_status(status_data)
        if failed_value is not None:
            raise RuntimeError(f"rayfin up status reported an unhealthy deployment state: {failed_value}")
        self._assert_reported_data_state("rayfin up status", self.manifest_loader.get_reported_data_enabled(status_data), manifest.data.enabled)

    def _assert_reported_data_state(self, source: str, reported_enabled: bool | None, expected_enabled: bool) -> None:
        if reported_enabled is not None and reported_enabled is not expected_enabled:
            raise RuntimeError(f"{source} reports managed data enabled={reported_enabled}, but the app manifest requires enabled={expected_enabled}")

    def _find_failed_status(self, value: Any) -> str | None:
        if isinstance(value, dict):
            for key, child in value.items():
                normalized_key = key.lower()
                if normalized_key in {"healthy", "success", "ready"} and child is False:
                    return f"{key}=false"
                if normalized_key in {"status", "state", "health"} and isinstance(child, str) and child.strip().lower() in _FAILED_STATUS_VALUES:
                    return f"{key}={child}"
                nested_failure = self._find_failed_status(child)
                if nested_failure is not None:
                    return nested_failure
        elif isinstance(value, list):
            for child in value:
                nested_failure = self._find_failed_status(child)
                if nested_failure is not None:
                    return nested_failure
        return None

    def _assert_fabric_item(self, workspace_id: str, deployment_record: RayfinDeploymentRecord) -> None:
        endpoint = f"workspaces/{workspace_id}/items/{deployment_record.fabric_item_id}"
        try:
            stdout, _ = self.fabric_cli.run(["api", endpoint, "-X", "get"], timeout=60)
            item_data = json.loads(stdout.strip())
        except Exception as e:
            raise RuntimeError(f"Rayfin deployment registry recorded Fabric item {deployment_record.fabric_item_id}, but Fabric could not resolve that item ID in workspace {workspace_id}.") from e

        if not isinstance(item_data, dict):
            raise RuntimeError(f"Fabric item lookup for {deployment_record.fabric_item_id} in workspace {workspace_id} returned an invalid response")

        wrapped_item_data = item_data.get("text")
        if isinstance(wrapped_item_data, dict):
            item_data = wrapped_item_data

        actual_item_id = item_data.get("id")
        actual_workspace_id = item_data.get("workspaceId")
        if not isinstance(actual_item_id, str) or actual_item_id.lower() != deployment_record.fabric_item_id.lower():
            raise RuntimeError(f"Rayfin Fabric item assertion failed: registry item ID {deployment_record.fabric_item_id} does not match Fabric item ID {actual_item_id!r}")
        if not isinstance(actual_workspace_id, str) or actual_workspace_id.lower() != workspace_id.lower():
            raise RuntimeError(f"Rayfin Fabric item assertion failed: item {deployment_record.fabric_item_id} belongs to workspace {actual_workspace_id!r}, expected {workspace_id}")

    def _parse_guid(self, output: str, description: str) -> str:
        value = output.strip().lstrip("* ").strip()
        try:
            decoded = json.loads(value)
            if isinstance(decoded, str):
                value = decoded.strip()
        except json.JSONDecodeError:
            pass
        if _GUID_PATTERN.fullmatch(value) is None:
            raise ValueError(f"Fabric CLI returned an invalid ID for {description}: {value!r}")
        return value
