# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import logging

import requests
from azure.core.credentials import TokenCredential
from pathlib import Path
from typing import Any

from fabric_workspace_deployment.client.fabric_rest import response_guid, verify_reconciliation

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    ArtifactType,
    CicdArtifactType,
    CommonParams,
    FabricWorkspaceParams,
    FabricFolderArtifact,
    FolderClient,
    GatewayClient,
    GatewayConnection,
    HttpRetryHandler,
    ModelManager,
    ModelParams,
    ModelConnectionParams,
    ModelBindingState,
    ModelDatasourceBinding,
    SqlEndpointIdentity,
    PrincipalType,
    RlsRoleDelta,
    RlsRoleMembership,
    SemanticModelClient,
    WorkspaceManager,
)

MODEL_PUBLISH_MAX_ATTEMPTS = 10
MODEL_PUBLISH_RETRY_DELAY_SECONDS = 3


class SemanticModelManager(ModelManager):
    """Concrete implementation of ModelManager for Semantic Models."""

    def __init__(
        self,
        common_params: CommonParams,
        az_cli: AzCli,
        workspace: WorkspaceManager,
        folder_client: FolderClient,
        http_retry_handler: HttpRetryHandler,
        token_credential: TokenCredential,
        gateway_client: GatewayClient,
        semantic_model_client: SemanticModelClient,
    ):
        """
        Initialize the Fabric Model manager.
        """
        super().__init__(common_params)
        self.az_cli = az_cli
        self.workspace = workspace
        self.folder_client = folder_client
        self.http_retry = http_retry_handler
        self.token_credential = token_credential
        self.gateway_client = gateway_client
        self.semantic_model_client = semantic_model_client

    async def _execute(self) -> None:
        """
        Execute reconciliation for all workspaces in parallel.
        """
        self.logger.info("Executing SemanticModelManager")
        tasks = []

        for workspace_params in self.common_params.fabric.workspaces:
            if workspace_params.skip_deploy:
                self.logger.info(f"Skipping models for workspace '{workspace_params.name}' due to skipDeploy=true")
                continue
            workspace_info = await self.workspace.get(workspace_params)

            if workspace_params.model and len(workspace_params.model) > 0:
                for model_params in workspace_params.model:
                    task = asyncio.create_task(self.reconcile(workspace_info.id, model_params, workspace_params), name=f"reconcile-model-{workspace_params.name}-{model_params.display_name}")
                    tasks.append(task)
            else:
                self.logger.info(f"No model configuration found for workspace '{workspace_params.name}', skipping model reconciliation")

        if tasks:
            self.logger.info(f"Executing model reconciliation for {len(tasks)} models across workspaces in parallel")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = []

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    task_name = tasks[i].get_name()
                    error_msg = f"Failed to reconcile model for task '{task_name}': {result}"
                    self.logger.error(error_msg)
                    errors.append(error_msg)

            if errors:
                error = f"Failed to reconcile models for some workspaces: {'; '.join(errors)}"
                raise Exception(error)
        else:
            self.logger.info("No models found to reconcile")

        self.logger.info("Finished executing SemanticModelManager")

    async def reconcile(self, workspace_id: str, model_params: ModelParams, workspace_params: FabricWorkspaceParams | None = None) -> None:
        """
        Reconcile a single model to desired state.

        Args:
            workspace_id: The Fabric workspace id
            model_params: Parameters for the model to reconcile
        """
        self.logger.info(f"Reconciling model '{model_params.display_name}' in workspace {workspace_id}")

        try:
            bindings: list[tuple[ModelConnectionParams, GatewayConnection]] = []
            resolved_connections: dict[str, GatewayConnection] = {}
            source_identities: dict[tuple[str, str], SqlEndpointIdentity] = {}
            for connection in model_params.connections:
                declaration = self.common_params.fabric.get_gateway_by_connection_id(connection.connection_id)
                key = declaration.connection_id.casefold()
                if key not in resolved_connections:
                    resolved_connections[key] = await self.gateway_client.get_connection(declaration.connection_id)
                bindings.append((connection, resolved_connections[key]))
                if connection.source_item is not None:
                    source_key = (connection.source_item.type, connection.source_item.name)
                    if source_key not in source_identities:
                        source_identities[source_key] = await self.semantic_model_client.resolve_source_item(workspace_id, connection.source_item)
            desired_security = {role: self._desired_rls_members(names) for role, names in model_params.security.items()}
            folder_info = await self.folder_client.get_fabric_folder_collection(workspace_id)
            matching_model = self._find_model(folder_info.artifacts, model_params.display_name)

            if matching_model is None:
                if workspace_params is None:
                    raise ValueError(f"Model '{model_params.display_name}' is missing and workspace deployment parameters were not provided")
                if model_params.dry_run:
                    self._get_model_source(workspace_params, model_params)
                    self.logger.info("Model preview: would publish missing model; binding/RLS preflight deferred until it exists")
                    return
                await self._publish_missing_model(workspace_id, workspace_params, model_params)
                matching_model = await self._wait_for_model(workspace_id, model_params.display_name)

            binding_updates: dict[str, dict[str, str]] = {}
            untouched_bindings: dict[str, tuple[frozenset[str], frozenset[str]]] = {}
            if bindings:
                binding_state = await self.semantic_model_client.get_bindings(matching_model.id)
                resolved_bindings = []
                for connection, gateway in bindings:
                    moniker = connection.moniker
                    if connection.source_item is not None:
                        source = source_identities[(connection.source_item.type, connection.source_item.name)]
                        matches = [entry.moniker for entry in binding_state.monikers if any(server == source.server and database in source.databases for server, database in entry.sql_identities)]
                        if len(matches) != 1:
                            raise ValueError("Source SqlEndpoint resolves to zero or multiple semantic model datasource monikers")
                        moniker = matches[0]
                    if moniker is None:
                        raise ValueError("Model connection has no resolved datasource moniker")
                    resolved_bindings.append((ModelConnectionParams(moniker, connection.connection_id), gateway))
                bindings = resolved_bindings
                if len({connection.moniker for connection, _ in bindings}) != len(bindings):
                    raise ValueError("Multiple configured sources resolve to the same model datasource moniker")
                binding_updates = self._plan_bindings(binding_state, bindings)
                managed_monikers = {connection.moniker.casefold() for connection, _ in bindings if connection.moniker is not None}
                untouched_bindings = {entry.moniker.casefold(): (entry.gateway_ids, entry.connection_ids) for entry in binding_state.monikers if entry.moniker.casefold() not in managed_monikers}
            rls_deltas: list[RlsRoleDelta] = []
            expected_rls: dict[int, tuple[str, frozenset[str]]] = {}
            if desired_security:
                roles = await self.semantic_model_client.get_rls_membership(matching_model.id)
                rls_deltas = self._plan_rls(roles, desired_security)
                expected_rls = {role.id: (role.name, frozenset(member.object_id.casefold() for member in role.members)) for role in roles}
                for name, members in desired_security.items():
                    role = self._find_role(roles, name)
                    expected_rls[role.id] = (name, frozenset(members))
            self.logger.info("Model plan: settings update, binding groups=%d, RLS role deltas=%d, dryRun=%s", len(binding_updates), len(rls_deltas), model_params.dry_run)
            if model_params.dry_run:
                return
            settings_data = f'{{"directLakeAutoSync":{str(model_params.direct_lake_auto_sync).lower()}}}'
            await self.set_model(str(matching_model.id), settings_data)
            for cluster_id, monikers in binding_updates.items():
                await self.semantic_model_client.bind(matching_model.id, cluster_id, monikers)
            if binding_updates:
                await verify_reconciliation(lambda: self.semantic_model_client.get_bindings(matching_model.id), lambda state: self._bindings_match(state, bindings, untouched_bindings), "Model binding reconciliation")
            if rls_deltas:
                await self.semantic_model_client.update_rls_membership(matching_model.id, rls_deltas)
                await verify_reconciliation(lambda: self.semantic_model_client.get_rls_membership(matching_model.id), lambda state: self._rls_matches(state, expected_rls), "Model RLS reconciliation")

            self.logger.info(f"Successfully reconciled model '{model_params.display_name}' " f"with: {settings_data}")

        except Exception as e:
            error_msg = f"Failed to reconcile model '{model_params.display_name}' in workspace {workspace_id}: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def _find_model(self, artifacts: list[FabricFolderArtifact], display_name: str) -> FabricFolderArtifact | None:
        matches = [artifact for artifact in artifacts if artifact.type_name == ArtifactType.MODEL.value and artifact.display_name == display_name]
        if len(matches) > 1:
            raise ValueError("Semantic model display name is ambiguous within the workspace")
        return matches[0] if matches else None

    def _get_model_source(self, workspace_params: FabricWorkspaceParams, model_params: ModelParams) -> Path:
        artifacts_root = self._get_artifacts_root(workspace_params)
        expected_directory_name = f"{model_params.display_name}.SemanticModel"
        matching_directories = sorted(path for path in artifacts_root.rglob(expected_directory_name) if path.is_dir())

        if len(matching_directories) == 0:
            raise FileNotFoundError(f"Semantic model source directory '{expected_directory_name}' was not found under {artifacts_root}")
        if len(matching_directories) > 1:
            raise ValueError(f"Multiple semantic model source directories named '{expected_directory_name}' were found under {artifacts_root}: {matching_directories}")
        return matching_directories[0]

    async def _publish_missing_model(self, workspace_id: str, workspace_params: FabricWorkspaceParams, model_params: ModelParams) -> None:
        self._get_model_source(workspace_params, model_params)
        artifacts_root = self._get_artifacts_root(workspace_params)

        import fabric_cicd

        fabric_cicd.disable_file_logging()
        fabric_cicd.constants.DEFAULT_API_ROOT_URL = self.common_params.endpoint.cicd
        fc_logger = logging.getLogger("fabric_cicd")
        fc_logger.setLevel(logging.DEBUG)
        fc_logger.propagate = False
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.FileHandler) and handler not in fc_logger.handlers:
                fc_logger.addHandler(handler)
                break

        required_feature_flags = ["enable_experimental_features", "enable_items_to_include"]
        for feature_flag in dict.fromkeys([*workspace_params.template.feature_flags, *required_feature_flags]):
            fabric_cicd.append_feature_flag(feature_flag)

        target_workspace = fabric_cicd.FabricWorkspace(
            workspace_id=workspace_id,
            environment=workspace_params.template.environment_key,
            repository_directory=str(artifacts_root),
            item_type_in_scope=[CicdArtifactType.SEMANTIC_MODEL.value],
            token_credential=self.token_credential,
        )

        item_name = f"{model_params.display_name}.SemanticModel"
        self.logger.info(f"Publishing missing semantic model '{item_name}' to workspace {workspace_id}")
        await asyncio.to_thread(
            fabric_cicd.publish_all_items,
            target_workspace,
            items_to_include=[item_name],
        )

    async def _wait_for_model(self, workspace_id: str, display_name: str) -> FabricFolderArtifact:
        for attempt in range(1, MODEL_PUBLISH_MAX_ATTEMPTS + 1):
            folder_info = await self.folder_client.get_fabric_folder_collection(workspace_id)
            matching_model = self._find_model(folder_info.artifacts, display_name)
            if matching_model is not None:
                self.logger.info(f"Semantic model '{display_name}' became available after publish")
                return matching_model
            if attempt < MODEL_PUBLISH_MAX_ATTEMPTS:
                self.logger.info(f"Waiting for semantic model '{display_name}' to become available ({attempt}/{MODEL_PUBLISH_MAX_ATTEMPTS})")
                await asyncio.sleep(MODEL_PUBLISH_RETRY_DELAY_SECONDS)

        raise RuntimeError(f"Semantic model '{display_name}' was published but did not become available in workspace {workspace_id}")

    def _find_moniker(self, state: ModelBindingState, moniker: str | None) -> ModelDatasourceBinding:
        if moniker is None:
            raise ValueError("Model connection has no resolved datasource moniker")
        matches = [entry for entry in state.monikers if entry.moniker.casefold() == moniker.casefold()]
        if len(matches) != 1:
            raise ValueError("Configured model moniker is missing or ambiguous")
        return matches[0]

    def _plan_bindings(self, state: ModelBindingState, bindings: list[tuple[ModelConnectionParams, GatewayConnection]]) -> dict[str, dict[str, str]]:
        updates: dict[str, dict[str, str]] = {}
        for index, (connection, gateway) in enumerate(bindings):
            current = self._find_moniker(state, connection.moniker)
            desired_id = gateway.id.casefold()
            cluster_id = gateway.cluster_id.casefold()
            candidate_clusters = {entry.gateway_cluster_id.casefold() for entry in state.datasources if entry.connection_id.casefold() == desired_id}
            if desired_id not in current.candidate_ids or candidate_clusters != {cluster_id}:
                raise ValueError(f"Model connections[{index}]: desired connection is not a valid candidate for that moniker and cluster")
            if current.gateway_ids != frozenset({cluster_id}) or current.connection_ids != frozenset({desired_id}):
                updates.setdefault(cluster_id, {})[current.moniker] = gateway.id
                self.logger.info("Model connections[%d]: current gateways=%s, connections=%s -> gateway=%s, connection=%s", index, sorted(current.gateway_ids), sorted(current.connection_ids), cluster_id, desired_id)
        return updates

    def _bindings_match(self, state: ModelBindingState, bindings: list[tuple[ModelConnectionParams, GatewayConnection]], untouched: dict[str, tuple[frozenset[str], frozenset[str]]]) -> bool:
        for connection, gateway in bindings:
            current = self._find_moniker(state, connection.moniker)
            if current.gateway_ids != frozenset({gateway.cluster_id.casefold()}) or current.connection_ids != frozenset({gateway.id.casefold()}):
                return False
        for moniker, expected in untouched.items():
            current = self._find_moniker(state, moniker)
            if (current.gateway_ids, current.connection_ids) != expected:
                return False
        return True

    def _desired_rls_members(self, names: list[str]) -> dict[str, dict[str, Any]]:
        members: dict[str, dict[str, Any]] = {}
        for index, name in enumerate(names):
            identity = self.common_params.get_identity_by_given_name(name)
            object_id = response_guid(identity.object_id, f"RLS desired members[{index}] objectId")
            if object_id.casefold() in members:
                raise ValueError("Configured RLS role contains duplicate desired principal IDs")
            if identity.principal_type not in (PrincipalType.GROUP, PrincipalType.USER):
                raise ValueError("RLS membership supports only Group and User identities")
            is_group = identity.principal_type == PrincipalType.GROUP
            if not is_group and (not identity.user_principal_name or "@" not in identity.user_principal_name):
                raise ValueError("RLS User identities require userPrincipalName")
            members[object_id.casefold()] = {"displayName": identity.given_name, "objectId": object_id, "userPrincipalName": None if is_group else identity.user_principal_name, "isSecurityGroup": is_group, "objectType": 2 if is_group else 1, "groupType": 1 if is_group else 0, "aadAppId": None, "emailAddress": None, "relevanceScore": None, "creatorObjectId": None}
        return members

    def _find_role(self, roles: list[RlsRoleMembership], name: str) -> RlsRoleMembership:
        matches = [role for role in roles if role.name == name]
        if len(matches) != 1:
            raise ValueError("Configured RLS role is missing or ambiguous")
        return matches[0]

    def _plan_rls(self, roles: list[RlsRoleMembership], desired: dict[str, dict[str, dict[str, Any]]]) -> list[RlsRoleDelta]:
        deltas = []
        for index, (name, members) in enumerate(desired.items()):
            role = self._find_role(roles, name)
            current = {member.object_id.casefold(): member.data for member in role.members}
            if len(current) != len(role.members):
                raise ValueError("Current RLS role contains ambiguous principal IDs")
            added = [members[object_id] for object_id in sorted(members.keys() - current.keys())]
            removed = [current[object_id] for object_id in sorted(current.keys() - members.keys())]
            self.logger.info("Model security role[%d]: add=%d, remove=%d", index, len(added), len(removed))
            if added or removed:
                deltas.append(RlsRoleDelta(role.id, role.name, added, removed))
        return deltas

    def _rls_matches(self, roles: list[RlsRoleMembership], expected: dict[int, tuple[str, frozenset[str]]]) -> bool:
        for role_id, (name, members) in expected.items():
            matches = [role for role in roles if role.id == role_id]
            if len(matches) != 1 or matches[0].name != name or frozenset(member.object_id.casefold() for member in matches[0].members) != members:
                return False
        return True

    def _get_artifacts_root(self, workspace_params: FabricWorkspaceParams) -> Path:
        artifacts_root = Path(self.common_params.local.root_folder) / workspace_params.template.artifacts_folder
        if not artifacts_root.is_dir():
            raise FileNotFoundError(f"Workspace artifacts folder does not exist: {artifacts_root}")
        return artifacts_root

    async def set_model(self, id: str, data: str) -> None:
        """
        Set Model properties for a given ID.

        Args:
            id: The id of the Model
            data: JSON string containing the data to update (e.g., '{"directLakeAutoSync":false}')

        Raises:
            RuntimeError: If the API call fails
        """
        self.logger.info(f"Setting model properties for model {id}")
        self.logger.debug(f"Model settings data: {data}")

        try:
            response = self.http_retry.execute(
                requests.post,
                f"{self.common_params.endpoint.analysis_service}/metadata/models/{id}/settings",
                headers={
                    "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                    "Content-Type": "application/json",
                },
                data=data,
                timeout=60,
            )

            self.logger.info(f"Successfully updated model settings for model {id}")

        except Exception as e:
            error_msg = f"Failed to set model properties for model '{id}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e
