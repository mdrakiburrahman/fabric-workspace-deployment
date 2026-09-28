# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import logging

import requests
from azure.core.credentials import TokenCredential
from pathlib import Path

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    ArtifactType,
    CicdArtifactType,
    CommonParams,
    FabricWorkspaceParams,
    FolderClient,
    HttpRetryHandler,
    ModelManager,
    ModelParams,
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
            folder_info = await self.folder_client.get_fabric_folder_collection(workspace_id)
            matching_model = self._find_model(folder_info.artifacts, model_params.display_name)

            if matching_model is None:
                if workspace_params is None:
                    raise ValueError(f"Model '{model_params.display_name}' is missing and workspace deployment parameters were not provided")
                await self._publish_missing_model(workspace_id, workspace_params, model_params)
                matching_model = await self._wait_for_model(workspace_id, model_params.display_name)

            settings_data = f'{{"directLakeAutoSync":{str(model_params.direct_lake_auto_sync).lower()}}}'
            await self.set_model(str(matching_model.id), settings_data)

            self.logger.info(f"Successfully reconciled model '{model_params.display_name}' " f"with: {settings_data}")

        except Exception as e:
            error_msg = f"Failed to reconcile model '{model_params.display_name}' in workspace {workspace_id}: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def _find_model(self, artifacts: list, display_name: str):
        return next((artifact for artifact in artifacts if artifact.type_name == ArtifactType.MODEL.value and artifact.display_name == display_name), None)

    async def _publish_missing_model(self, workspace_id: str, workspace_params: FabricWorkspaceParams, model_params: ModelParams) -> None:
        artifacts_root = self._get_artifacts_root(workspace_params)
        expected_directory_name = f"{model_params.display_name}.SemanticModel"
        matching_directories = sorted(path for path in artifacts_root.rglob(expected_directory_name) if path.is_dir())

        if len(matching_directories) == 0:
            raise FileNotFoundError(f"Semantic model source directory '{expected_directory_name}' was not found under {artifacts_root}")
        if len(matching_directories) > 1:
            raise ValueError(f"Multiple semantic model source directories named '{expected_directory_name}' were found under {artifacts_root}: {matching_directories}")

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

    async def _wait_for_model(self, workspace_id: str, display_name: str):
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
