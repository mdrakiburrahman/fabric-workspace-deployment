# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import logging

import dacite
import requests

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    FabricFolder,
    CommonParams,
    HttpRetryHandler,
    ModelManager,
    ModelParams,
    WorkspaceManager,
)
from fabric_workspace_deployment.static.transformers import StringTransformer

class SemanticModelManager(ModelManager):
    """Concrete implementation of ModelManager for Semantic Models."""

    def __init__(
        self,
        common_params: CommonParams,
        az_cli: AzCli,
        workspace: WorkspaceManager,
        http_retry_handler: HttpRetryHandler,
    ):
        """
        Initialize the Fabric Model manager.
        """
        super().__init__(common_params)
        self.az_cli = az_cli
        self.workspace = workspace
        self.logger = logging.getLogger(__name__)
        self.http_retry = http_retry_handler

    async def execute(self) -> None:
        """
        Execute reconciliation for all workspaces in parallel.
        """
        self.logger.info("Executing SemanticModelManager")
        tasks = []
        
        for workspace_params in self.common_params.fabric.workspaces:
            workspace_info = await self.workspace.get(workspace_params)
            
            if workspace_params.model and len(workspace_params.model) > 0:
                for model_params in workspace_params.model:
                    task = asyncio.create_task(
                        self.reconcile(workspace_info.id, model_params), 
                        name=f"reconcile-model-{workspace_params.name}-{model_params.display_name}"
                    )
                    tasks.append(task)
            else:
                self.logger.info(
                    f"No model configuration found for workspace '{workspace_params.name}', skipping model reconciliation"
                )

        if tasks:
            self.logger.info(
                f"Executing model reconciliation for {len(tasks)} models across workspaces in parallel")
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

    async def reconcile(self, workspace_id: str, model_params: ModelParams) -> None:
        """
        Reconcile a single model to desired state.

        Args:
            workspace_id: The Fabric workspace id
            model_params: Parameters for the model to reconcile
        """
        self.logger.info(f"Reconciling model '{model_params.display_name}' in workspace {workspace_id}")
        
        try:
            folder_info = await self.get_fabric_folder_info(workspace_id)
            matching_model = None
            for artifact in folder_info.artifacts:
                if (artifact.type_name == "Model" and 
                    artifact.display_name == model_params.display_name):
                    matching_model = artifact
                    break
            
            if matching_model is None:
                self.logger.warning(
                    f"Model '{model_params.display_name}' not found in workspace {workspace_id}")
                return

            settings_data = f'{{"directLakeAutoSync":{str(model_params.direct_lake_auto_sync).lower()}}}'
            await self.set_model(str(matching_model.id), settings_data)
            
            self.logger.info(
                f"Successfully reconciled model '{model_params.display_name}' "
                f"with: {settings_data}")
            
        except Exception as e:
            error_msg = f"Failed to reconcile model '{model_params.display_name}' in workspace {workspace_id}: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def get_fabric_folder_info(self, workspace_id: str) -> FabricFolder:
        """
        Get Fabric folder information for a workspace.

        Args:
            workspace_id: The Fabric workspace id

        Returns:
            FabricFolder: Fabric workspace folder information
        """
        self.logger.info(
            f"Getting Fabric folder info for workspace {workspace_id}")
        try:
            response = self.http_retry.execute(
                requests.get,
                f"{self.common_params.endpoint.analysis_service}/metadata/relations/folder/{workspace_id}",
                headers={
                    "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                    "Content-Type": "application/json",
                    "X-Consuming-Feature": "ListView",
                },
                timeout=60,
            )
            folder_data = response.json()
            self.logger.debug(
                f"Fabric folder info raw response for {workspace_id}: {folder_data}")
            
            folder_data_snake_case = StringTransformer.convert_keys_to_snake_case(
                folder_data)
            
            fabric_folder = dacite.from_dict(
                data_class=FabricFolder,
                data=folder_data_snake_case,
                config=dacite.Config(
                    check_types=False,
                    cast=[str, int, bool, float],
                ),
            )

            self.logger.info(
                f"Successfully retrieved Fabric folder info for workspace {workspace_id}")
            return fabric_folder

        except Exception as e:
            error_msg = f"Failed to get Fabric folder info for workspace '{workspace_id}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e
    
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