# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import json
import logging

import dacite
import requests

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.fabric.cli import FabricCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    AnalysisServiceCapacity,
    CommonParams,
    FabricStorageParams,
    FabricWorkspaceInfo,
    FabricWorkspaceParams,
    WorkspaceManager,
)
from fabric_workspace_deployment.static.transformers import StringTransformer


class FabricWorkspaceManager(WorkspaceManager):
    """Concrete implementation of WorkspaceManager for Microsoft Fabric."""

    def __init__(self, common_params: CommonParams, az_cli: AzCli, fabric_cli: FabricCli):
        """
        Initialize the Fabric workspace manager.
        """
        super().__init__(common_params)
        self.az_cli = az_cli
        self.fabric_cli = fabric_cli
        self.logger = logging.getLogger(__name__)

    async def execute(self) -> None:
        self.logger.info("Executing FabricWorkspaceManager")
        tasks = []
        for workspace in self.common_params.fabric.workspaces:
            task = asyncio.create_task(self.reconcile(
                workspace), name=f"reconcile-workspace-{workspace.name}")
            tasks.append(task)

        if tasks:
            self.logger.info(
                f"Executing workspace reconciliation for {len(tasks)} workspaces in parallel")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    workspace_name = self.common_params.fabric.workspaces[i].name
                    error_msg = f"Failed to reconcile workspace '{workspace_name}': {result}"
                    self.logger.error(error_msg)
                    errors.append(error_msg)

            if errors:
                error = f"Failed to reconcile some workspaces: {'; '.join(errors)}"
                raise Exception(error)
        else:
            self.logger.info("No workspaces found to reconcile")

        self.logger.info("Finished executing FabricWorkspaceManager")

    async def reconcile(self, workspace_params: FabricWorkspaceParams) -> None:
        self.logger.info(f"Reconciling workspace: {workspace_params.name}")
        workspace_exists = await self.exists(workspace_params)

        if not workspace_exists:
            self.logger.info(
                f"Workspace '{workspace_params.name}' does not exist. Creating.")
            await self.create(workspace_params)
            self.logger.info(
                f"Successfully created workspace '{workspace_params.name}'")
        else:
            self.logger.info(
                f"Workspace '{workspace_params.name}' already exists")

        workspace_info = await self.get(workspace_params)
        as_capacity = await self.get_analysis_service_capacity(workspace_info.id)

        if workspace_info.display_name != workspace_params.name:
            self.logger.info(
                f"Workspace '{workspace_params.name}' display name mismatch. Current: {workspace_info.display_name}, Desired: {workspace_params.name}. Updating."  # fmt: skip # noqa: E501
            )
            await self.set(workspace_params, "displayName", workspace_params.name)
            self.logger.info(
                f"Successfully updated display name for workspace '{workspace_params.name}'")
        else:
            self.logger.info(
                f"Workspace '{workspace_params.name}' display name is already correct")

        if workspace_info.description != workspace_params.description:
            self.logger.info(
                f"Workspace '{workspace_params.name}' description mismatch. Current: {workspace_info.description}, Desired: {workspace_params.description}. Updating."  # fmt: skip # noqa: E501
            )
            await self.set(workspace_params, "description", workspace_params.description)
            self.logger.info(
                f"Successfully updated description for workspace '{workspace_params.name}'")
        else:
            self.logger.info(
                f"Workspace '{workspace_params.name}' description is already correct")

        if as_capacity.default_dataset_storage_mode != workspace_params.dataset_storage_mode:
            self.logger.info(
                f"Workspace '{workspace_params.name}' datasetStorageMode mismatch. Current: {as_capacity.default_dataset_storage_mode}, Desired: {workspace_params.dataset_storage_mode}. Updating."  # fmt: skip # noqa: E501
            )
            await self.set_analysis_service_capacity(
                workspace_info.id, f'{{"datasetStorageMode":{workspace_params.dataset_storage_mode}}}'
            )
            self.logger.info(
                f"Successfully updated datasetStorageMode for workspace '{workspace_params.name}'")

        if workspace_info.workspace_identity is None:
            self.logger.info(
                f"Workspace '{workspace_params.name}' has no managed identity. Creating.")
            await self.create_managed_identity(workspace_params)
            self.logger.info(
                f"Successfully created managed identity for workspace '{workspace_params.name}'")
        else:
            self.logger.info(
                f"Workspace '{workspace_params.name}' already has a managed identity '{workspace_info.workspace_identity}'"
            )

        await self.assign_workspace_storage_reader(await self.get(workspace_params), self.common_params.fabric.storage)

        self.logger.info(
            f"Completed reconciliation for workspace: {workspace_params.name}")

    async def create(self, workspace_params: FabricWorkspaceParams) -> None:
        """
        Create a new workspace.

        Args:
            workspace_params: Parameters for the fabric workspace
        """
        self.logger.info(f"Creating workspace: {workspace_params.name}")
        self.fabric_cli.run_command(
            f"create {workspace_params.name}.workspace -P capacityname={workspace_params.capacity.name}")

    async def set(self, workspace_params: FabricWorkspaceParams, property_path: str, value: str) -> None:
        """
        Set a property on the workspace.

        Args:
            workspace_params: Parameters for the fabric workspace
            property_path: The property path to set (e.g., "displayName", "description")
            value: The value to set
        """
        available_properties = ["displayName", "description"]
        if property_path not in available_properties:
            error = f"Invalid property '{property_path}'. Available properties: {', '.join(available_properties)}"
            raise ValueError(error)

        self.fabric_cli.run_command(
            f"set '{workspace_params.name}.Workspace' -q {property_path} -i '{value}' -f")

    async def exists(self, workspace_params: FabricWorkspaceParams) -> bool:
        """
        Check if the workspace exists.

        Args:
            workspace_params: Parameters for the fabric workspace

        Returns:
            bool: True if the workspace exists, False otherwise
        """
        self.logger.info(
            f"Checking if workspace exists: {workspace_params.name}")
        output = self.fabric_cli.run_command(
            f"exists '{workspace_params.name}.Workspace'")
        exists = output.strip().lstrip("* ").strip().lower() == "true"
        self.logger.info(
            f"Workspace '{workspace_params.name}' exists: {exists}")
        return exists

    async def get(self, workspace_params: FabricWorkspaceParams) -> FabricWorkspaceInfo:
        """
        Get workspace details.

        Args:
            workspace_params: Parameters for the fabric workspace

        Returns:
            FabricWorkspaceInfo: Workspace information
        """
        self.logger.info(f"Getting workspace details: {workspace_params.name}")
        output = self.fabric_cli.run_command(
            f"get '{workspace_params.name}.Workspace' -q .")
        workspace_data = json.loads(output.strip())
        workspace_data_snake_case = StringTransformer.convert_keys_to_snake_case(
            workspace_data)

        try:
            workspace_info = dacite.from_dict(
                data_class=FabricWorkspaceInfo,
                data=workspace_data_snake_case,
                config=dacite.Config(
                    check_types=False,
                    cast=[str, int, bool, float],
                ),
            )
            return workspace_info
        except Exception as e:
            self.logger.error(
                f"Failed to parse workspace data with dacite: {e}")
            self.logger.error(f"Raw workspace data: {workspace_data}")
            self.logger.error(
                f"Converted workspace data: {workspace_data_snake_case}")
            raise

    async def get_analysis_service_capacity(self, object_id: str) -> AnalysisServiceCapacity:
        self.logger.info(
            f"Getting Analysis Service capacity for object ID: {object_id}")
        response = requests.get(
            f"{self.common_params.endpoint.analysis_service}/metadata/folders/{object_id}",
            headers={
                "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                "Content-Type": "application/json",
            },
            timeout=60,
        )
        response.raise_for_status()
        capacity_data = response.json()
        capacity_data_snake_case = StringTransformer.convert_keys_to_snake_case(
            capacity_data)

        try:
            capacity_info = dacite.from_dict(
                data_class=AnalysisServiceCapacity,
                data=capacity_data_snake_case,
                config=dacite.Config(
                    check_types=False,
                    cast=[str, int, bool, float],
                ),
            )
            self.logger.info(
                f"Successfully retrieved Analysis Service capacity: {capacity_info.display_name}")
            return capacity_info
        except Exception as e:
            self.logger.error(
                f"Failed to parse Analysis Service capacity data with dacite: {e}")
            self.logger.error(f"Raw capacity data: {capacity_data}")
            self.logger.error(
                f"Converted capacity data: {capacity_data_snake_case}")
            error_msg = f"Failed to parse Analysis Service capacity response: {e}"
            raise RuntimeError(error_msg) from e

    async def set_analysis_service_capacity(self, object_id: str, data: str) -> None:
        self.logger.info(
            f"Setting Analysis Service capacity for object ID: {object_id}")
        self.logger.info(f"Data to set: {data}")
        try:
            json.loads(data)
        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON data provided: {e}"
            self.logger.error(error_msg)
            raise ValueError(error_msg) from e

        response = requests.put(
            f"{self.common_params.endpoint.analysis_service}/metadata/folders/{object_id}",
            headers={
                "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                "Content-Type": "application/json",
            },
            data=data,
            timeout=60,
        )
        response.raise_for_status()
        self.logger.info(
            f"Successfully updated Analysis Service capacity for object ID: {object_id}")

    async def create_managed_identity(self, workspace_params: FabricWorkspaceParams) -> None:
        self.logger.info(
            f"Creating managed identity for workspace: {workspace_params.name}")
        try:
            self.fabric_cli.run_command(
                f"create '{workspace_params.name}.Workspace/.managedidentities/dummy.ManagedIdentity'")
            self.logger.info(
                f"Successfully created managed identity for workspace: {workspace_params.name}")
        except Exception as e:
            error_msg = f"Failed to create managed identity for workspace '{workspace_params.name}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def assign_workspace_storage_reader(
        self, workspace_info: FabricWorkspaceInfo, storage_params: FabricStorageParams
    ) -> None:
        if workspace_info.workspace_identity is None:
            error_msg = f"Workspace '{workspace_info.display_name}' has no managed identity"
            raise RuntimeError(error_msg)

        app_id = workspace_info.workspace_identity.application_id
        object_id = workspace_info.workspace_identity.service_principal_id
        scope = f"/subscriptions/{storage_params.subscription_id}/resourceGroups/{storage_params.resource_group}/providers/Microsoft.Storage/storageAccounts/{storage_params.account}"  # noqa: E501
        role = "Storage Blob Data Reader"

        self.logger.info(
            f"Assigning role '{role}' to workspace identity '{app_id}' for storage scope: {scope}")

        try:
            self.az_cli.run(["role", "assignment", "create", "--assignee-object-id", object_id,
                            "--assignee-principal-type", "ServicePrincipal", "--role", role, "--scope", scope])
            self.logger.info(
                f"Successfully assigned storage role for workspace: {workspace_info.display_name}")
        except Exception as e:
            error_msg = f"Failed to assign storage role for workspace '{workspace_info.display_name}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e
