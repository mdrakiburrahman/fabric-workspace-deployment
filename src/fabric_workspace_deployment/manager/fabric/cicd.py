# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import logging
import shutil
from pathlib import Path

from azure.core.credentials import TokenCredential

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.fabric.cli import FabricCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    CicdManager,
    CommonParams,
    CustomLibrary,
    FabricWorkspaceTemplateParams,
    WorkspaceManager,
)


class FabricCicdManager(CicdManager):
    """Concrete implementation of CicdManager for Microsoft Fabric."""

    def __init__(
        self,
        common_params: CommonParams,
        token_credential: TokenCredential,
        az_cli: AzCli,
        fabric_cli: FabricCli,
        workspace: WorkspaceManager,
    ):
        """
        Initialize the Fabric CICD manager.
        """
        super().__init__(common_params)
        self.token_credential = token_credential
        self.az_cli = az_cli
        self.fabric_cli = fabric_cli
        self.workspace = workspace
        self.logger = logging.getLogger(__name__)

    async def execute(self) -> None:
        self.logger.info("Executing FabricCicdManager")
        tasks = []
        for workspace_params in self.common_params.fabric.workspaces:
            if workspace_params.skip_deploy:
                self.logger.info(f"Skipping CICD for workspace '{workspace_params.name}' due to skipDeploy=true")
                continue
            workspace_info = await self.workspace.get(workspace_params)
            task = asyncio.create_task(self.reconcile(workspace_info.id, workspace_params.template), name=f"reconcile-cicd-{workspace_params.name}")
            tasks.append(task)

        if tasks:
            self.logger.info(f"Executing CICD reconciliation for {len(tasks)} workspaces in parallel")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    workspace_name = self.common_params.fabric.workspaces[i].name
                    error_msg = f"Failed to reconcile CICD for workspace '{workspace_name}': {result}"
                    self.logger.error(error_msg)
                    errors.append(error_msg)

            if errors:
                error = f"Failed to reconcile CICD for some workspaces: {'; '.join(errors)}"
                raise Exception(error)
        else:
            self.logger.info("No workspaces found to reconcile CICD")

        self.logger.info("Finished executing FabricCicdManager")

    async def reconcile(self, workspace_id: str, template_params: FabricWorkspaceTemplateParams) -> None:
        """
        Reconcile CICD for a single workspace.

        Import fabric_cicd lazily to avoid logging configuration conflicts.
        """
        current_level = logging.getLogger().level
        current_handlers = logging.getLogger().handlers[:]

        self.logger.info("Processing custom library manifests")
        if template_params.custom_libraries:
            self._copy_all_custom_libraries(template_params.custom_libraries)
        else:
            self.logger.info("No custom libraries configured, skipping custom library copy")

        import fabric_cicd

        if logging.getLogger().level != current_level:
            logging.getLogger().setLevel(current_level)
        if logging.getLogger().handlers != current_handlers:
            logging.getLogger().handlers = current_handlers

        self.logger.info(f"Starting CICD reconciliation for workspace ID: {workspace_id}")

        fabric_cicd.constants.DEFAULT_API_ROOT_URL = self.common_params.endpoint.cicd
        target_workspace = fabric_cicd.FabricWorkspace(
            workspace_id=workspace_id,
            environment=template_params.environment_key,
            repository_directory=str(Path(self.common_params.local.root_folder) / template_params.artifacts_folder),
            item_type_in_scope=template_params.item_types_in_scope,
            token_credential=self.token_credential,
        )

        self.logger.info(f"Adding {len(template_params.feature_flags)} feature flags")
        for feature_flag in template_params.feature_flags:
            self.logger.debug(f"Adding feature flag: {feature_flag}")
            fabric_cicd.append_feature_flag(feature_flag)

        self.logger.info("Publishing all items")
        fabric_cicd.publish_all_items(target_workspace)

        if template_params.unpublish_orphans:
            self.logger.info("Unpublishing orphan items")
            fabric_cicd.unpublish_all_orphan_items(target_workspace)

        self.logger.info(f"Completed CICD reconciliation for workspace ID: {workspace_id}")

    def _copy_all_custom_libraries(self, custom_libraries: list[CustomLibrary]) -> None:
        """
        Copy custom library files based on the configuration.

        For each custom library, searches the specified source folderPath for files matching the
        prefix and suffix, then copies the matching file(s) to all destination folderPath(s).

        Args:
            custom_libraries: List of CustomLibrary configurations

        Raises:
            FileNotFoundError: If the source folderPath doesn't exist
            AssertionError: If errorOnMultiple is True and multiple files match the pattern
            FileNotFoundError: If no files match the pattern
        """
        for idx, custom_lib in enumerate(custom_libraries):
            self.logger.info(f"Processing custom library {idx + 1}/{len(custom_libraries)}")

            source_folder = Path(custom_lib.source.folder_path)
            if not source_folder.is_absolute():
                source_folder = Path(self.common_params.local.root_folder) / custom_lib.source.folder_path

            if not source_folder.exists():
                error_msg = f"Source folder does not exist: {source_folder}"
                self.logger.error(error_msg)
                raise FileNotFoundError(error_msg)

            pattern = f"{custom_lib.source.prefix}*{custom_lib.source.suffix}"
            matching_files = list(source_folder.glob(pattern))

            self.logger.info(f"Searching for pattern '{pattern}' in {source_folder}, found {len(matching_files)} file(s)")

            if custom_lib.source.error_on_multiple and len(matching_files) > 1:
                error_msg = f"Multiple files match pattern '{pattern}' in {source_folder}: " f"{[f.name for f in matching_files]}"
                self.logger.error(error_msg)
                raise AssertionError(error_msg)

            if len(matching_files) == 0:
                error_msg = f"No files match pattern '{pattern}' in {source_folder}"
                self.logger.error(error_msg)
                raise FileNotFoundError(error_msg)

            for dest_path_str in custom_lib.dest.folder_path:
                dest_folder = Path(dest_path_str)
                if not dest_folder.is_absolute():
                    dest_folder = Path(self.common_params.local.root_folder) / dest_path_str

                dest_folder.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"Copying to destination: {dest_folder}")

                for source_file in matching_files:
                    dest_file = dest_folder / source_file.name
                    self.logger.info(f"Copying {source_file} to {dest_file}")
                    shutil.copy2(source_file, dest_file)
                    self.logger.info(f"Successfully copied {source_file.name} to {dest_folder}")
