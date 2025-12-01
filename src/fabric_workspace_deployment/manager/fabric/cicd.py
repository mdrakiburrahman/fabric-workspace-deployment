# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import logging
from pathlib import Path

from azure.core.credentials import TokenCredential

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.fabric.cli import FabricCli
from fabric_workspace_deployment.operations.operation_interfaces import CicdManager, CommonParams, FabricWorkspaceTemplateParams, WorkspaceManager


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
            task = asyncio.create_task(
                self.reconcile(workspace_info.id, workspace_params.template), name=f"reconcile-cicd-{workspace_params.name}"
            )
            tasks.append(task)

        if tasks:
            self.logger.info(
                f"Executing CICD reconciliation for {len(tasks)} workspaces in parallel")
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

        import fabric_cicd

        if logging.getLogger().level != current_level:
            logging.getLogger().setLevel(current_level)
        if logging.getLogger().handlers != current_handlers:
            logging.getLogger().handlers = current_handlers

        self.logger.info(
            f"Starting CICD reconciliation for workspace ID: {workspace_id}")

        fabric_cicd.constants.DEFAULT_API_ROOT_URL = self.common_params.endpoint.cicd
        target_workspace = fabric_cicd.FabricWorkspace(
            workspace_id=workspace_id,
            environment=template_params.environment_key,
            repository_directory=str(
                Path(self.common_params.local.root_folder) / template_params.artifacts_folder),
            item_type_in_scope=template_params.item_types_in_scope,
            token_credential=self.token_credential,
        )

        self.logger.info(
            f"Adding {len(template_params.feature_flags)} feature flags")
        for feature_flag in template_params.feature_flags:
            self.logger.debug(f"Adding feature flag: {feature_flag}")
            fabric_cicd.append_feature_flag(feature_flag)

        self.logger.info("Publishing all items")
        fabric_cicd.publish_all_items(target_workspace)

        if template_params.unpublish_orphans:
            self.logger.info("Unpublishing orphan items")
            fabric_cicd.unpublish_all_orphan_items(target_workspace)

        self.logger.info(
            f"Completed CICD reconciliation for workspace ID: {workspace_id}")
