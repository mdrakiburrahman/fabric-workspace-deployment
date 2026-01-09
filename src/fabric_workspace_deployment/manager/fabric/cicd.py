# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import logging
import os
import shutil
from pathlib import Path

from azure.core.credentials import TokenCredential

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.fabric.cli import FabricCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    ArtifactType,
    CicdDirectedAcyclicGraph,
    CicdManager,
    CommonParams,
    CustomLibrary,
    FabricWorkspaceParams,
    FabricWorkspaceTemplateParams,
    FolderClient,
    MonitoringManager,
    SparkJobDefinitionClient,
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
        spark_job_definition_client: SparkJobDefinitionClient,
        folder_client: FolderClient,
        monitoring_manager: MonitoringManager,
    ):
        """
        Initialize the Fabric CICD manager.
        """
        super().__init__(common_params)
        self.token_credential = token_credential
        self.az_cli = az_cli
        self.fabric_cli = fabric_cli
        self.workspace = workspace
        self.spark_job_definition_client = spark_job_definition_client
        self.folder_client = folder_client
        self.monitoring_manager = monitoring_manager
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

        self.logger.info("Processing Spark Job Definitions")
        if template_params.spark_job_definitions:
            await self._create_spark_job_definition_artifacts(workspace_id, template_params)
        else:
            self.logger.info("No Spark Job Definitions configured, skipping")

        self.logger.info("Processing custom library manifests")
        if template_params.custom_libraries:
            self._copy_all_custom_libraries(template_params.custom_libraries)
        else:
            self.logger.info("No custom libraries configured, skipping custom library copy")

        self.logger.info("Processing monitoring template generation")
        if template_params.generator.monitoring:
            workspace_params = None
            for wp in self.common_params.fabric.workspaces:
                workspace_info = await self.workspace.get(wp)
                if workspace_info.id == workspace_id:
                    workspace_params = wp
                    break

            if workspace_params:
                await self._generate_template(workspace_id, workspace_params, template_params)
            else:
                error_msg = f"Could not find workspace params for workspace ID: {workspace_id}"
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)
        else:
            self.logger.info("No monitoring templates configured, skipping template generation")

        import fabric_cicd

        if logging.getLogger().level != current_level:
            logging.getLogger().setLevel(current_level)
        if logging.getLogger().handlers != current_handlers:
            logging.getLogger().handlers = current_handlers

        self.logger.info(f"Starting CICD reconciliation for workspace ID: {workspace_id}")

        param_file_full_path = os.path.join(self.common_params.local.root_folder, template_params.parameter_file_path)
        with open(param_file_full_path, encoding="utf-8") as f:
            param_content = f.read()
        self.logger.debug(f"Parameter file content ({param_file_full_path}):\n{param_content}")

        fabric_cicd.constants.DEFAULT_API_ROOT_URL = self.common_params.endpoint.cicd

        dag = CicdDirectedAcyclicGraph()
        dag_summary = dag.get_dependency_summary(template_params.item_types_in_scope)
        self.logger.debug(f"\n{dag_summary}")

        deployment_batches = dag.get_deployment_batches(template_params.item_types_in_scope)
        self.logger.info(f"Deployment will proceed in {len(deployment_batches)} batch(es)")

        for batch_idx, batch in enumerate(deployment_batches, start=1):
            self.logger.info(f"Batch {batch_idx}/{len(deployment_batches)}: Deploying {len(batch)} artifact type(s): {batch}")

            target_workspace = fabric_cicd.FabricWorkspace(
                workspace_id=workspace_id,
                environment=template_params.environment_key,
                repository_directory=str(Path(self.common_params.local.root_folder) / template_params.artifacts_folder),
                item_type_in_scope=batch,
                token_credential=self.token_credential,
            )

            if batch_idx == 1:  # Only add feature flags once
                self.logger.info(f"Adding {len(template_params.feature_flags)} feature flags")
                for feature_flag in template_params.feature_flags:
                    self.logger.debug(f"Adding feature flag: {feature_flag}")
                    fabric_cicd.append_feature_flag(feature_flag)

            self.logger.info(f"Publishing items in batch {batch_idx}")
            await asyncio.to_thread(fabric_cicd.publish_all_items, target_workspace)
            self.logger.info(f"Completed batch {batch_idx}/{len(deployment_batches)}")

        if template_params.unpublish_orphans:
            self.logger.info("Unpublishing orphan items")
            # Use full scope for unpublishing orphans
            target_workspace_full = fabric_cicd.FabricWorkspace(
                workspace_id=workspace_id,
                environment=template_params.environment_key,
                repository_directory=str(Path(self.common_params.local.root_folder) / template_params.artifacts_folder),
                item_type_in_scope=template_params.item_types_in_scope,
                token_credential=self.token_credential,
            )
            await asyncio.to_thread(fabric_cicd.unpublish_all_orphan_items, target_workspace_full)

        self.logger.info("Updating Spark Job Definition configurations")
        if template_params.spark_job_definitions:
            await self._update_spark_job_definition_configs(workspace_id, template_params)
        else:
            self.logger.info("No Spark Job Definitions configured, skipping config update")

        self.logger.info(f"Completed CICD reconciliation for workspace ID: {workspace_id}")

    def _clean_path(self, dest_folder: Path) -> None:
        """
        Clean all contents from a destination folder.

        Args:
            dest_folder: The folder path to clean

        """
        if dest_folder.exists():
            self.logger.info(f"Cleaning destination folder: {dest_folder}")
            for item in dest_folder.iterdir():
                if item.is_file():
                    item.unlink()
                    self.logger.debug(f"Deleted file: {item}")
                elif item.is_dir():
                    shutil.rmtree(item)
                    self.logger.debug(f"Deleted directory: {item}")
            self.logger.info(f"Successfully cleaned {dest_folder}")

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
        folders_to_clean = set()
        for custom_lib in custom_libraries:
            if custom_lib.dest.clean_folder_path:
                for dest_path_str in custom_lib.dest.folder_path:
                    dest_folder = Path(dest_path_str)
                    if not dest_folder.is_absolute():
                        dest_folder = Path(self.common_params.local.root_folder) / dest_path_str
                    folders_to_clean.add(dest_folder)

        for dest_folder in folders_to_clean:
            self._clean_path(dest_folder)

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

    async def _generate_template(
        self,
        workspace_id: str,
        workspace_params: FabricWorkspaceParams,
        template_params: FabricWorkspaceTemplateParams,
    ) -> None:
        """
        Generate monitoring template files by replacing placeholders with actual values.

        Args:
            workspace_id: The workspace ID
            workspace_params: Workspace parameters containing capacity information
            template_params: Template parameters containing monitoring generator config

        Raises:
            FileNotFoundError: If source file doesn't exist
            RuntimeError: If monitoring metadata cannot be retrieved
        """
        self.logger.info(f"Generating {len(template_params.generator.monitoring)} monitoring template file(s)")
        monitoring_metadata = await self.monitoring_manager.get_monitoring_metadata(workspace_id, workspace_params)

        cluster_uri = monitoring_metadata.kusto.query_service_uri
        database_id = monitoring_metadata.kusto.database_id

        self.logger.info(f"Retrieved monitoring metadata - Cluster URI: {cluster_uri}, Database ID: {database_id}")

        for idx, monitoring_file in enumerate(template_params.generator.monitoring):
            self.logger.info(f"Processing monitoring template {idx + 1}/{len(template_params.generator.monitoring)}: " f"{monitoring_file.source} -> {monitoring_file.dest}")

            source_path = Path(monitoring_file.source)
            if not source_path.is_absolute():
                source_path = Path(self.common_params.local.root_folder) / monitoring_file.source

            if not source_path.exists():
                error_msg = f"Source template file does not exist: {source_path}"
                self.logger.error(error_msg)
                raise FileNotFoundError(error_msg)

            self.logger.debug(f"Reading source file: {source_path}")
            with open(source_path, encoding="utf-8") as f:
                content = f.read()

            original_content = content
            content = content.replace("{monitoring_kql_cluster_uri}", cluster_uri)
            content = content.replace("{monitoring_kql_database_id}", database_id)

            replacements_made = content != original_content
            if replacements_made:
                self.logger.debug(f"Replaced placeholders in {source_path.name}: " f"{{monitoring_kql_cluster_uri}} -> {cluster_uri}, " f"{{monitoring_kql_database_id}} -> {database_id}")
            else:
                self.logger.debug(f"No placeholders found in {source_path.name}")

            dest_path = Path(monitoring_file.dest)
            if not dest_path.is_absolute():
                dest_path = Path(self.common_params.local.root_folder) / monitoring_file.dest

            dest_path.parent.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Ensured destination directory exists: {dest_path.parent}")
            self.logger.info(f"Writing generated file to: {dest_path}")
            with open(dest_path, "w", encoding="utf-8") as f:
                f.write(content)

            self.logger.info(f"Successfully generated monitoring template {idx + 1}/{len(template_params.generator.monitoring)}")

        self.logger.info("Completed monitoring template generation")

    async def _create_spark_job_definition_artifacts(self, workspace_id: str, template_params: FabricWorkspaceTemplateParams) -> None:
        """
        Create all Spark Job Definitions for the workspace.

        Args:
            workspace_id: The workspace id
            template_params: Template parameters containing Spark Job Definitions
        """
        self.logger.info(f"Creating {len(template_params.spark_job_definitions)} Spark Job Definition(s)")
        tasks = []
        for sjd in template_params.spark_job_definitions:
            task = asyncio.create_task(self.spark_job_definition_client.create_spark_job_definition_artifact(workspace_id, sjd), name=f"create-sjd-{sjd.display_name}")
            tasks.append(task)

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    sjd_name = template_params.spark_job_definitions[i].display_name
                    error_msg = f"Failed to create Spark Job Definition '{sjd_name}': {result}"
                    self.logger.error(error_msg)
                    errors.append(error_msg)
                else:
                    self.logger.info(f"Successfully created/retrieved Spark Job Definition: {result.display_name}")

            if errors:
                raise Exception(f"Failed to create some Spark Job Definitions: {'; '.join(errors)}")

        self.logger.info("Completed Spark Job Definition creation")

    async def _update_spark_job_definition_configs(self, workspace_id: str, template_params: FabricWorkspaceTemplateParams) -> None:
        """
        Update all Spark Job Definition configurations with correct Lakehouse artifact IDs.

        Args:
            workspace_id: The workspace id
            template_params: Template parameters containing Spark Job Definitions
        """
        self.logger.info(f"Updating configurations for {len(template_params.spark_job_definitions)} Spark Job Definition(s)")
        artifact_collection = await self.folder_client.get_fabric_folder_collection(workspace_id)
        sjd_map = {artifact.display_name: artifact.object_id for artifact in artifact_collection.artifacts if artifact.type_name == ArtifactType.SPARK_JOB_DEFINITION.value}

        self.logger.info(f"Found {len(sjd_map)} Spark Job Definition(s) in workspace: {list(sjd_map.keys())}")

        tasks = []
        for sjd in template_params.spark_job_definitions:
            if sjd.display_name not in sjd_map:
                error_msg = f"Spark Job Definition '{sjd.display_name}' not found in workspace. " f"Available: {list(sjd_map.keys())}"
                self.logger.error(error_msg)
                raise ValueError(error_msg)

            sjd_artifact_id = sjd_map[sjd.display_name]
            lakehouse_name = sjd.default_lakehouse_artifact_name

            self.logger.info(f"Updating Spark Job Definition '{sjd.display_name}' " f"(ID: {sjd_artifact_id}) with default Lakehouse '{lakehouse_name}'")

            task = asyncio.create_task(
                self.spark_job_definition_client.update_spark_job_definition_config(
                    workspace_id,
                    sjd_artifact_id,
                    lakehouse_name,
                    sjd.spark_job_definition_v1_config,
                ),
                name=f"update-sjd-config-{sjd.display_name}",
            )
            tasks.append(task)

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    sjd_name = template_params.spark_job_definitions[i].display_name
                    error_msg = f"Failed to update Spark Job Definition config for '{sjd_name}': {result}"
                    self.logger.error(error_msg)
                    errors.append(error_msg)
                else:
                    self.logger.info(f"Successfully updated config for Spark Job Definition: " f"{template_params.spark_job_definitions[i].display_name}")

            if errors:
                raise Exception(f"Failed to update some Spark Job Definition configs: {'; '.join(errors)}")

        self.logger.info("Completed Spark Job Definition configuration updates")
