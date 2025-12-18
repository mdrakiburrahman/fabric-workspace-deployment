# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import logging
from pathlib import Path

from fabric_workspace_deployment.manager.azure.storage import AzStorageManager
from fabric_workspace_deployment.operations.operation_interfaces import (
    CommonParams,
    SeedManager,
)


class FabricSeedManager(SeedManager):
    """Concrete implementation of SeedManager for uploading seed files to Azure Storage."""

    def __init__(self, common_params: CommonParams, storage_manager: AzStorageManager, logger: logging.Logger | None = None):
        """
        Initialize the Fabric Seed manager.

        Args:
            common_params: Common parameters containing seed file configuration
            storage_manager: Azure Storage manager for uploading blobs
            logger: Optional logger instance
        """
        super().__init__(common_params)
        self.storage_manager = storage_manager
        self.logger = logger or logging.getLogger(__name__)

    async def execute(self) -> None:
        """
        Execute seed file upload operations.

        Uploads all configured seed files from local storage to Azure Storage
        based on the configuration in FabricStorageParams.seed_files.

        Raises:
            FileNotFoundError: If a local seed file does not exist
            RuntimeError: If blob upload fails
        """
        self.logger.info("Executing FabricSeedManager")

        storage = self.common_params.fabric.storage
        seed_files = storage.seed_files

        if not seed_files or len(seed_files) == 0:
            self.logger.info("No seed files configured, skipping seed upload")
            return

        self.logger.info(f"Uploading {len(seed_files)} seed file(s) to Azure Storage")

        tasks = []
        for i, seed_file in enumerate(seed_files):
            task = asyncio.create_task(
                self._upload_seed_file(
                    index=i,
                    account=storage.account,
                    container=storage.container,
                    local_file_path=seed_file.local_concrete_file.file_path,
                    azure_file_path=seed_file.storage_account_file.file_path,
                ),
                name=f"upload-seed-{i}",
            )
            tasks.append(task)

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    error_msg = f"Failed to upload seed file at index {i}: {result}"
                    self.logger.error(error_msg)
                    errors.append(error_msg)

            if errors:
                error = f"Failed to upload some seed files: {'; '.join(errors)}"
                raise RuntimeError(error)

        self.logger.info("Finished executing FabricSeedManager")

    async def _upload_seed_file(
        self,
        index: int,
        account: str,
        container: str,
        local_file_path: str,
        azure_file_path: str,
    ) -> None:
        """
        Upload a single seed file to Azure Storage.

        Args:
            index: The index of the seed file (for logging)
            account: The storage account name
            container: The storage container name
            local_file_path: The relative local file path (from root folder)
            azure_file_path: The Azure file path (relative path in container)

        Raises:
            FileNotFoundError: If the local file does not exist
            RuntimeError: If blob upload fails
        """
        absolute_local_path = str(Path(self.common_params.local.root_folder) / local_file_path)

        self.logger.info(f"Uploading seed file [{index}]: {local_file_path} -> {azure_file_path}")

        # Run the synchronous upload_blob in a thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            self.storage_manager.upload_blob,
            account,
            container,
            absolute_local_path,
            azure_file_path,
        )

        self.logger.info(f"Successfully uploaded seed file [{index}]: {local_file_path}")
