# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import logging
import tempfile
from pathlib import Path

from fabric_workspace_deployment.manager.azure.storage import AzStorageManager
from fabric_workspace_deployment.operations.operation_interfaces import (
    CommonParams,
    SeedFile,
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
        super().__init__(common_params, logger)
        self.storage_manager = storage_manager
        self.namespace_rewriter = common_params.fabric.create_storage_namespace_rewriter()

    async def _execute(self) -> None:
        """
        Execute seed file upload operations.

        Uploads all configured seed files from local storage to Azure Storage
        based on the configuration in FabricStorageParams.seed_files.

        Raises:
            FileNotFoundError: If a local seed file does not exist
            RuntimeError: If blob upload fails
        """
        self.logger.info("Executing FabricSeedManager")

        with tempfile.TemporaryDirectory(prefix="fabric-workspace-deployment-seed-") as staging_root:
            tasks = []
            for storage in self.common_params.fabric.storages:
                if not storage.seed_files:
                    self.logger.info(f"No seed files configured for storage account '{storage.account}', skipping")
                    continue

                self.logger.info(f"Queuing {len(storage.seed_files)} seed file(s) for storage account '{storage.account}'")
                for i, seed_file in enumerate(storage.seed_files):
                    tasks.append(
                        asyncio.create_task(
                            self._upload_seed_file(
                                index=i,
                                account=storage.account,
                                container=storage.container,
                                seed_file=seed_file,
                                staging_root=Path(staging_root),
                            ),
                            name=f"upload-seed-{storage.account}-{i}",
                        )
                    )

            if not tasks:
                self.logger.info("No seed files configured across any storage account, skipping")
                self.logger.info("Finished executing FabricSeedManager")
                return

            self.logger.info(f"Uploading {len(tasks)} seed file(s) across all storage accounts in parallel")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = [f"Task '{tasks[i].get_name()}': {result}" for i, result in enumerate(results) if isinstance(result, Exception)]
            for err in errors:
                self.logger.error(err)

            if errors:
                raise RuntimeError(f"Failed to upload some seed files: {'; '.join(errors)}")

        self.logger.info("Finished executing FabricSeedManager")

    async def _upload_seed_file(
        self,
        index: int,
        account: str,
        container: str,
        seed_file: SeedFile,
        staging_root: Path,
    ) -> None:
        """
        Upload a single seed file to Azure Storage.

        Args:
            index: The index of the seed file (for logging)
            account: The storage account name
            container: The storage container name
            seed_file: The seed file configuration (concrete or searched local file)
            staging_root: Temporary root for rewritten text seed files

        Raises:
            FileNotFoundError: If the local file does not exist or a searched file matches nothing
            AssertionError: If a searched file matches more than one file
            RuntimeError: If blob upload fails
        """
        local_path = seed_file.resolve_local_absolute_path(self.common_params.local.root_folder)
        upload_path = self.namespace_rewriter.materialize_file(
            local_path,
            staging_root / account / container / str(index) / local_path.name,
        )
        azure_file_path = self.namespace_rewriter.effective_path(
            account,
            container,
            seed_file.storage_account_file.file_path,
        )

        self.logger.info(f"Uploading seed file [{index}]: {local_path} -> {azure_file_path}")

        # Run the synchronous upload_blob in a thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            self.storage_manager.upload_blob,
            account,
            container,
            str(upload_path),
            azure_file_path,
        )

        self.logger.info(f"Successfully uploaded seed file [{index}]: {local_path}")
