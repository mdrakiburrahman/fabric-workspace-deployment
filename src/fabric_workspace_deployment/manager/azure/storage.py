# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import json
import logging
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    AzureStorageManager,
    CommonParams,
)


class AzStorageManager(AzureStorageManager):
    """Concrete implementation of AzureStorageManager for Azure Storage operations."""

    END_OF_JOB_IDENTIFIER = "EndOfJob"
    SUCCESS_IDENTIFIER = "Completed"

    def __init__(self, common_params: CommonParams, az_cli: AzCli | None = None, logger: logging.Logger | None = None):
        """
        Initialize the Azure Storage manager.

        Args:
            common_params: Common parameters containing storage configuration
            az_cli: Optional AzCli instance for running commands
            logger: Optional logger instance
        """
        super().__init__(common_params)
        self.az_cli = az_cli or AzCli(exit_on_error=True)
        self.logger = logger or logging.getLogger(__name__)

    def generate_user_delegated_sas_token(self, account: str, container: str, duration_in_minutes: int = 5) -> str:
        """
        Generate a User Delegated SAS token.

        Args:
            account: The storage account name
            container: The storage container name
            duration_in_minutes: The duration of the SAS token in minutes (default: 5)

        Returns:
            str: The SAS token string with leading '?'

        Raises:
            RuntimeError: If SAS token generation fails
        """
        try:
            expiry_time = datetime.now(timezone.utc) + timedelta(minutes=duration_in_minutes)
            formatted_expiry_time = expiry_time.strftime("%Y-%m-%dT%H:%MZ")

            self.logger.debug(f"Generating SAS token for account={account}, container={container}, expiry={formatted_expiry_time}")

            stdout, stderr = self.az_cli.run(
                [
                    "storage",
                    "container",
                    "generate-sas",
                    "--account-name",
                    account,
                    "--name",
                    container,
                    "--permissions",
                    "w",
                    "--expiry",
                    formatted_expiry_time,
                    "--auth-mode",
                    "login",
                    "--as-user",
                ],
                timeout=60,
            )

            sas_token = stdout.strip().strip('"')

            if not sas_token:
                error_msg = f"Failed to generate SAS token: {stderr}"
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)

            self.logger.debug("Successfully generated SAS token")
            return f"?{sas_token}"

        except Exception as e:
            error_msg = f"Failed to generate SAS token with error: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def upload_blob(self, account: str, container: str, absolute_local_file_path: str, azure_file_path: str) -> None:
        """
        Upload a blob to Azure Storage.

        Args:
            account: The storage account name
            container: The storage container name
            absolute_local_file_path: The absolute local file path
            azure_file_path: The Azure file path (relative path in container)

        Raises:
            FileNotFoundError: If the local file does not exist
            RuntimeError: If blob upload fails
        """
        try:
            local_file = Path(absolute_local_file_path)
            if not local_file.exists():
                error_msg = f"Local file does not exist: {absolute_local_file_path}"
                self.logger.error(error_msg)
                raise FileNotFoundError(error_msg)

            if not local_file.is_file():
                error_msg = f"Path is not a file: {absolute_local_file_path}"
                self.logger.error(error_msg)
                raise FileNotFoundError(error_msg)

            sas_token = self.generate_user_delegated_sas_token(account, container)
            destination_url = f"https://{account}.blob.core.windows.net/{container}/{azure_file_path}"

            self.logger.info(f"Uploading blob from {absolute_local_file_path} to {destination_url}")

            result = subprocess.run(
                [
                    "az",
                    "storage",
                    "copy",
                    "--sas-token",
                    sas_token,
                    "--source",
                    absolute_local_file_path,
                    "--destination",
                    destination_url,
                    "--",
                    "--output-type",
                    "json",
                ],
                capture_output=True,
                text=True,
                timeout=300,
                check=False,
            )

            blob_upload_status = result.stdout

            if not blob_upload_status.strip():
                error_msg = f"No output from blob upload command. stderr: {result.stderr}"
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)

            end_of_job_payload = None
            for line in blob_upload_status.split("\n"):
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                    if isinstance(item, dict) and item.get("MessageType") == self.END_OF_JOB_IDENTIFIER:
                        end_of_job_payload = item
                        break
                except json.JSONDecodeError:
                    continue

            if not end_of_job_payload:
                error_msg = f"EndOfJob message not found in the upload status: {blob_upload_status}"
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)

            blob_upload_timestamp = end_of_job_payload.get("TimeStamp")
            message_content_str = end_of_job_payload.get("MessageContent")

            if not message_content_str:
                error_msg = "MessageContent not found in EndOfJob payload"
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)

            try:
                end_of_job_msg_content = json.loads(message_content_str)
            except json.JSONDecodeError as e:
                error_msg = f"Failed to parse MessageContent JSON: {e}"
                self.logger.error(error_msg)
                raise RuntimeError(error_msg) from e

            job_status = end_of_job_msg_content.get("JobStatus")

            if job_status != self.SUCCESS_IDENTIFIER:
                error_msg = f"Blob upload failed at {blob_upload_timestamp} with status: {job_status} " f"and message: {json.dumps(blob_upload_status, indent=2)}"
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)

            self.logger.info(f"Successfully uploaded blob to Azure Storage at {blob_upload_timestamp} " f"with status: {json.dumps(end_of_job_msg_content, indent=2)}")

        except FileNotFoundError:
            raise
        except Exception as e:
            error_msg = f"Blob upload failed with error: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e
