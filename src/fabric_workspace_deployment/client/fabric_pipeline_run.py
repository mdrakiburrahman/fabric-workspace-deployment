# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import logging
import requests

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    CommonParams,
    HttpRetryHandler,
    PipelineRunClient,
)


class PipelineStatus(Enum):
    """
    Enumeration of Fabric Pipeline run statuses.

    Note: Fabric uses numeric status codes that can be combined with commas.
    """

    NOT_STARTED = "0"
    IN_PROGRESS = "1"
    SUCCEEDED = "2"
    FAILED = "3,5,7,8"
    CANCELLED = "4,6"
    UNKNOWN = "9"


@dataclass
class OwnerUser:
    """Owner user information for a pipeline run."""

    id: int
    name: str
    object_id: str
    user_principal_name: str
    aad_app_id: str | None
    type: str | None


@dataclass
class PipelineRun:
    """Represents a Fabric Pipeline run."""

    id: int
    artifact_job_instance_id: str
    owner_user_id: int
    artifact_id: int
    tenant_object_id: str
    artifact_object_id: str
    artifact_type: str
    artifact_name: str | None
    workspace_object_id: str
    workspace_name: str | None
    artifact_job_type: str
    artifact_job_permission: int
    artifact_job_invoke_type: int
    is_successful: bool | None
    status: int
    root_activity_id: str
    job_schedule_time_utc: str
    job_start_time_utc: str
    job_end_time_utc: str | None
    job_next_check_time_utc: str | None
    service_exception_json: str | None
    artifact_job_history_properties: dict[str, Any]
    owner_user: OwnerUser
    capacity_display_name: str | None
    artifact_job_permission_string: str
    artifact_job_invoke_type_string: str
    status_string: str
    triggered_by_artifact_object_id: str | None
    triggered_by_artifact_job_object_id: str | None
    trigger_job_count: int
    triggered_by_artifact_name: str | None
    triggered_by_artifact_permissions: int | None
    as_azure_object_id: str
    job_definition_object_id: str | None
    window_offset: str | None
    retries_count: int
    job_type_public_name: str


@dataclass
class StartPipelineResponse:
    """Response from starting a pipeline run."""

    id: int
    artifact_job_instance_id: str
    owner_user_id: int | None
    tenant_object_id: str | None
    artifact_id: int
    artifact_object_id: str
    artifact_job_type: str
    artifact_job_permission: int
    artifact_job_permission_string: str
    artifact_job_invoke_type: int
    artifact_job_invoke_type_string: str
    is_successful: bool | None
    status: int
    status_string: str
    root_activity_id: str
    job_schedule_time_utc: str | None
    job_start_time_utc: str | None
    job_end_time_utc: str | None
    job_next_check_time_utc: str | None
    service_exception_json: str | None
    triggered_by_artifact_object_id: str | None
    triggered_by_artifact_job_object_id: str | None
    trigger_job_count: int | None
    triggered_by_artifact_name: str | None
    triggered_by_artifact_permissions: int | None
    as_azure_object_id: str | None
    job_definition_object_id: str | None
    concurrency: int | None
    window_offset: str | None
    retries_count: int | None


class FabricPipelineRunClient(PipelineRunClient):
    """
    Client for interacting with Fabric Pipeline runs.

    This client provides methods to list, start, cancel, and monitor
    pipeline run executions in a Fabric workspace.
    """

    def __init__(
        self,
        common_params: CommonParams,
        az_cli: AzCli,
        http_retry_handler: HttpRetryHandler,
    ):
        """
        Initialize the Fabric Pipeline Run client.

        Args:
            common_params: Common configuration parameters
            az_cli: Azure CLI client for authentication
            http_retry_handler: HTTP retry handler for resilient requests
        """
        super().__init__(common_params)
        self.az_cli = az_cli
        self.http_retry = http_retry_handler
        self.logger = logging.getLogger(__name__)

    def get_non_terminal_statuses(self) -> list[str]:
        """
        Get the list of non-terminal pipeline statuses.

        Returns:
            List of status codes that represent non-terminal states
        """
        return [
            PipelineStatus.NOT_STARTED.value,
            PipelineStatus.IN_PROGRESS.value,
        ]

    async def list_non_terminal_runs(self, workspace_object_id: str, pipeline_id: str) -> list[PipelineRun]:
        """
        List all non-terminal pipeline runs.

        Args:
            workspace_object_id: The Fabric workspace object ID
            pipeline_id: The pipeline artifact object ID

        Returns:
            List of PipelineRun objects in non-terminal states

        Raises:
            RuntimeError: If the API call fails
        """
        return await self.list_runs(workspace_object_id, pipeline_id, self.get_non_terminal_statuses())

    async def list_runs(
        self,
        workspace_object_id: str,
        pipeline_id: str,
        statuses: list[str],
        limit: int = 1000,
        days_back: int = 90,
    ) -> list[PipelineRun]:
        """
        List pipeline runs filtered by status.

        Args:
            workspace_object_id: The Fabric workspace object ID
            pipeline_id: The pipeline artifact object ID
            statuses: List of status codes to filter by (e.g., ["0", "1"])
            limit: Maximum number of runs to return (default: 1000)
            days_back: Number of days to look back (default: 90)

        Returns:
            List of PipelineRun objects matching the status filter

        Raises:
            RuntimeError: If the API call fails
        """
        self.logger.info(f"Listing pipeline runs for pipeline {pipeline_id} in workspace {workspace_object_id} " f"with statuses {statuses}")

        try:
            # Calculate time range
            end_time = datetime.utcnow()
            start_time = datetime(1970, 1, 1)  # Epoch start as in the sample

            # Join statuses with comma (e.g., "0,1" for NOT_STARTED and IN_PROGRESS)
            status_param = ",".join(statuses)

            # Build the URL with query parameters
            url = f"{self.common_params.endpoint.analysis_service}/metadata/artifacts/{pipeline_id}/jobs" f"?limit={limit}" f"&endTime={end_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z" f"&artifactObjectId={pipeline_id}" f"&artifactType=Pipeline" f"&status={status_param}" f"&startTime={start_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z" f"&usePublicName=true"

            response = self.http_retry.execute(
                requests.get,
                url,
                headers={
                    "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                    "Content-Type": "application/json",
                },
                timeout=60,
            )

            response.raise_for_status()
            runs_data = response.json()

            # Parse the response into PipelineRun objects
            runs = [self._cast_to_pipeline_run(run) for run in runs_data]

            self.logger.info(f"Found {len(runs)} pipeline runs matching statuses {statuses}")
            return runs

        except Exception as e:
            error_msg = f"Failed to list pipeline runs for pipeline {pipeline_id}: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def start_run(
        self,
        workspace_object_id: str,
        pipeline_id: str,
        parameters: dict[str, Any] | None = None,
    ) -> StartPipelineResponse:
        """
        Start a pipeline run.

        Args:
            workspace_object_id: The Fabric workspace object ID
            pipeline_id: The pipeline artifact object ID
            parameters: Optional pipeline parameters as a dictionary

        Returns:
            StartPipelineResponse with the run details

        Raises:
            RuntimeError: If the API call fails
        """
        self.logger.info(f"Starting pipeline run for pipeline {pipeline_id} in workspace {workspace_object_id}")

        try:
            url = f"{self.common_params.endpoint.analysis_service}/metadata/artifacts/{pipeline_id}/jobs/Pipeline"

            # Prepare payload
            params = parameters or {}
            payload = {"jobPayloadJson": f'{{"parameters":{params}}}'}

            response = self.http_retry.execute(
                requests.post,
                url,
                headers={
                    "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=60,
            )

            response.raise_for_status()
            response_data = response.json()

            start_response = self._cast_to_start_response(response_data)
            self.logger.info(f"Successfully started pipeline run with instance ID {start_response.artifact_job_instance_id}")
            return start_response

        except Exception as e:
            error_msg = f"Failed to start pipeline run for pipeline {pipeline_id}: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def start_run_idempotently(
        self,
        workspace_object_id: str,
        pipeline_id: str,
        parameters: dict[str, Any] | None = None,
    ) -> StartPipelineResponse | None:
        """
        Start a pipeline run only if no runs are in NOT_STARTED or IN_PROGRESS state.

        Args:
            workspace_object_id: The Fabric workspace object ID
            pipeline_id: The pipeline artifact object ID
            parameters: Optional pipeline parameters as a dictionary

        Returns:
            StartPipelineResponse if a new run was started, None if a run is already in progress

        Raises:
            RuntimeError: If the API call fails
        """
        self.logger.info(f"Checking for existing runs before starting pipeline {pipeline_id}")

        non_terminal_runs = await self.list_non_terminal_runs(workspace_object_id, pipeline_id)

        if non_terminal_runs:
            self.logger.info(f"Pipeline {pipeline_id} already has {len(non_terminal_runs)} run(s) in progress, skipping start")
            return None

        self.logger.info(f"No runs in progress, starting pipeline {pipeline_id}")
        return await self.start_run(workspace_object_id, pipeline_id, parameters)

    async def cancel_run(
        self,
        workspace_object_id: str,
        pipeline_id: str,
        run_instance_id: str,
    ) -> bool:
        """
        Cancel a running pipeline.

        Args:
            workspace_object_id: The Fabric workspace object ID
            pipeline_id: The pipeline artifact object ID
            run_instance_id: The artifact job instance ID to cancel

        Returns:
            True if cancellation was accepted (HTTP 202)

        Raises:
            RuntimeError: If the API call fails
        """
        self.logger.info(f"Cancelling pipeline run {run_instance_id} for pipeline {pipeline_id} " f"in workspace {workspace_object_id}")

        try:
            url = f"{self.common_params.endpoint.analysis_service}/metadata/artifacts/{pipeline_id}/jobs/{run_instance_id}"

            response = self.http_retry.execute(
                requests.delete,
                url,
                headers={
                    "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                    "Content-Type": "application/json",
                },
                timeout=60,
            )

            success = response.status_code == 202
            if success:
                self.logger.info(f"Successfully cancelled pipeline run {run_instance_id}")
            else:
                self.logger.warning(f"Cancel request returned status {response.status_code} for run {run_instance_id}")

            return success

        except Exception as e:
            error_msg = f"Failed to cancel pipeline run {run_instance_id}: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def get_run(
        self,
        workspace_object_id: str,
        pipeline_id: str,
        run_instance_id: str,
    ) -> PipelineRun | None:
        """
        Get a specific pipeline run by instance ID.

        This method polls the list of runs to find the specific run, as the Fabric API
        doesn't provide a direct get-by-ID endpoint.

        Args:
            workspace_object_id: The Fabric workspace object ID
            pipeline_id: The pipeline artifact object ID
            run_instance_id: The artifact job instance ID

        Returns:
            PipelineRun object if found, None otherwise

        Raises:
            RuntimeError: If the API call fails
        """
        self.logger.info(f"Getting pipeline run {run_instance_id} for pipeline {pipeline_id}")

        try:
            # List all runs (we can't filter by instance ID, so we get all recent runs)
            all_statuses = [
                PipelineStatus.NOT_STARTED.value,
                PipelineStatus.IN_PROGRESS.value,
                PipelineStatus.SUCCEEDED.value,
                PipelineStatus.FAILED.value,
                PipelineStatus.CANCELLED.value,
                PipelineStatus.UNKNOWN.value,
            ]

            runs = await self.list_runs(workspace_object_id, pipeline_id, all_statuses)

            # Find the matching run
            for run in runs:
                if run.artifact_job_instance_id == run_instance_id:
                    self.logger.info(f"Found pipeline run {run_instance_id}")
                    return run

            self.logger.warning(f"Pipeline run {run_instance_id} not found")
            return None

        except Exception as e:
            error_msg = f"Failed to get pipeline run {run_instance_id}: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def wait_for_completion(
        self,
        workspace_object_id: str,
        pipeline_id: str,
        run_instance_id: str,
        poll_interval_seconds: int = 10,
        timeout_seconds: int = 3600,
    ) -> PipelineRun:
        """
        Wait for a pipeline run to complete (reach a terminal state).

        Args:
            workspace_object_id: The Fabric workspace object ID
            pipeline_id: The pipeline artifact object ID
            run_instance_id: The artifact job instance ID to wait for
            poll_interval_seconds: Seconds to wait between status checks (default: 10)
            timeout_seconds: Maximum seconds to wait before timing out (default: 3600)

        Returns:
            PipelineRun object in terminal state

        Raises:
            RuntimeError: If timeout is reached or API call fails
            ValueError: If the run is not found
        """
        self.logger.info(f"Waiting for pipeline run {run_instance_id} to complete " f"(timeout: {timeout_seconds}s, poll interval: {poll_interval_seconds}s)")

        start_time = datetime.utcnow()
        non_terminal_statuses = self.get_non_terminal_statuses()

        while True:
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            if elapsed >= timeout_seconds:
                error_msg = f"Timeout waiting for pipeline run {run_instance_id} after {timeout_seconds}s"
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)

            # Get the current run status
            run = await self.get_run(workspace_object_id, pipeline_id, run_instance_id)
            if run is None:
                raise ValueError(f"Pipeline run {run_instance_id} not found")

            # Check if in terminal state
            if str(run.status) not in ",".join(non_terminal_statuses):
                self.logger.info(f"Pipeline run {run_instance_id} completed with status: {run.status_string}")
                return run

            self.logger.debug(f"Pipeline run {run_instance_id} still running (status: {run.status_string}), " f"waiting {poll_interval_seconds}s...")
            await asyncio.sleep(poll_interval_seconds)

    def _cast_to_pipeline_run(self, data: dict[str, Any]) -> PipelineRun:
        """
        Convert API response data to PipelineRun object.

        Args:
            data: Dictionary from API response

        Returns:
            PipelineRun object
        """
        owner_user_data = data.get("ownerUser", {})
        owner_user = OwnerUser(
            id=owner_user_data.get("id"),
            name=owner_user_data.get("name"),
            object_id=owner_user_data.get("objectId"),
            user_principal_name=owner_user_data.get("userPrincipalName"),
            aad_app_id=owner_user_data.get("aadAppId"),
            type=owner_user_data.get("type"),
        )

        return PipelineRun(
            id=data.get("id"),
            artifact_job_instance_id=data.get("artifactJobInstanceId"),
            owner_user_id=data.get("ownerUserId"),
            artifact_id=data.get("artifactId"),
            tenant_object_id=data.get("tenantObjectId"),
            artifact_object_id=data.get("artifactObjectId"),
            artifact_type=data.get("artifactType"),
            artifact_name=data.get("artifactName"),
            workspace_object_id=data.get("workspaceObjectId"),
            workspace_name=data.get("workspaceName"),
            artifact_job_type=data.get("artifactJobType"),
            artifact_job_permission=data.get("artifactJobPermission"),
            artifact_job_invoke_type=data.get("artifactJobInvokeType"),
            is_successful=data.get("isSuccessful"),
            status=data.get("status"),
            root_activity_id=data.get("rootActivityId"),
            job_schedule_time_utc=data.get("jobScheduleTimeUtc"),
            job_start_time_utc=data.get("jobStartTimeUtc"),
            job_end_time_utc=data.get("jobEndTimeUtc"),
            job_next_check_time_utc=data.get("jobNextCheckTimeUtc"),
            service_exception_json=data.get("serviceExceptionJson"),
            artifact_job_history_properties=data.get("artifactJobHistoryProperties", {}),
            owner_user=owner_user,
            capacity_display_name=data.get("capacityDisplayName"),
            artifact_job_permission_string=data.get("artifactJobPermissionString"),
            artifact_job_invoke_type_string=data.get("ArtifactJobInvokeTypeString"),
            status_string=data.get("statusString"),
            triggered_by_artifact_object_id=data.get("triggeredByArtifactObjectId"),
            triggered_by_artifact_job_object_id=data.get("triggeredByArtifactJobObjectId"),
            trigger_job_count=data.get("triggerJobCount"),
            triggered_by_artifact_name=data.get("triggeredByArtifactName"),
            triggered_by_artifact_permissions=data.get("triggeredByArtifactPermissions"),
            as_azure_object_id=data.get("asAzureObjectId"),
            job_definition_object_id=data.get("jobDefinitionObjectId"),
            window_offset=data.get("windowOffset"),
            retries_count=data.get("retriesCount"),
            job_type_public_name=data.get("jobTypePublicName"),
        )

    def _cast_to_start_response(self, data: dict[str, Any]) -> StartPipelineResponse:
        """
        Convert API response data to StartPipelineResponse object.

        Args:
            data: Dictionary from API response

        Returns:
            StartPipelineResponse object
        """
        return StartPipelineResponse(
            id=data.get("id"),
            artifact_job_instance_id=data.get("artifactJobInstanceId"),
            owner_user_id=data.get("ownerUserId"),
            tenant_object_id=data.get("tenantObjectId"),
            artifact_id=data.get("artifactId"),
            artifact_object_id=data.get("artifactObjectId"),
            artifact_job_type=data.get("artifactJobType"),
            artifact_job_permission=data.get("artifactJobPermission"),
            artifact_job_permission_string=data.get("artifactJobPermissionString"),
            artifact_job_invoke_type=data.get("artifactJobInvokeType"),
            artifact_job_invoke_type_string=data.get("ArtifactJobInvokeTypeString"),
            is_successful=data.get("isSuccessful"),
            status=data.get("status"),
            status_string=data.get("statusString"),
            root_activity_id=data.get("rootActivityId"),
            job_schedule_time_utc=data.get("jobScheduleTimeUtc"),
            job_start_time_utc=data.get("jobStartTimeUtc"),
            job_end_time_utc=data.get("jobEndTimeUtc"),
            job_next_check_time_utc=data.get("jobNextCheckTimeUtc"),
            service_exception_json=data.get("serviceExceptionJson"),
            triggered_by_artifact_object_id=data.get("triggeredByArtifactObjectId"),
            triggered_by_artifact_job_object_id=data.get("triggeredByArtifactJobObjectId"),
            trigger_job_count=data.get("triggerJobCount"),
            triggered_by_artifact_name=data.get("triggeredByArtifactName"),
            triggered_by_artifact_permissions=data.get("triggeredByArtifactPermissions"),
            as_azure_object_id=data.get("asAzureObjectId"),
            job_definition_object_id=data.get("jobDefinitionObjectId"),
            concurrency=data.get("concurrency"),
            window_offset=data.get("windowOffset"),
            retries_count=data.get("retriesCount"),
        )
