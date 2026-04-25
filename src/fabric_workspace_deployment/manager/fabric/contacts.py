# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import json
import logging
import os

import requests

from fabric_workspace_deployment.client.fabric_folder import FabricFolderClient
from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.fabric.workspace import FabricWorkspaceManager
from fabric_workspace_deployment.operations.operation_interfaces import (
    AlertManager,
    AlertParams,
    CommonParams,
    ContactDetail,
    FabricWorkspaceParams,
    HttpRetryHandler,
)


class FabricAlertManager(AlertManager):
    """Concrete implementation of AlertManager for Microsoft Fabric artifact contacts."""

    def __init__(
        self,
        common_params: CommonParams,
        az_cli: AzCli,
        workspace_manager: FabricWorkspaceManager,
        folder_client: FabricFolderClient,
        http_retry_handler: HttpRetryHandler,
    ):
        """
        Initialize the Fabric alert manager.

        Args:
            common_params: Common parameters containing alert and contacts configuration
            az_cli: Azure CLI instance for authentication
            workspace_manager: Workspace manager for getting workspace IDs
            folder_client: Folder client for getting artifact information
            http_retry_handler: HTTP retry handler with exponential backoff
        """
        super().__init__(common_params)
        self.az_cli = az_cli
        self.workspace_manager = workspace_manager
        self.folder_client = folder_client
        self.http_retry = http_retry_handler
        self.logger = logging.getLogger(__name__)

    async def execute(self) -> None:
        """
        Execute alert contact deployment for all workspaces in parallel.
        """
        self.logger.info("Executing FabricAlertManager")

        tasks = []
        for workspace in self.common_params.fabric.workspaces:
            if workspace.skip_deploy:
                self.logger.info(f"Skipping alert contacts for workspace '{workspace.name}' due to skipDeploy=true")
                continue

            if workspace.alert is None:
                self.logger.info(f"No alert configuration for workspace '{workspace.name}', skipping")
                continue

            task = asyncio.create_task(
                self.reconcile(workspace),
                name=f"reconcile-alert-{workspace.name}",
            )
            tasks.append(task)

        if tasks:
            self.logger.info(f"Executing alert contact reconciliation for {len(tasks)} workspaces in parallel")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.error(f"Failed to reconcile alert contacts: {result}")
                    errors.append(str(result))

            if errors:
                error = f"Failed to reconcile alert contacts for some workspaces: {'; '.join(errors)}"
                raise RuntimeError(error)
        else:
            self.logger.info("No workspaces found for alert contact reconciliation")

        self.logger.info("Finished executing FabricAlertManager")

    async def reconcile(self, workspace_params: FabricWorkspaceParams) -> None:
        """
        Reconcile alert contacts for a single workspace.

        Algorithm:
        1. Scan artifacts_folder for in-scope artifact types
        2. For each artifact, resolve contacts from .contacts file or defaults
        3. Get workspace ID and artifact IDs from Fabric API
        4. Fire PUT requests to set contacts on each artifact

        Args:
            workspace_params: The workspace parameters including alert config
        """
        alert = workspace_params.alert
        if alert is None:
            return

        self.logger.info(f"Reconciling alert contacts for workspace: {workspace_params.name}")

        artifact_contact_plan = self._build_artifact_contact_plan(workspace_params, alert)
        if not artifact_contact_plan:
            self.logger.info(f"No in-scope artifacts found for workspace '{workspace_params.name}'")
            return

        self._log_contact_plan(workspace_params.name, artifact_contact_plan)

        workspace_info = await self.workspace_manager.get(workspace_params)
        workspace_id = workspace_info.id
        folder_collection = await self.folder_client.get_fabric_folder_collection(workspace_id)

        artifact_id_map: dict[str, str] = {}
        for artifact in folder_collection.artifacts:
            artifact_id_map[artifact.display_name] = artifact.object_id

        for artifact_name, plan_entry in artifact_contact_plan.items():
            if plan_entry["action"] == "SKIPPED":
                continue

            if artifact_name not in artifact_id_map:
                self.logger.warning(f"Artifact '{artifact_name}' not found in workspace '{workspace_params.name}' " f"(may not be deployed yet). Skipping contacts API call.")
                continue

            artifact_id = artifact_id_map[artifact_name]
            contacts_payload = plan_entry["contacts"]

            await self._set_artifact_contacts(artifact_id, contacts_payload)

        self.logger.info(f"Completed alert contact reconciliation for workspace: {workspace_params.name}")

    def _build_artifact_contact_plan(
        self,
        workspace_params: FabricWorkspaceParams,
        alert: AlertParams,
    ) -> dict[str, dict]:
        """
        Build the contact plan by scanning the artifacts folder.

        Resolution order:
        1. overrides[artifact_name] — if present, use its owners or honor skip_alert
        2. alert.default — fallback for in-scope items not in overrides

        Returns:
            dict mapping artifact_name -> { "action": "DEFAULT"|"CUSTOM"|"SKIPPED", "contacts": [...] }
        """
        root_folder = self.common_params.local.root_folder
        artifacts_folder = os.path.join(root_folder, workspace_params.template.artifacts_folder)
        contacts_dict = self.common_params.contacts or {}

        plan: dict[str, dict] = {}

        if not os.path.isdir(artifacts_folder):
            self.logger.warning(f"Artifacts folder does not exist: {artifacts_folder}")
            return plan

        for entry in sorted(os.listdir(artifacts_folder)):
            entry_path = os.path.join(artifacts_folder, entry)
            if not os.path.isdir(entry_path):
                continue

            parts = entry.rsplit(".", 1)
            if len(parts) != 2:  # noqa: PLR2004
                continue

            artifact_name, artifact_type = parts[0], parts[1]

            if artifact_type not in alert.item_types_in_scope:
                continue

            override = (alert.overrides or {}).get(artifact_name)

            if override is not None:
                if override.skip_alert:
                    plan[artifact_name] = {"action": "SKIPPED", "contacts": []}
                else:
                    resolved = self._resolve_contact_keys(override.owners or alert.default, contacts_dict)
                    plan[artifact_name] = {"action": "CUSTOM", "contacts": resolved}
            else:
                resolved = self._resolve_contact_keys(alert.default, contacts_dict)
                plan[artifact_name] = {"action": "DEFAULT", "contacts": resolved}

        return plan

    def _resolve_contact_keys(
        self,
        keys: list[str],
        contacts_dict: dict[str, ContactDetail],
    ) -> list[dict]:
        """
        Resolve contact keys to REST API payload format.

        Args:
            keys: List of contact alias keys
            contacts_dict: The contacts dictionary from common config

        Returns:
            list of contact payload dicts ready for the REST API
        """
        contacts_payload = []
        for key in keys:
            if key not in contacts_dict:
                self.logger.warning(f"Contact key '{key}' not found in contacts dictionary, skipping")
                continue

            detail = contacts_dict[key]
            contacts_payload.append(
                {
                    "contact": {
                        "displayName": detail.display_name,
                        "objectId": detail.object_id,
                        "userPrincipalName": detail.user_principal_name,
                        "isSecurityGroup": detail.is_security_group,
                        "objectType": detail.object_type,
                        "groupType": detail.group_type,
                        "aadAppId": detail.aad_app_id,
                        "emailAddress": detail.email_address,
                        "relevanceScore": detail.relevance_score,
                        "creatorObjectId": detail.creator_object_id,
                    },
                    "contactType": detail.contact_type,
                }
            )

        return contacts_payload

    def _log_contact_plan(self, workspace_name: str, plan: dict[str, dict]) -> None:
        """Log the contact assignment plan."""
        self.logger.info(f"Contact plan for workspace '{workspace_name}':")
        for artifact_name, entry in plan.items():
            action = entry["action"]
            if action == "SKIPPED":
                self.logger.info(f"  [{action}] {artifact_name}")
            else:
                contact_names = [c["contact"]["displayName"] for c in entry["contacts"]]
                self.logger.info(f"  [{action}] {artifact_name} -> {contact_names}")

    async def _set_artifact_contacts(self, artifact_id: str, contacts_payload: list[dict]) -> None:
        """
        Set contacts on a Fabric artifact via the Analysis Service REST API.

        Args:
            artifact_id: The Fabric artifact object ID
            contacts_payload: List of contact dicts in the API-expected format
        """
        url = f"{self.common_params.endpoint.analysis_service}/metadata/artifacts/{artifact_id}/contacts"
        token = self.az_cli.get_access_token(self.common_params.scope.analysis_service)

        payload = {"contacts": contacts_payload}

        self.logger.info(f"Setting contacts for artifact {artifact_id}: PUT {url}")
        self.logger.debug(f"Contacts payload: {json.dumps(payload, indent=2)}")

        self.http_retry.execute(
            requests.put,
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60,
        )

        self.logger.info(f"Successfully set contacts for artifact {artifact_id}")
