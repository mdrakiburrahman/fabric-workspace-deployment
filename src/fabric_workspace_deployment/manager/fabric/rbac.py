# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio

import dacite
import requests

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.manager.fabric.cli import FabricCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    AccessSource,
    ArtifactType,
    CicdArtifactType,
    CommonParams,
    DatamartBatchResponse,
    DatamartParametersResponse,
    FabricWorkspaceFolderInfo,
    FabricWorkspaceFolderRbacDetail,
    FabricWorkspaceFolderRbacInfo,
    FabricWorkspaceItem,
    FabricWorkspaceItemRbacDetail,
    FabricWorkspaceItemRbacInfo,
    FolderClient,
    FolderRole,
    HttpRetryHandler,
    Identity,
    ItemRbacDetailParams,
    ItemRbacParams,
    MONITORING_KUSTO_DATABASE,
    MONITORING_KUSTO_EVENTHOUSE,
    PrincipalType,
    RbacManager,
    RbacParams,
    SqlEndpointSecurity,
    UniversalSecurity,
    WorkspaceManager,
    WorkspaceRbacParams,
)
from fabric_workspace_deployment.static.transformers import StringTransformer


class FabricRbacManager(RbacManager):
    """Concrete implementation of RbacManager for Microsoft Fabric."""

    def __init__(
        self,
        common_params: CommonParams,
        az_cli: AzCli,
        fabric_cli: FabricCli,
        workspace: WorkspaceManager,
        folder_client: FolderClient,
        http_retry_handler: HttpRetryHandler,
    ):
        """
        Initialize the Fabric RBAC manager.
        """
        super().__init__(common_params)
        self.az_cli = az_cli
        self.fabric_cli = fabric_cli
        self.workspace = workspace
        self.folder_client = folder_client
        self.http_retry = http_retry_handler

    async def _execute(self) -> None:
        self.logger.info("Executing FabricRbacManager")
        tasks = []
        for workspace_params in self.common_params.fabric.workspaces:
            if workspace_params.skip_deploy:
                self.logger.info(f"Skipping RBAC for workspace '{workspace_params.name}' due to skipDeploy=true")
                continue
            workspace_info = await self.workspace.get(workspace_params)
            if workspace_params.rbac is not None:
                task = asyncio.create_task(self.reconcile(workspace_info.id, workspace_params.rbac), name=f"reconcile-rbac-{workspace_params.name}")
                tasks.append(task)
            else:
                self.logger.info(f"No RBAC configuration found for workspace '{workspace_params.name}', skipping RBAC reconciliation")

        if tasks:
            self.logger.info(f"Executing RBAC reconciliation for {len(tasks)} workspaces in parallel")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    workspace_name = self.common_params.fabric.workspaces[i].name
                    error_msg = f"Failed to reconcile RBAC for workspace '{workspace_name}': {result}"
                    self.logger.error(error_msg)
                    errors.append(error_msg)

            if errors:
                error = f"Failed to reconcile RBAC for some workspaces: {'; '.join(errors)}"
                raise Exception(error)
        else:
            self.logger.info("No workspaces found to reconcile RBAC")

        self.logger.info("Finished executing FabricRbacManager")

    async def _resolve_semantic_model_ids(self, workspace_id: str, workspace_items: list[FabricWorkspaceItem]) -> dict[str, int]:
        semantic_model_object_ids = {item.id for item in workspace_items if item.type == CicdArtifactType.SEMANTIC_MODEL.value}
        if not semantic_model_object_ids:
            return {}

        folder_collection = await self.folder_client.get_fabric_folder_collection(workspace_id)
        return {artifact.object_id: artifact.id for artifact in folder_collection.artifacts if artifact.type_name == ArtifactType.MODEL.value and artifact.object_id in semantic_model_object_ids}

    async def reconcile(self, workspace_id: str, rbac_params: RbacParams) -> None:
        """
        Reconcile all RBAC for a single workspace.

        The try-catch is because when workspace permissions are updated, Fabric
        automatically but non-deterministically propagates removal of items (some, but not all),
        so since they are non-deterministic, it leads to a race condition with us.

        By retrying with no state (refresh via GET), we can eventually converge
        to the desired state.
        """
        max_retries = 5
        for attempt in range(max_retries):
            try:
                workspace_folder_info = await self.get_fabric_workspace_folder_info(workspace_id)
                workspace_items = await self.get_fabric_workspace_item_info(workspace_id)
                workspace_rbac_info = await self.get_fabric_workspace_folder_rbac_info(workspace_folder_info.id)
                semantic_model_ids = await self._resolve_semantic_model_ids(workspace_id, workspace_items)

                item_rbac_infos = []
                for workspace_item in workspace_items:
                    items_not_scanned = {
                        CicdArtifactType.EVENTHOUSE.value: MONITORING_KUSTO_EVENTHOUSE,
                        CicdArtifactType.KQL_DATABASE.value: MONITORING_KUSTO_DATABASE,
                    }

                    if workspace_item.type in items_not_scanned and workspace_item.display_name == items_not_scanned[workspace_item.type]:
                        self.logger.info(f"Skipping RBAC reconciliation for monitoring item '{workspace_item.display_name}' with ID {workspace_item.id}")
                        continue

                    if workspace_item.type == CicdArtifactType.SEMANTIC_MODEL.value:
                        model_id = semantic_model_ids.get(workspace_item.id)
                        if model_id is None:
                            raise RuntimeError(f"Failed to resolve internal model id for SemanticModel '{workspace_item.display_name}' " f"with object id '{workspace_item.id}' in workspace '{workspace_id}'")
                        item_rbac_infos.append(await self.get_fabric_model_rbac_info(model_id))
                    elif workspace_item.type in [
                        CicdArtifactType.APP_BACKEND.value,
                        CicdArtifactType.KQL_DATABASE.value,
                        CicdArtifactType.EVENTHOUSE.value,
                        CicdArtifactType.SQL_ENDPOINT.value,
                        CicdArtifactType.LAKEHOUSE.value,
                    ]:
                        item_rbac_infos.append(await self.get_fabric_workspace_item_rbac_info(workspace_item.id, workspace_item.type))
                    else:
                        self.logger.warning(f"Skipping RBAC reconciliation for unsupported item type '{workspace_item.type}' with ID {workspace_item.id}")

                await self._reconcile_role_assignments(rbac_params, workspace_rbac_info, item_rbac_infos)
                await self._reconcile_one_security(rbac_params.universal_security, workspace_items)

                break
            except Exception as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"Failed to reconcile role assignments after {max_retries} attempts: {e}")
                    raise

                delay = min(2**attempt, 15)
                self.logger.warning(f"Attempt {attempt + 1}/{max_retries} failed to reconcile role assignments: {e}. " f"Retrying in {delay} seconds.")
                await asyncio.sleep(delay)

    async def get_fabric_workspace_folder_info(self, workspace_id: str) -> FabricWorkspaceFolderInfo:
        self.logger.info(f"Getting Fabric workspace folder info for folder {workspace_id}")
        try:
            response = self.http_retry.execute(
                requests.get,
                f"{self.common_params.endpoint.analysis_service}/metadata/folders/{workspace_id}",
                headers={
                    "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                    "Content-Type": "application/json",
                },
                timeout=60,
            )
            folder_data = response.json()
            folder_data_snake_case = StringTransformer.convert_keys_to_snake_case(folder_data)
            fabric_folder_info = dacite.from_dict(
                data_class=FabricWorkspaceFolderInfo,
                data=folder_data_snake_case,
                config=dacite.Config(
                    check_types=False,
                    cast=[str, int, bool, float],
                ),
            )

            self.logger.info(f"Successfully retrieved Fabric workspace folder info for folder {workspace_id}")
            return fabric_folder_info

        except Exception as e:
            error_msg = f"Failed to get Fabric workspace folder info for folder '{workspace_id}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def get_fabric_workspace_item_info(self, workspace_id: str) -> list[FabricWorkspaceItem]:
        self.logger.info(f"Getting Fabric workspace items for workspace {workspace_id}")
        try:
            response = self.http_retry.execute(
                requests.get,
                f"{self.common_params.endpoint.power_bi}/v1/workspaces/{workspace_id}/items",
                headers={
                    "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                    "Content-Type": "application/json",
                },
                timeout=60,
            )
            items_data = response.json()
            self.logger.debug(f"Current State Workspace Items RBAC for {workspace_id}: {items_data}")
            items_list = items_data.get("value", [])
            fabric_items = []
            for item_data in items_list:
                item_data_snake_case = StringTransformer.convert_keys_to_snake_case(item_data)
                fabric_item = dacite.from_dict(
                    data_class=FabricWorkspaceItem,
                    data=item_data_snake_case,
                    config=dacite.Config(
                        check_types=False,
                        cast=[str, int, bool, float],
                    ),
                )
                fabric_items.append(fabric_item)

            self.logger.info(f"Successfully retrieved {len(fabric_items)} items for workspace {workspace_id}")
            return fabric_items

        except Exception as e:
            error_msg = f"Failed to get Fabric workspace items for workspace '{workspace_id}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def get_fabric_workspace_folder_rbac_info(self, folder_id: int) -> FabricWorkspaceFolderRbacInfo:
        self.logger.info(f"Getting Fabric workspace folder RBAC info for workspace folder {folder_id}")
        try:
            response = self.http_retry.execute(
                requests.get,
                f"{self.common_params.endpoint.analysis_service}/metadata/access/folders/{folder_id}",
                headers={
                    "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                    "Content-Type": "application/json",
                },
                timeout=60,
            )
            rbac_data = response.json()
            self.logger.debug(f"Current State Workspace RBAC for {folder_id}: {rbac_data}")
            rbac_data_snake_case = StringTransformer.convert_keys_to_snake_case(rbac_data)
            detail_items = []
            for detail_data in rbac_data_snake_case.get("detail", []):
                detail_item = dacite.from_dict(
                    data_class=FabricWorkspaceFolderRbacDetail,
                    data=detail_data,
                    config=dacite.Config(
                        check_types=False,
                        cast=[str, int, bool, float],
                    ),
                )
                detail_items.append(detail_item)
            rbac_data_snake_case["detail"] = detail_items
            fabric_rbac_info = dacite.from_dict(
                data_class=FabricWorkspaceFolderRbacInfo,
                data=rbac_data_snake_case,
                config=dacite.Config(
                    check_types=False,
                    cast=[str, int, bool, float],
                ),
            )
            self.logger.info(f"Successfully retrieved Fabric workspace folder RBAC info for workspace folder {folder_id}")
            return fabric_rbac_info

        except Exception as e:
            error_msg = f"Failed to get Fabric workspace folder RBAC info for workspace folder '{folder_id}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def _parse_item_rbac_info(self, rbac_data: dict, item_type: str) -> FabricWorkspaceItemRbacInfo:
        rbac_data_snake_case = StringTransformer.convert_keys_to_snake_case(rbac_data)
        detail_items = []
        for detail_data in rbac_data_snake_case.get("detail", []):
            if "access_source" in detail_data and detail_data["access_source"] is not None:
                access_source_data = detail_data["access_source"]
                if "folder_role" in access_source_data and access_source_data["folder_role"] is not None:
                    folder_role_data = access_source_data["folder_role"]
                    folder_role = dacite.from_dict(
                        data_class=FolderRole,
                        data=folder_role_data,
                        config=dacite.Config(
                            check_types=False,
                            cast=[str, int, bool, float],
                        ),
                    )
                    access_source_data["folder_role"] = folder_role
                access_source = dacite.from_dict(
                    data_class=AccessSource,
                    data=access_source_data,
                    config=dacite.Config(
                        check_types=False,
                        cast=[str, int, bool, float],
                    ),
                )
                detail_data["access_source"] = access_source

            detail_items.append(
                dacite.from_dict(
                    data_class=FabricWorkspaceItemRbacDetail,
                    data=detail_data,
                    config=dacite.Config(
                        check_types=False,
                        cast=[str, int, bool, float],
                    ),
                )
            )

        rbac_data_snake_case["detail"] = detail_items
        rbac_data_snake_case["type"] = item_type
        return dacite.from_dict(
            data_class=FabricWorkspaceItemRbacInfo,
            data=rbac_data_snake_case,
            config=dacite.Config(
                check_types=False,
                cast=[str, int, bool, float],
            ),
        )

    async def get_fabric_workspace_item_rbac_info(self, item_id: str, item_type: str) -> FabricWorkspaceItemRbacInfo:
        self.logger.info(f"Getting Fabric workspace item RBAC info for {item_type}: {item_id}")
        try:
            response = self.http_retry.execute(
                requests.get,
                f"{self.common_params.endpoint.analysis_service}/metadata/access/artifacts/{item_id}",
                headers={
                    "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                    "Content-Type": "application/json",
                },
                params={"includeRestrictedUsers": "true"},
                timeout=60,
            )
            rbac_data = response.json()
            self.logger.debug(rbac_data)
            fabric_rbac_info = self._parse_item_rbac_info(rbac_data, item_type)
            self.logger.info(f"Successfully retrieved Fabric workspace item RBAC info for item {item_id}")
            return fabric_rbac_info

        except Exception as e:
            error_msg = f"Failed to get Fabric workspace item RBAC info for item '{item_id}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def get_fabric_model_rbac_info(self, model_id: int) -> FabricWorkspaceItemRbacInfo:
        self.logger.info(f"Getting SemanticModel RBAC info for internal model id {model_id}")
        access_token = self.az_cli.get_access_token(self.common_params.scope.analysis_service)
        try:
            response = self.http_retry.execute(
                requests.get,
                f"{self.common_params.endpoint.analysis_service}/metadata/access/models/{model_id}",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                params={"includeRestrictedUsers": "true"},
                timeout=60,
            )
            rbac_data = response.json()
            self.logger.debug(rbac_data)
            fabric_rbac_info = self._parse_item_rbac_info(rbac_data, CicdArtifactType.SEMANTIC_MODEL.value)
            self.logger.info(f"Successfully retrieved SemanticModel RBAC info for internal model id {model_id}")
            return fabric_rbac_info

        except Exception as e:
            error_msg = f"Failed to get SemanticModel RBAC info for internal model id '{model_id}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def update_item_role_assignment(self, item_id: str, assignment: ItemRbacDetailParams, identity: Identity) -> None:
        is_service_principal = identity.principal_type == PrincipalType.SERVICE_PRINCIPAL
        is_group = identity.principal_type == PrincipalType.GROUP

        artifact_data = {
            "artifactObjectId": item_id,
            "permissions": assignment.permissions,
            "isServicePrincipal": is_service_principal,
        }

        if assignment.artifact_permissions is not None:
            artifact_data["artifactPermissions"] = assignment.artifact_permissions

        if is_group:
            artifact_data["groupObjectId"] = assignment.object_id
            artifact_data["userId"] = None
            artifact_data["groupId"] = None
            artifact_data["userObjectId"] = None
        else:
            artifact_data["userObjectId"] = assignment.object_id
            artifact_data["userId"] = None
            artifact_data["groupId"] = None
            artifact_data["groupObjectId"] = None

        payload = {
            "dashboards": [],
            "reports": [],
            "workbooks": [],
            "models": [],
            "datamarts": [],
            "artifacts": [artifact_data],
        }

        self.logger.info(f"Updating item role assignment for {identity.given_name} ({assignment.object_id}) " f"on item {item_id} with permissions {assignment.permissions}, " f"artifactPermissions {assignment.artifact_permissions}")
        self.logger.debug(payload)

        try:
            response = self.http_retry.execute(
                requests.put,
                f"{self.common_params.endpoint.analysis_service}/metadata/access",
                headers={
                    "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=60,
            )
            self.logger.info(f"Successfully updated item role assignment for {identity.given_name} ({assignment.object_id}) on item {item_id}")

        except Exception as e:
            error_msg = f"Failed to update item role assignment for {identity.given_name} ({assignment.object_id}) on item {item_id}: {e}"  # noqa: E501
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def update_model_role_assignment(self, model_id: int, assignment: ItemRbacDetailParams, identity: Identity) -> None:
        is_service_principal = identity.principal_type == PrincipalType.SERVICE_PRINCIPAL
        is_group = identity.principal_type == PrincipalType.GROUP
        access_token = self.az_cli.get_access_token(self.common_params.scope.analysis_service)

        model_data = {
            "id": model_id,
            "permissions": assignment.permissions,
            "isServicePrincipal": is_service_principal,
            "userId": None,
            "groupId": None,
            "userObjectId": None if is_group else assignment.object_id,
            "groupObjectId": assignment.object_id if is_group else None,
        }
        payload = {
            "dashboards": [],
            "reports": [],
            "workbooks": [],
            "models": [model_data],
            "datamarts": [],
            "artifacts": [],
        }

        self.logger.info(f"Updating SemanticModel role assignment for {identity.given_name} ({assignment.object_id}) " f"on internal model id {model_id} with permissions {assignment.permissions}")
        self.logger.debug(payload)

        try:
            self.http_retry.execute(
                requests.put,
                f"{self.common_params.endpoint.analysis_service}/metadata/access",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=60,
            )
            self.logger.info(f"Successfully updated SemanticModel role assignment for {identity.given_name} " f"({assignment.object_id}) on internal model id {model_id}")

        except Exception as e:
            error_msg = f"Failed to update SemanticModel role assignment for {identity.given_name} " f"({assignment.object_id}) on internal model id {model_id}: {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    async def update_workspace_role_assignment(self, folder_id: int, assignment: WorkspaceRbacParams, identity: Identity) -> None:
        is_service_principal = identity.principal_type == PrincipalType.SERVICE_PRINCIPAL
        is_group = identity.principal_type == PrincipalType.GROUP

        if assignment.permissions == 0:
            folder_data = {"id": folder_id, "permissions": assignment.permissions}
            if is_group:
                folder_data["groupId"] = identity.fabric_principal_id
            else:
                folder_data["userId"] = identity.fabric_principal_id
            payload = {"folders": [folder_data]}
            action = "REMOVING"

        else:
            folder_data = {
                "id": folder_id,
                "permissions": assignment.permissions,
                "isServicePrincipal": is_service_principal,
            }
            if is_group:
                folder_data["groupObjectId"] = assignment.object_id
            else:
                folder_data["userObjectId"] = assignment.object_id
            payload = {"folders": [folder_data]}
            action = "ADDING/UPDATING"

        self.logger.info(f"{action} role assignment for {identity.given_name} ({assignment.object_id}) with permissions {assignment.permissions}")  # noqa: E501
        self.logger.debug(payload)

        try:
            response = self.http_retry.execute(
                requests.put,
                f"{self.common_params.endpoint.analysis_service}/metadata/access/folders/{folder_id}",
                headers={
                    "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=60,
            )

            self.logger.info(f"Successfully updated role assignment for {identity.given_name} ({assignment.object_id})")

        except Exception as e:
            error_msg = f"Failed to update role assignment for {identity.given_name} ({assignment.object_id}): {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    # ---------------------------------------------------------------------------- #

    async def _reconcile_role_assignments(
        self,
        desired_state: RbacParams,
        current_state: FabricWorkspaceFolderRbacInfo,
        workspace_items: list[FabricWorkspaceItemRbacInfo],
    ) -> None:
        self.logger.info(f"Reconciling role assignments for workspace folder {current_state.id}")
        await self._reconcile_workspace_level_permissions(desired_state, current_state)
        if desired_state.items:
            workspace_role_object_ids = {detail.object_id for detail in current_state.detail}
            await self._reconcile_item_level_permissions(desired_state.items, workspace_items, desired_state, workspace_role_object_ids)
        else:
            self.logger.info("No item-level RBAC configuration found, skipping item reconciliation")

        self.logger.info("Completed role assignment reconciliation")

    async def _reconcile_one_security(self, universal_security: "UniversalSecurity", workspace_items: list[FabricWorkspaceItem]) -> None:
        """
        Reconcile Universal/One Security for supported item types.
        """
        sql_endpoints = [item for item in workspace_items if item.type == "SQLEndpoint"]

        if not sql_endpoints:
            self.logger.info("No SQLEndpoint items found for universal security reconciliation")
            return

        skip_list = universal_security.sql_endpoint.skip_reconcile
        filtered_sql_endpoints = []

        for sql_item in sql_endpoints:
            if sql_item.display_name in skip_list:
                self.logger.info(f"Skipping universal security reconciliation for SQL endpoint '{sql_item.display_name}' " f"as it is in the skipReconcile list")
            else:
                filtered_sql_endpoints.append(sql_item)

        if not filtered_sql_endpoints:
            self.logger.info("No SQL endpoints to reconcile after applying skipReconcile filter")
            return

        tasks = []
        for sql_item in filtered_sql_endpoints:
            tasks.append(asyncio.create_task(self._reconcile_sql_endpoint_universal_security(sql_item, universal_security.sql_endpoint), name=f"reconcile-universal-security-{sql_item.id}"))

        if tasks:
            self.logger.info(f"Executing universal security reconciliation for {len(tasks)} SQL endpoints in parallel")
            await asyncio.gather(*tasks)

    async def _reconcile_workspace_level_permissions(self, desired_state: RbacParams, current_state: FabricWorkspaceFolderRbacInfo) -> None:
        """
        Reconcile workspace-level role assignments.
        """
        self.logger.info("Reconciling workspace-level permissions")
        desired_assignments = {rbac.object_id: rbac for rbac in desired_state.workspace}
        current_assignments = {detail.object_id: detail for detail in current_state.detail}
        assignments_to_add = []
        assignments_to_update = []
        assignments_to_remove = []

        for object_id, desired_rbac in desired_assignments.items():
            identity = desired_state.get_identity_by_object_id(object_id, self.common_params.identities)
            if object_id in current_assignments:
                current_detail = current_assignments[object_id]
                if current_detail.permissions != desired_rbac.permissions:
                    assignments_to_update.append((desired_rbac, identity))
                    self.logger.info(f"Workspace permissions update needed for {identity.given_name} ({object_id}): " f"{current_detail.permissions} -> {desired_rbac.permissions}")
            else:
                assignments_to_add.append((desired_rbac, identity))
                self.logger.info(f"ADDING new workspace assignment for {identity.given_name} ({object_id}) with permissions {desired_rbac.permissions}")  # noqa: E501

        if desired_state.purge_unmatched_role_assignments:
            for object_id, current_detail in current_assignments.items():
                if object_id not in desired_assignments:
                    if current_detail.group_id:
                        principal_type = PrincipalType.GROUP
                        fabric_principal_id = current_detail.group_id
                    elif current_detail.aad_app_id:
                        principal_type = PrincipalType.SERVICE_PRINCIPAL
                        fabric_principal_id = current_detail.user_id
                    else:
                        principal_type = PrincipalType.USER
                        fabric_principal_id = current_detail.user_id
                    removal_assignment = WorkspaceRbacParams(
                        permissions=0,
                        object_id=current_detail.object_id,
                        purpose="REMOVING unmatched role assignment",
                    )
                    removal_identity = Identity(
                        given_name=current_detail.given_name or f"Unknown-{current_detail.object_id}",
                        object_id=current_detail.object_id,
                        principal_type=principal_type,
                        aad_app_id=current_detail.aad_app_id,
                        fabric_principal_id=fabric_principal_id,
                    )
                    assignments_to_remove.append((removal_assignment, removal_identity))
                    self.logger.warning(f"REMOVING workspace assignment for {removal_identity.given_name} ({object_id})")

        all_workspace_changes = assignments_to_add + assignments_to_update + assignments_to_remove

        if not all_workspace_changes:
            self.logger.info("No workspace-level role assignment changes needed")
        else:
            self.logger.info(f"Executing {len(all_workspace_changes)} workspace-level role assignment changes")
            for assignment, identity in all_workspace_changes:
                await self.update_workspace_role_assignment(current_state.id, assignment, identity)

    async def _reconcile_item_level_permissions(
        self,
        desired_items: list[ItemRbacParams],
        workspace_items: list[FabricWorkspaceItemRbacInfo],
        rbac_params: RbacParams,
        workspace_role_object_ids: set[str] | None = None,
    ) -> None:
        """
        Reconcile item-level role assignments.
        """
        self.logger.info("Reconciling item-level permissions")
        workspace_items_lookup = {(item.type, item.display_name): item for item in workspace_items}
        workspace_role_object_ids = workspace_role_object_ids or set()

        for desired_item in desired_items:
            item_key = (desired_item.type, desired_item.display_name)

            if item_key in workspace_items_lookup:
                current_item = workspace_items_lookup[item_key]
                self.logger.info(f"Reconciling item: {desired_item.type} - {desired_item.display_name} (ID: {current_item.id})")

                await self._reconcile_single_item_permissions(desired_item, current_item, rbac_params, workspace_role_object_ids)
            else:
                self.logger.warning(f"Desired item not found in workspace: {desired_item.type} - {desired_item.display_name}. " f"Available items: {list(workspace_items_lookup.keys())}")

    async def _reconcile_single_item_permissions(
        self,
        desired_item: ItemRbacParams,
        current_item: FabricWorkspaceItemRbacInfo,
        rbac_params: RbacParams,
        workspace_role_object_ids: set[str] | None = None,
    ) -> None:
        """
        Reconcile permissions for a single workspace item.
        """
        self.logger.info(f"Reconciling permissions for item {desired_item.display_name} (ID: {current_item.id})")
        workspace_role_object_ids = workspace_role_object_ids or set()

        current_permissions_by_object_id: dict[str, list[FabricWorkspaceItemRbacDetail]] = {}
        for current_detail in current_item.detail:
            if current_detail.object_id not in current_permissions_by_object_id:
                current_permissions_by_object_id[current_detail.object_id] = []
            current_permissions_by_object_id[current_detail.object_id].append(current_detail)

        desired_permissions_by_object_id: dict[str, list[ItemRbacDetailParams]] = {}
        for desired_detail in desired_item.detail:
            if desired_detail.object_id not in desired_permissions_by_object_id:
                desired_permissions_by_object_id[desired_detail.object_id] = []
            desired_permissions_by_object_id[desired_detail.object_id].append(desired_detail)

        item_assignments_to_add: list[tuple[ItemRbacDetailParams, Identity]] = []
        item_assignments_to_update: list[tuple[ItemRbacDetailParams, Identity]] = []
        item_assignments_to_remove: list[tuple[ItemRbacDetailParams, Identity]] = []

        for object_id, desired_details in desired_permissions_by_object_id.items():
            current_details = current_permissions_by_object_id.get(object_id, [])

            identity = rbac_params.get_identity_by_object_id(object_id, self.common_params.identities)
            for desired_detail in desired_details:
                matching_current = None
                for current_detail in current_details:
                    if current_detail.permissions == desired_detail.permissions and self._artifact_permissions_match(current_detail.artifact_permissions, desired_detail.artifact_permissions):
                        matching_current = current_detail
                        break

                if matching_current is None:
                    if current_details:
                        item_assignments_to_update.append((desired_detail, identity))
                        self.logger.info(f"UPDATING item assignment for {identity.given_name} ({desired_detail.object_id}) " f"on {desired_item.display_name} to permissions {desired_detail.permissions}, " f"artifactPermissions {desired_detail.artifact_permissions}")
                    else:
                        item_assignments_to_add.append((desired_detail, identity))
                        self.logger.info(f"ADDING new item assignment for {identity.given_name} ({desired_detail.object_id}) " f"on {desired_item.display_name} with permissions {desired_detail.permissions}, " f"artifactPermissions {desired_detail.artifact_permissions}")

        if rbac_params.purge_unmatched_role_assignments:
            scheduled_removals: set[str] = set()
            for object_id, current_details in current_permissions_by_object_id.items():
                if object_id in desired_permissions_by_object_id:
                    continue

                for current_detail in current_details:
                    if self._skip_unmatched_item_removal(desired_item.type, current_detail, workspace_role_object_ids):
                        continue
                    if object_id in scheduled_removals:
                        continue

                    if current_detail.group_id:
                        principal_type = PrincipalType.GROUP
                    elif current_detail.aad_app_id:
                        principal_type = PrincipalType.SERVICE_PRINCIPAL
                    else:
                        principal_type = PrincipalType.USER
                    removal_assignment = ItemRbacDetailParams(
                        permissions=0,
                        artifact_permissions=0,
                        object_id=current_detail.object_id,
                        purpose="REMOVING unmatched item permission entry",
                    )
                    removal_identity = Identity(
                        given_name=current_detail.given_name or f"Unknown-{current_detail.object_id}",
                        object_id=current_detail.object_id,
                        principal_type=principal_type,
                        aad_app_id=current_detail.aad_app_id,
                    )
                    item_assignments_to_remove.append((removal_assignment, removal_identity))
                    scheduled_removals.add(object_id)
                    self.logger.warning(f"REMOVING item assignment for {removal_identity.given_name} ({current_detail.object_id}) " f"on {desired_item.display_name}: permissions {current_detail.permissions} -> 0, " f"artifactPermissions {current_detail.artifact_permissions} -> 0")

        all_item_changes = item_assignments_to_add + item_assignments_to_update + item_assignments_to_remove

        if not all_item_changes:
            self.logger.info(f"No item-level role assignment changes needed for {desired_item.display_name}")
        else:
            self.logger.info(f"Executing {len(all_item_changes)} item-level role assignment changes for {desired_item.display_name}")
            for assignment, identity in all_item_changes:
                if desired_item.type == CicdArtifactType.SEMANTIC_MODEL.value:
                    await self.update_model_role_assignment(current_item.id, assignment, identity)
                else:
                    await self.update_item_role_assignment(current_item.object_id, assignment, identity)

    def _skip_unmatched_item_removal(
        self,
        item_type: str,
        current_detail: FabricWorkspaceItemRbacDetail,
        workspace_role_object_ids: set[str],
    ) -> bool:
        if item_type == CicdArtifactType.APP_BACKEND.value and current_detail.access_source is not None:
            self.logger.info(f"Skipping removal of inherited AppBackend assignment for {current_detail.given_name} " f"({current_detail.object_id}) because it has an accessSource")
            return True

        if item_type == CicdArtifactType.SEMANTIC_MODEL.value and current_detail.object_id in workspace_role_object_ids:
            self.logger.info(f"Skipping removal of inherited SemanticModel assignment for {current_detail.given_name} " f"({current_detail.object_id}) because the principal has a workspace role")
            return True

        return False

    def _artifact_permissions_match(self, current_artifact_permissions: int | None, desired_artifact_permissions: int | None) -> bool:
        current_val = current_artifact_permissions if current_artifact_permissions is not None else 0
        desired_val = desired_artifact_permissions if desired_artifact_permissions is not None else 0
        return current_val == desired_val

    async def _get_sql_ep_parameter(self, sql_ep_id: str, parameter_name: str) -> DatamartParametersResponse:
        """
        Retrieve a specific datamart parameter for a SQL endpoint.
        """
        params_url = f"{self.common_params.endpoint.analysis_service}/v1.0/myorg/lhdatamarts/{sql_ep_id}/parameters"
        resp = self.http_retry.execute(
            requests.get,
            params_url,
            headers={
                "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                "Content-Type": "application/json",
            },
            params={"name": parameter_name},
            timeout=60,
        )
        raw = resp.json()
        self.logger.debug(f"Datamart parameters GET raw response for {sql_ep_id}: {raw}")

        return dacite.from_dict(
            data_class=DatamartParametersResponse,
            data=StringTransformer.convert_keys_to_snake_case(raw),
            config=dacite.Config(check_types=False, cast=[str, int, bool, float]),
        )

    async def _set_sql_endpoint_security(self, sql_ep_id: str, desired_enabled: bool) -> None:
        """
        Set enableUniversalSecurityMode and poll until terminal state is reached.
        """

        post_url = f"{self.common_params.endpoint.analysis_service}/v1.0/myorg/lhdatamarts/{sql_ep_id}"
        post_resp = self.http_retry.execute(
            requests.post,
            post_url,
            headers={
                "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                "Content-Type": "application/json",
            },
            json={
                "commands": [
                    {
                        "$type": "ChangeUniversalSecurityModeCommand",
                        "enableUniversalSecurityMode": desired_enabled,
                    }
                ]
            },
            timeout=60,
        )
        post_raw = post_resp.json()
        self.logger.debug(f"Datamart changeUniversalSecurityMode POST raw response for {sql_ep_id}: {post_raw}")

        batch_resp = dacite.from_dict(
            data_class=DatamartBatchResponse,
            data=StringTransformer.convert_keys_to_snake_case(post_raw),
            config=dacite.Config(check_types=False, cast=[str, int, bool, float]),
        )

        batch_id = batch_resp.batch_id
        if not batch_id:
            error_msg = f"Received empty batch id when changing universal security for {sql_ep_id}: {post_raw}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        batch_url = f"{self.common_params.endpoint.analysis_service}/v1.0/myorg/lhdatamarts/{sql_ep_id}/batches/{batch_id}"
        while True:
            poll_resp = self.http_retry.execute(
                requests.get,
                batch_url,
                headers={
                    "Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}",
                    "Content-Type": "application/json",
                },
                timeout=60,
            )
            poll_raw = poll_resp.json()
            self.logger.debug(f"Datamart batch poll raw response for {sql_ep_id} batch {batch_id}: {poll_raw}")

            poll_batch = dacite.from_dict(
                data_class=DatamartBatchResponse,
                data=StringTransformer.convert_keys_to_snake_case(poll_raw),
                config=dacite.Config(check_types=False, cast=[str, int, bool, float]),
            )

            if poll_batch.progress_state == "success":
                self.logger.info(f"Datamart batch completed successfully for {sql_ep_id}")
                return
            elif poll_batch.progress_state == "inProgress":
                await asyncio.sleep(5)
                continue
            else:
                error = f"Datamart batch failed or in unexpected state for {sql_ep_id} batch {batch_id}: {poll_raw}"
                self.logger.error(error)
                raise RuntimeError(error)

    def _parse_sql_endpoint_security(self, resp: DatamartParametersResponse) -> bool:
        us_param = None
        for p in resp.parameters:
            if p.name == "UniversalSecurityMode":
                us_param = p
                break

        if us_param is None:
            error = f"UniversalSecurityMode parameter not found in datamart parameters response: {resp}"
            self.logger.error(error)
            raise RuntimeError(error)

        return us_param.value.lower() == "true"

    async def _reconcile_sql_endpoint_universal_security(self, sql_item: "FabricWorkspaceItem", security: SqlEndpointSecurity) -> None:
        self.logger.info(f"Reconciling universal security for SqlEndpoint {sql_item.display_name} (ID: {sql_item.id})")

        try:
            if self._parse_sql_endpoint_security(await self._get_sql_ep_parameter(sql_item.id, "UniversalSecurityMode")) == security.enabled:
                success = f"Universal security already set to {security.enabled} for SqlEndpoint {sql_item.id}"
                self.logger.info(success)
                return
            else:
                msg = f"Universal security needs to be set to {security.enabled} for SqlEndpoint {sql_item.id}"
                self.logger.info(msg)
                await self._set_sql_endpoint_security(sql_item.id, security.enabled)

            if self._parse_sql_endpoint_security(await self._get_sql_ep_parameter(sql_item.id, "UniversalSecurityMode")) == security.enabled:
                success = f"Universal security successfully set to {security.enabled} for SqlEndpoint {sql_item.id}"
                self.logger.info(success)
                return
            else:
                error = f"Desired universal security state not reached even after successful batch for {sql_item.id}, something went wrong and the GET is out of sync"
                self.logger.error(error)
                raise RuntimeError(error)

        except Exception as e:
            error_msg = f"An error occurred while reconciling universal security for SqlEndpoint {sql_item.id}: {e}"
            self.logger.error(error_msg)
            raise
