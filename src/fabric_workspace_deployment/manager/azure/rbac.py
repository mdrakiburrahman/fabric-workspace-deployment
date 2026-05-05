# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import functools
import logging
import uuid

import requests

from fabric_workspace_deployment.operations.operation_interfaces import (
    AzureRbacManager,
    CommonParams,
    HttpRetryHandler,
    PrincipalType,
)

ARM_BASE_URL = "https://management.azure.com"
ARM_API_VERSION = "2022-04-01"

# Fixed private namespace for deterministic UUID v5 generation.
# Changing this value would invalidate all previously created assignment names.
_ASSIGNMENT_NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


class ArmRbacManager(AzureRbacManager):
    """
    Concrete ARM REST API implementation of AzureRbacManager.

    Uses caller-supplied Bearer tokens for every request — no az login session
    is required by this class.  All HTTP calls are wrapped by HttpRetryHandler.
    """

    def __init__(
        self,
        common_params: CommonParams,
        http_retry_handler: HttpRetryHandler,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the ARM RBAC manager.

        Args:
            common_params: Common parameters (used for subscription ID context in logs)
            http_retry_handler: Retry-capable HTTP executor
            logger: Optional logger; defaults to module logger
        """
        super().__init__(common_params)
        self.http_retry = http_retry_handler
        self.logger = logger or logging.getLogger(__name__)

    # ---------------------------------------------------------------------- #
    # Public interface
    # ---------------------------------------------------------------------- #

    def create_role_assignment(
        self,
        token: str,
        scope: str,
        role_name: str,
        object_id: str,
        principal_type: PrincipalType,
    ) -> str:
        """
        Idempotently create an ARM role assignment.

        Resolves role_name to its definition ID, checks whether the assignment
        already exists for the principal, and only issues a PUT if needed.

        Args:
            token: Bearer JWT scoped to https://management.azure.com
            scope: Full ARM resource scope
            role_name: Human-readable role name (e.g. "Storage Blob Data Reader")
            object_id: AAD object ID of the principal
            principal_type: PrincipalType enum value

        Returns:
            str: Deterministic role assignment name (UUID v5 string)

        Raises:
            ValueError: If role_name cannot be resolved
            RuntimeError: If an ARM API call fails
        """
        role_definition_id = self._resolve_role_definition_id(token, scope, role_name)
        assignment_name = self._generate_assignment_name(scope, role_definition_id, object_id)

        if self._assignment_exists(token, scope, object_id, role_definition_id):
            self.logger.info(f"Role assignment already exists — skipping: " f"role='{role_name}', object_id='{object_id}', scope='{scope}'")
            return assignment_name

        self.logger.info(f"Creating role assignment: role='{role_name}', " f"object_id='{object_id}', scope='{scope}', name='{assignment_name}'")
        self._put_role_assignment(token, scope, assignment_name, role_definition_id, object_id, principal_type)
        self.logger.info(f"Successfully created role assignment '{assignment_name}'")
        return assignment_name

    # ---------------------------------------------------------------------- #
    # Private helpers
    # ---------------------------------------------------------------------- #

    @staticmethod
    def _generate_assignment_name(scope: str, role_definition_id: str, object_id: str) -> str:
        """
        Generate a deterministic UUID v5 for a role assignment.

        The same (scope, role_definition_id, object_id) triple always produces
        the same UUID, making repeated PUT calls safely idempotent.

        Args:
            scope: ARM resource scope
            role_definition_id: Full role definition resource ID
            object_id: AAD object ID of the principal

        Returns:
            str: UUID v5 string
        """
        key = f"{scope}:{role_definition_id}:{object_id}"
        return str(uuid.uuid5(_ASSIGNMENT_NAMESPACE, key))

    @functools.cache  # noqa: B019
    def _resolve_role_definition_id(self, token: str, scope: str, role_name: str) -> str:
        """
        Resolve a human-readable role name to its ARM role definition resource ID.

        Result is cached for the lifetime of the instance so repeated calls within
        the same run do not issue redundant API requests.

        Args:
            token: Bearer JWT
            scope: ARM resource scope (used to search for role definitions in context)
            role_name: Human-readable role name

        Returns:
            str: Full role definition resource ID, e.g.
                 "/subscriptions/{sub}/providers/Microsoft.Authorization/roleDefinitions/{id}"

        Raises:
            ValueError: If no role definition is found for role_name
            RuntimeError: If the API call fails
        """
        url = f"{ARM_BASE_URL}{scope}/providers/Microsoft.Authorization/roleDefinitions"
        params = {
            "api-version": ARM_API_VERSION,
            "$filter": f"roleName eq '{role_name}'",
        }
        headers = self._auth_headers(token)

        self.logger.debug(f"Resolving role definition for '{role_name}' at scope '{scope}'")

        try:
            response = self.http_retry.execute(requests.get, url, headers=headers, params=params, timeout=60)
        except Exception as e:
            error_msg = f"Failed to resolve role definition for '{role_name}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

        data = response.json()
        definitions = data.get("value", [])

        if not definitions:
            error_msg = f"No role definition found for role name '{role_name}' at scope '{scope}'"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        role_definition_id: str = definitions[0]["id"]
        self.logger.debug(f"Resolved '{role_name}' → '{role_definition_id}'")
        return role_definition_id

    def _assignment_exists(self, token: str, scope: str, object_id: str, role_definition_id: str) -> bool:
        """
        Check whether a role assignment for the given principal already exists at scope.

        Args:
            token: Bearer JWT
            scope: ARM resource scope
            object_id: AAD object ID of the principal
            role_definition_id: Full role definition resource ID

        Returns:
            bool: True if an assignment for this (role, principal) pair exists
        """
        url = f"{ARM_BASE_URL}{scope}/providers/Microsoft.Authorization/roleAssignments"
        params = {
            "api-version": ARM_API_VERSION,
            "$filter": f"principalId eq '{object_id}'",
        }
        headers = self._auth_headers(token)

        self.logger.debug(f"Listing role assignments for principal '{object_id}' at scope '{scope}'")

        try:
            response = self.http_retry.execute(requests.get, url, headers=headers, params=params, timeout=60)
        except Exception as e:
            error_msg = f"Failed to list role assignments for principal '{object_id}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

        assignments = response.json().get("value", [])
        for assignment in assignments:
            if assignment.get("properties", {}).get("roleDefinitionId") == role_definition_id:
                return True
        return False

    def _put_role_assignment(
        self,
        token: str,
        scope: str,
        assignment_name: str,
        role_definition_id: str,
        object_id: str,
        principal_type: PrincipalType,
    ) -> None:
        """
        Issue the ARM PUT to create the role assignment.

        Args:
            token: Bearer JWT
            scope: ARM resource scope
            assignment_name: Deterministic UUID v5 string (the ARM resource name)
            role_definition_id: Full role definition resource ID
            object_id: AAD object ID of the principal
            principal_type: PrincipalType enum value

        Raises:
            RuntimeError: If the API call fails
        """
        url = f"{ARM_BASE_URL}{scope}/providers/Microsoft.Authorization/roleAssignments/{assignment_name}"
        headers = self._auth_headers(token)
        body = {
            "properties": {
                "roleDefinitionId": role_definition_id,
                "principalId": object_id,
                "principalType": principal_type.value,
            }
        }

        try:
            self.http_retry.execute(requests.put, url, headers=headers, json=body, params={"api-version": ARM_API_VERSION}, timeout=60)
        except Exception as e:
            error_msg = f"Failed to create role assignment '{assignment_name}' " f"for principal '{object_id}' at scope '{scope}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    @staticmethod
    def _auth_headers(token: str) -> dict[str, str]:
        """
        Build the Authorization + Content-Type headers for ARM API calls.

        Args:
            token: Bearer JWT

        Returns:
            dict[str, str]: Headers dict
        """
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
