# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging

import requests

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import (
    CommonParams,
    GraphClient,
    HttpRetryHandler,
)

GRAPH_API_VERSION = "v1.0"

# Microsoft Graph caps checkMemberGroups at 20 group ids per request.
CHECK_MEMBER_GROUPS_MAX_IDS = 20


class MsGraphMembershipClient(GraphClient):
    """
    Concrete Microsoft Graph implementation of GraphClient.

    Targets the principal-type agnostic /directoryObjects surface so users, groups and
    service principals are all supported by the same code path. All HTTP calls are
    wrapped by HttpRetryHandler.
    """

    def __init__(
        self,
        common_params: CommonParams,
        az_cli: AzCli,
        http_retry_handler: HttpRetryHandler,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the Microsoft Graph membership client.

        Args:
            common_params: Common parameters providing the Graph endpoint and scope
            az_cli: Azure CLI wrapper used to acquire the Graph access token
            http_retry_handler: Retry-capable HTTP executor
            logger: Optional logger; defaults to module logger
        """
        super().__init__(common_params)
        self.az_cli = az_cli
        self.http_retry = http_retry_handler
        self.logger = logger or logging.getLogger(__name__)

    # ---------------------------------------------------------------------- #
    # Public interface
    # ---------------------------------------------------------------------- #

    async def check_member_groups(self, object_id: str, group_ids: list[str]) -> set[str]:
        """
        Determine which of the supplied groups a directory object is a transitive member of.

        Args:
            object_id: The AAD object id of the principal (user, group or service principal)
            group_ids: The group object ids to test membership against

        Returns:
            set[str]: The subset of group_ids the principal is a member of, lowercased

        Raises:
            RuntimeError: If the Graph API call fails
        """
        if not group_ids:
            self.logger.debug(f"No group ids supplied for principal '{object_id}' — skipping Graph call")
            return set()

        url = f"{self.common_params.endpoint.graph.rstrip('/')}/{GRAPH_API_VERSION}/directoryObjects/{object_id}/checkMemberGroups"
        headers = self._auth_headers()

        matched: set[str] = set()
        for batch in self._chunk(group_ids, CHECK_MEMBER_GROUPS_MAX_IDS):
            self.logger.debug(f"Checking membership of principal '{object_id}' against {len(batch)} group(s)")
            matched.update(self._post_check_member_groups(url, headers, object_id, batch))

        self.logger.debug(f"Principal '{object_id}' matched {len(matched)} of {len(group_ids)} configured group(s)")
        return matched

    # ---------------------------------------------------------------------- #
    # Private helpers
    # ---------------------------------------------------------------------- #

    def _post_check_member_groups(self, url: str, headers: dict[str, str], object_id: str, group_ids: list[str]) -> set[str]:
        """
        Issue a single checkMemberGroups request for one batch of group ids.

        Args:
            url: The fully qualified checkMemberGroups URL
            headers: Request headers including authorization
            object_id: The AAD object id of the principal, used for error context
            group_ids: A batch of at most CHECK_MEMBER_GROUPS_MAX_IDS group ids

        Returns:
            set[str]: The matched group ids from this batch, lowercased

        Raises:
            RuntimeError: If the Graph API call fails or returns an unparseable body
        """
        try:
            response = self.http_retry.execute(requests.post, url, headers=headers, json={"groupIds": group_ids}, timeout=60)
        except requests.HTTPError as e:
            error_msg = f"Failed to check group membership for principal '{object_id}': {e}. Response: {self._safe_response_body(e.response)}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e
        except Exception as e:
            error_msg = f"Failed to check group membership for principal '{object_id}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

        try:
            values = response.json().get("value", [])
        except ValueError as e:
            error_msg = f"Failed to parse checkMemberGroups response for principal '{object_id}': {e}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

        return {str(value).lower() for value in values}

    def _auth_headers(self) -> dict[str, str]:
        """
        Build the Authorization + Content-Type headers for Microsoft Graph calls.

        Returns:
            dict[str, str]: Headers dict
        """
        token = self.az_cli.get_access_token(self.common_params.scope.graph)
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _chunk(values: list[str], size: int) -> list[list[str]]:
        """
        Split a list into fixed-size batches.

        Args:
            values: The values to split
            size: Maximum batch size

        Returns:
            list[list[str]]: The batches, in order
        """
        return [values[i : i + size] for i in range(0, len(values), size)]

    @staticmethod
    def _safe_response_body(response: requests.Response | None) -> str:
        """
        Extract a response body for error reporting without raising.

        Args:
            response: The failed response, if any

        Returns:
            str: The response text, or a placeholder when unavailable
        """
        if response is None:
            return "<no response>"
        try:
            return response.text
        except Exception:  # noqa: BLE001
            return "<unreadable response body>"
