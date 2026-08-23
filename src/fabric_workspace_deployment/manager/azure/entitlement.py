# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import logging

from fabric_workspace_deployment.operations.operation_interfaces import (
    CommonParams,
    Entitlement,
    EntitlementManager,
    EntitlementMatchMode,
    EntitlementResult,
    GraphClient,
)


class AzEntitlementManager(EntitlementManager):
    """
    Concrete implementation of EntitlementManager backed by Microsoft Graph.

    Owns the policy layer only: match-mode semantics, skip rules and aggregated failure
    reporting. All directory I/O is delegated to the injected GraphClient.
    """

    def __init__(
        self,
        common_params: CommonParams,
        graph_client: GraphClient,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the entitlement manager.

        Args:
            common_params: Common parameters containing the configured entitlements
            graph_client: Directory client used to resolve group membership
            logger: Optional logger; defaults to module logger
        """
        super().__init__(common_params, logger)
        self.graph_client = graph_client

    # ---------------------------------------------------------------------- #
    # Public interface
    # ---------------------------------------------------------------------- #

    async def _execute(self) -> None:
        """
        Verify every configured entitlement, failing the run if any is not satisfied.

        Raises:
            RuntimeError: If one or more entitlements are not satisfied
        """
        entitlements = self.common_params.entitlements
        if not entitlements:
            self.logger.info("No entitlements configured — skipping entitlement checks.")
            return

        self.logger.info(f"Verifying {len(entitlements)} entitlement(s).")
        results = await self.evaluate()

        failures = [result for result in results if not result.satisfied]
        if failures:
            error_msg = self._format_failures(failures)
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        self.logger.info(f"All {len(entitlements)} entitlement(s) satisfied.")

    async def evaluate(self) -> list[EntitlementResult]:
        """
        Evaluate every configured entitlement without raising on an unsatisfied result.

        Every entitlement is evaluated even when an earlier one fails, so a single run
        surfaces the complete set of missing memberships.

        Returns:
            list[EntitlementResult]: One result per configured entitlement, in config order

        Raises:
            RuntimeError: If a directory call fails
        """
        return list(await asyncio.gather(*(self._evaluate_one(entitlement, index) for index, entitlement in enumerate(self.common_params.entitlements))))

    # ---------------------------------------------------------------------- #
    # Private helpers
    # ---------------------------------------------------------------------- #

    async def _evaluate_one(self, entitlement: Entitlement, index: int) -> EntitlementResult:
        """
        Evaluate a single entitlement against the directory.

        Args:
            entitlement: The entitlement to evaluate
            index: Index of the entitlement for log context

        Returns:
            EntitlementResult: The outcome of the evaluation
        """
        if not entitlement.group_ids:
            self.logger.info(f"entitlements[{index}]: no groupIds configured for principal '{entitlement.object_id}' — skipping.")
            return EntitlementResult(entitlement=entitlement, satisfied=True, skipped=True)

        matched = await self.graph_client.check_member_groups(entitlement.object_id, entitlement.group_ids)
        result = EntitlementResult(
            entitlement=entitlement,
            matched_group_ids=matched,
            satisfied=self._is_satisfied(entitlement, matched),
        )

        if result.satisfied:
            self.logger.info(f"entitlements[{index}]: principal '{entitlement.object_id}' satisfies match='{self._match_value(entitlement)}' across {len(entitlement.group_ids)} group(s).")
        else:
            self.logger.error(f"entitlements[{index}]: principal '{entitlement.object_id}' does not satisfy match='{self._match_value(entitlement)}'. Missing group(s): {', '.join(result.missing_group_ids)}.")

        return result

    def _is_satisfied(self, entitlement: Entitlement, matched: set[str]) -> bool:
        """
        Apply the entitlement match mode to the resolved membership set.

        Args:
            entitlement: The entitlement being evaluated
            matched: The lowercased group ids the principal is a member of

        Returns:
            bool: True if the entitlement is satisfied

        Raises:
            ValueError: If the match mode is unset or unrecognized
        """
        match entitlement.match:
            case EntitlementMatchMode.ANY:
                return len(matched) > 0
            case EntitlementMatchMode.ALL:
                return matched.issuperset({group_id.lower() for group_id in entitlement.group_ids})
            case _:
                error_msg = f"Unknown entitlement match mode for principal '{entitlement.object_id}': {entitlement.match}"
                raise ValueError(error_msg)

    def _format_failures(self, failures: list[EntitlementResult]) -> str:
        """
        Build a single aggregated error message describing every unsatisfied entitlement.

        Args:
            failures: The unsatisfied results

        Returns:
            str: The aggregated, human-readable failure message
        """
        lines = [f"{len(failures)} entitlement check(s) failed:"]
        for result in failures:
            entitlement = result.entitlement
            lines.append(f"  - {entitlement.reason}")
            lines.append(f"      objectId: {entitlement.object_id}")
            lines.append(f"      match: {self._match_value(entitlement)}")
            lines.append(f"      missing groupIds: {', '.join(result.missing_group_ids)}")
        return "\n".join(lines)

    @staticmethod
    def _match_value(entitlement: Entitlement) -> str:
        """
        Render the entitlement match mode for logging.

        Args:
            entitlement: The entitlement to render

        Returns:
            str: The match mode value
        """
        return entitlement.match.value if entitlement.match else "unset"
