# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

from fabric_workspace_deployment.operations.operation_interfaces import GitLinkManager


class FabricGitLinkManager(GitLinkManager):
    """Concrete implementation of GitLinkManager for Microsoft Fabric."""

    async def _execute(self) -> None:
        self.logger.warning("Deploy Git link operation not yet implemented.")
