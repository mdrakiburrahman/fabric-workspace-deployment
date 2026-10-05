# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

from fabric_workspace_deployment.client.fabric_rest import response_guid, verify_reconciliation
from fabric_workspace_deployment.operations.operation_interfaces import CommonParams, GatewayClient, GatewayConnection, GatewayManager, GatewayParams, GatewayRole, GatewayUser, PrincipalType


class FabricGatewayManager(GatewayManager):
    """Authoritative reconciliation of declared, existing connections."""

    def __init__(self, common_params: CommonParams, gateway_client: GatewayClient):
        super().__init__(common_params)
        self.gateway_client = gateway_client

    async def _execute(self) -> None:
        for index, gateway in enumerate(self.common_params.fabric.gateways):
            self.logger.info("Reconciling common.fabric.gateways[%d]", index)
            await self.reconcile(gateway)

    def _desired_users(self, gateway: GatewayParams) -> dict[tuple[str, str], GatewayUser]:
        desired = {}
        object_ids: set[str] = set()
        for index, assignment in enumerate(gateway.users):
            identity = self.common_params.get_identity_by_given_name(assignment.identity)
            object_id = response_guid(identity.object_id, f"Gateway users[{index}] identity objectId")
            if object_id.casefold() in object_ids:
                raise ValueError("Gateway desired access contains duplicate identity object IDs")
            object_ids.add(object_id.casefold())
            if identity.principal_type == PrincipalType.GROUP:
                identifier = object_id
            elif identity.principal_type == PrincipalType.USER and identity.user_principal_name and "@" in identity.user_principal_name:
                identifier = identity.user_principal_name
            else:
                raise ValueError("Gateway access requires Group or User identities with valid User metadata")
            if assignment.role not in (GatewayRole.OWNER, GatewayRole.USER) or assignment.datasource_access_right != "Read":
                raise ValueError("Gateway access supports only Owner/User roles with Read")
            user = GatewayUser(identifier, identity.principal_type.value, assignment.role.value, assignment.datasource_access_right, object_id)
            if user.key in desired:
                raise ValueError("Gateway desired access contains duplicate principal identifiers")
            desired[user.key] = user
        if not any(user.role == GatewayRole.OWNER.value for user in desired.values()):
            raise ValueError("Gateway desired access must retain at least one Owner")
        return desired

    def _protect_owners(self, affected: list[GatewayUser]) -> None:
        owners = [user for user in affected if user.role == GatewayRole.OWNER.value]
        if not owners:
            return
        caller = self.gateway_client.get_caller_identifiers()
        if any(user.principal_type == PrincipalType.USER.value and "@" in user.identifier and user.object_id is None for user in owners) and not any("@" in identifier for identifier in caller):
            raise ValueError("Cannot safely identify the deploying User's explicit Owner grant from the selected token")
        for user in owners:
            if user.identifier.casefold() in caller or (user.object_id and user.object_id.casefold() in caller):
                raise ValueError("Gateway reconciliation cannot remove, demote, or replace the deploying principal's explicit Owner grant")

    async def reconcile(self, gateway: GatewayParams) -> None:
        desired = self._desired_users(gateway)
        connection = await self.gateway_client.get_connection(gateway.connection_id)
        users = await self.gateway_client.list_users(connection)
        current = {user.key: user for user in users}
        if len(current) != len(users):
            raise RuntimeError("Gateway current access contains ambiguous principal identifiers")
        additions = [user for key, user in desired.items() if key not in current]
        replacements = [(current[key], user) for key, user in desired.items() if key in current and current[key].permission != user.permission]
        removals = [user for key, user in current.items() if key not in desired]
        self._protect_owners(removals + [old for old, _ in replacements])
        replacements.sort(key=lambda pair: pair[1].role != GatewayRole.OWNER.value)
        projected_owners = {user.key for user in users if user.role == GatewayRole.OWNER.value} | {user.key for user in additions if user.role == GatewayRole.OWNER.value}
        for old, new in replacements:
            if old.role == GatewayRole.OWNER.value:
                if projected_owners == {old.key}:
                    raise ValueError("Cannot replace the only current Owner grant without another Owner")
                projected_owners.discard(old.key)
            if new.role == GatewayRole.OWNER.value:
                projected_owners.add(new.key)
        rename = connection.datasource_name != gateway.display_name
        self.logger.info("Gateway plan: rename=%s, add=%d, replace=%d, remove=%d, dryRun=%s", rename, len(additions), len(replacements), len(removals), gateway.dry_run)
        for index, user in enumerate(desired.values()):
            if user in additions or any(new.key == user.key for _, new in replacements):
                self.logger.info("Gateway users[%d]: %s role=%s", index, "add" if user in additions else "replace", user.role)
        for user in removals:
            self.logger.info("Gateway currentUsers[%d]: remove role=%s", users.index(user), user.role)
        if gateway.dry_run or not (rename or additions or replacements or removals):
            return
        for user in sorted(additions, key=lambda entry: entry.role != GatewayRole.OWNER.value):
            await self.gateway_client.add_user(connection, user)
        for old, new in replacements:
            await self.gateway_client.delete_user(connection, old)
            await self.gateway_client.add_user(connection, new)
        for user in removals:
            await self.gateway_client.delete_user(connection, user)
        if rename:
            await self.gateway_client.rename(connection, gateway.display_name)

        async def read() -> tuple[GatewayConnection, list[GatewayUser]]:
            actual = await self.gateway_client.get_connection(gateway.connection_id)
            return actual, await self.gateway_client.list_users(actual)

        def matches(state: tuple[GatewayConnection, list[GatewayUser]]) -> bool:
            actual = {user.key: user.permission for user in state[1]}
            return state[0].datasource_name == gateway.display_name and len(actual) == len(state[1]) and actual == {key: user.permission for key, user in desired.items()}

        await verify_reconciliation(read, matches, "Gateway reconciliation")
