# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

from fabric_workspace_deployment.client.fabric_rest import FabricRestClient, response_array, response_guid, response_object, response_string
from fabric_workspace_deployment.client.sql_source import normalize_sql_server, sql_connection_identity
from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import CommonParams, HttpRetryHandler, RayfinDatabaseClient
from fabric_workspace_deployment.rayfin.manifest import RayfinManagedSqlDatabase


class FabricRayfinDatabaseClient(FabricRestClient, RayfinDatabaseClient):
    """Resolve SQL ownership through typed Fabric item relations, never workspace position."""

    def __init__(self, common_params: CommonParams, az_cli: AzCli, http_retry_handler: HttpRetryHandler):
        super().__init__(common_params, az_cli, http_retry_handler, common_params.endpoint.cicd)

    def resolve_managed_database(self, workspace_id: str, app_backend_id: str) -> RayfinManagedSqlDatabase:
        workspace_id = response_guid(workspace_id, "Guarded Rayfin workspace ID")
        app_backend_id = response_guid(app_backend_id, "Guarded Rayfin AppBackend ID")
        related_items: dict[str, tuple[str, str]] = {}
        owned_ids: set[str] = set()
        for direction in ("upstream", "downstream"):
            path = f"/v1/workspaces/{workspace_id}/items/{app_backend_id}/relations/{direction}?beta=true"
            data = response_object(self._get_json(path, "Rayfin managed SQL ownership relations"), "Rayfin ownership relations")
            for value in response_array(data.get("items"), "Rayfin related items"):
                item = response_object(value, "Rayfin related item")
                item_id = response_guid(item.get("id"), "Rayfin related item ID").casefold()
                item_type = response_string(item.get("type"), "Rayfin related item type")
                item_workspace = response_guid(item.get("workspaceId"), "Rayfin related item workspace ID").casefold()
                identity = (item_type, item_workspace)
                if item_id in related_items and related_items[item_id] != identity:
                    raise RuntimeError("Fabric ownership relations report conflicting item identities")
                related_items[item_id] = identity
                if item_id == app_backend_id.casefold() and identity != ("AppBackend", workspace_id.casefold()):
                    raise RuntimeError("Fabric ownership relations contradict the guarded AppBackend identity")
            for value in response_array(data.get("relations"), "Rayfin ownership edges"):
                edge = response_object(value, "Rayfin ownership edge")
                parent_id = response_guid(edge.get("itemId"), "Rayfin relation source ID")
                child_id = response_guid(edge.get("dependentOnItemId"), "Rayfin relation dependent ID")
                relation_type = response_string(edge.get("relationType"), "Rayfin relation type")
                if relation_type == "CascadeDelete" and parent_id.casefold() == app_backend_id.casefold():
                    owned_ids.add(child_id.casefold())

        sql_ids: set[str] = set()
        for item_id in owned_ids:
            owned_identity = related_items.get(item_id)
            if owned_identity is None:
                raise RuntimeError("Fabric ownership relation lacks the dependent item's identity")
            if owned_identity[0] != "SQLDatabase":
                continue
            if owned_identity[1] != workspace_id.casefold():
                raise RuntimeError("Rayfin managed SQL ownership crosses the guarded workspace")
            sql_ids.add(item_id)
        if len(sql_ids) != 1:
            raise RuntimeError("Fabric item relations must expose exactly one AppBackend-owned SQLDatabase through a parent CascadeDelete edge; refusing missing or ambiguous migration targeting")

        database_id = next(iter(sql_ids))
        database = response_object(self._get_json(f"/v1/workspaces/{workspace_id}/sqlDatabases/{database_id}", "Rayfin managed SQL identity"), "Rayfin managed SQL Database")
        if response_guid(database.get("id"), "Managed SQL Database ID").casefold() != database_id:
            raise RuntimeError("Managed SQL Database identity differs from its ownership relation")
        if response_guid(database.get("workspaceId"), "Managed SQL Database workspace ID").casefold() != workspace_id.casefold() or database.get("type") != "SQLDatabase":
            raise RuntimeError("Managed SQL Database lookup violates the guarded workspace/type")
        properties = response_object(database.get("properties"), "Managed SQL Database properties")
        server = normalize_sql_server(response_string(properties.get("serverFqdn"), "Managed SQL server FQDN"))
        database_name = response_string(properties.get("databaseName"), "Managed SQL database name")
        if not server or ";" in server or "=" in server:
            raise RuntimeError("Managed SQL Database has an invalid server identity")
        connection_string = properties.get("connectionString")
        if connection_string is not None:
            connection_server, connection_database = sql_connection_identity(response_string(connection_string, "Managed SQL connection identity"))
            if connection_server != server or connection_database is None or connection_database.casefold() != database_name.casefold():
                raise RuntimeError("Managed SQL connection details conflict with the verified server/database identity")
        return RayfinManagedSqlDatabase(workspace_id, app_backend_id, database_id, server, database_name)
