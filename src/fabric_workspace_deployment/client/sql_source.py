# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import json


def normalize_sql_server(value: str) -> str:
    server = value.strip().casefold()
    if server.startswith("tcp:"):
        server = server[4:]
    if server.endswith(",1433"):
        server = server[:-5]
    return server.rstrip(".")


def sql_connection_identity(value: str) -> tuple[str, str | None]:
    if "=" not in value:
        return normalize_sql_server(value), None
    fields = {}
    for part in value.split(";"):
        if "=" in part:
            key, item = part.split("=", 1)
            fields[key.strip().casefold()] = item.strip().strip('"')
    server = fields.get("data source") or fields.get("server")
    if not server:
        raise RuntimeError("SQL endpoint connection details lack a server")
    return normalize_sql_server(server), fields.get("initial catalog") or fields.get("database")


def datasource_sql_identity(record: dict) -> frozenset[tuple[str, str]]:
    identities = set()
    details = record.get("connectionDetails")
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except ValueError:
            raise RuntimeError("Datasource connectionDetails is not valid JSON") from None
    source_type = record.get("dataSourceType", record.get("datasourceType"))
    if source_type is not None and (isinstance(source_type, bool) or not isinstance(source_type, (str, int))):
        raise RuntimeError("Datasource type marker must be a string, integer, or null")
    accepts_sql_details = source_type is None or isinstance(source_type, int) or source_type.casefold() == "sql"
    if isinstance(details, dict) and accepts_sql_details:
        server, database = details.get("server"), details.get("database")
        if isinstance(server, str) and isinstance(database, str) and server.strip() and database.strip():
            identities.add((normalize_sql_server(server), database.strip().casefold()))
    reference = record.get("dataSourceReference")
    if reference is not None:
        if isinstance(reference, str):
            try:
                reference = json.loads(reference)
            except ValueError:
                raise RuntimeError("Datasource dataSourceReference is not valid JSON") from None
        if not isinstance(reference, dict):
            raise RuntimeError("Datasource dataSourceReference must be an object")
        if str(reference.get("kind", "")).casefold() == "sql":
            path = reference.get("path")
            if not isinstance(path, str) or ";" not in path:
                raise RuntimeError("SQL datasource reference lacks server/database identity")
            server, database = path.split(";", 1)
            if not server.strip() or not database.strip():
                raise RuntimeError("SQL datasource reference lacks server/database identity")
            identities.add((normalize_sql_server(server), database.strip().casefold()))
    return frozenset(identities)
