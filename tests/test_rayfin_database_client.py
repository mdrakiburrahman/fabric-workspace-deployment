# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging
from types import SimpleNamespace

import pytest
import requests

from fabric_workspace_deployment.client.rayfin_database import FabricRayfinDatabaseClient

WORKSPACE_ID = "11111111-1111-1111-1111-111111111111"
APP_ID = "22222222-2222-2222-2222-222222222222"
DATABASE_ID = "33333333-3333-3333-3333-333333333333"
OTHER_ID = "44444444-4444-4444-4444-444444444444"
SERVER = "managed.database.fabric.microsoft.com"
DATABASE = "managed-app-db"


class FakeHttp:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def execute(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        value = self.responses.pop(0)
        return SimpleNamespace(json=lambda: value)


class FakeAz:
    def get_access_token(self, scope):
        assert scope == "https://analysis.windows.net/powerbi/api"
        return "fabric-token"


def _client(responses):
    common = SimpleNamespace(endpoint=SimpleNamespace(cicd="https://fabric.example.invalid"), scope=SimpleNamespace(analysis_service="https://analysis.windows.net/powerbi/api"))
    http = FakeHttp(responses)
    return FabricRayfinDatabaseClient(common, FakeAz(), http), http


def _item(item_id=DATABASE_ID, workspace_id=WORKSPACE_ID, item_type="SQLDatabase"):
    return {"id": item_id, "workspaceId": workspace_id, "type": item_type, "displayName": "same-name"}


def _edge(parent=APP_ID, child=DATABASE_ID, relation_type="CascadeDelete"):
    return {"itemId": parent, "dependentOnItemId": child, "relationType": relation_type}


def _graph(items=None, edges=None):
    return {"items": [_item()] if items is None else items, "relations": [_edge()] if edges is None else edges, "workspaces": [{"id": WORKSPACE_ID}]}


def _database(**overrides):
    return {"id": DATABASE_ID, "workspaceId": WORKSPACE_ID, "type": "SQLDatabase", "properties": {"serverFqdn": f"tcp:{SERVER.upper()},1433", "databaseName": DATABASE, "connectionString": f"Server=tcp:{SERVER},1433;Database={DATABASE};Encrypt=True"}, **overrides}


def test_selects_exact_owned_database_among_multiple_related_sql_items(caplog):
    graph = _graph(items=[_item(OTHER_ID), _item()], edges=[_edge(child=OTHER_ID, relation_type="Datasource"), _edge()])
    client, http = _client([graph, _graph(items=[], edges=[]), _database()])
    caplog.set_level(logging.DEBUG)

    target = client.resolve_managed_database(WORKSPACE_ID, APP_ID)

    assert (target.workspace_id, target.app_backend_id, target.database_id, target.server, target.database_name) == (WORKSPACE_ID, APP_ID, DATABASE_ID, SERVER, DATABASE)
    assert [call[1] for call in http.calls] == [
        f"https://fabric.example.invalid/v1/workspaces/{WORKSPACE_ID}/items/{APP_ID}/relations/upstream?beta=true",
        f"https://fabric.example.invalid/v1/workspaces/{WORKSPACE_ID}/items/{APP_ID}/relations/downstream?beta=true",
        f"https://fabric.example.invalid/v1/workspaces/{WORKSPACE_ID}/sqlDatabases/{DATABASE_ID}",
    ]
    assert all(call[0] is requests.get and call[2]["safe_log_context"] for call in http.calls)
    assert SERVER not in caplog.text
    assert DATABASE not in caplog.text


def test_accepts_parent_ownership_edge_reported_only_by_downstream_endpoint():
    client, _ = _client([_graph(items=[], edges=[]), _graph(), _database()])

    assert client.resolve_managed_database(WORKSPACE_ID, APP_ID).database_id == DATABASE_ID


def test_duplicate_identical_edges_across_directions_are_not_ambiguous():
    client, _ = _client([_graph(), _graph(), _database()])

    assert client.resolve_managed_database(WORKSPACE_ID, APP_ID).database_id == DATABASE_ID


@pytest.mark.parametrize("relation_type", ["Datasource", "WeakAssociation", "PushData", "Orchestration", "HiddenInWorkspace", "Shortcut"])
def test_soft_dependency_does_not_prove_managed_database_ownership(relation_type):
    client, http = _client([_graph(edges=[_edge(relation_type=relation_type)]), _graph(items=[], edges=[])])

    with pytest.raises(RuntimeError, match="exactly one AppBackend-owned"):
        client.resolve_managed_database(WORKSPACE_ID, APP_ID)

    assert len(http.calls) == 2


def test_reversed_cascade_edge_does_not_prove_appbackend_owns_database():
    client, http = _client([_graph(edges=[_edge(parent=DATABASE_ID, child=APP_ID)]), _graph(items=[], edges=[])])

    with pytest.raises(RuntimeError, match="exactly one AppBackend-owned"):
        client.resolve_managed_database(WORKSPACE_ID, APP_ID)

    assert len(http.calls) == 2


@pytest.mark.parametrize(
    "graph",
    [
        _graph(items=[], edges=[]),
        _graph(items=[_item()], edges=[]),
        _graph(items=[_item(), _item(OTHER_ID)], edges=[_edge(), _edge(child=OTHER_ID)]),
        _graph(items=[_item(item_type="SQLEndpoint")]),
    ],
)
def test_missing_or_ambiguous_managed_database_fails_before_connection_read(graph):
    client, http = _client([graph, _graph(items=[], edges=[])])

    with pytest.raises(RuntimeError, match="exactly one AppBackend-owned"):
        client.resolve_managed_database(WORKSPACE_ID, APP_ID)

    assert len(http.calls) == 2


def test_cross_workspace_ownership_fails_before_connection_read():
    client, http = _client([_graph(items=[_item(workspace_id=OTHER_ID)]), _graph(items=[], edges=[])])

    with pytest.raises(RuntimeError, match="guarded workspace"):
        client.resolve_managed_database(WORKSPACE_ID, APP_ID)

    assert len(http.calls) == 2


def test_conflicting_item_identity_across_relation_directions_fails():
    client, http = _client([_graph(), _graph(items=[_item(item_type="Lakehouse")])])

    with pytest.raises(RuntimeError, match="conflicting item identities"):
        client.resolve_managed_database(WORKSPACE_ID, APP_ID)

    assert len(http.calls) == 2


def test_missing_owned_item_identity_fails_even_with_another_visible_database():
    client, _ = _client([_graph(items=[_item(OTHER_ID)]), _graph(items=[], edges=[])])

    with pytest.raises(RuntimeError, match="lacks the dependent"):
        client.resolve_managed_database(WORKSPACE_ID, APP_ID)


@pytest.mark.parametrize(
    ("database", "message"),
    [
        (_database(id=OTHER_ID), "differs"),
        (_database(workspaceId=OTHER_ID), "guarded workspace/type"),
        (_database(type="SQLEndpoint"), "guarded workspace/type"),
        (_database(properties={}), "server FQDN"),
        (_database(properties={"serverFqdn": SERVER, "databaseName": ""}), "database name"),
        (_database(properties={"serverFqdn": SERVER, "databaseName": DATABASE, "connectionString": f"Server=other.example.invalid;Database={DATABASE}"}), "conflict"),
        (_database(properties={"serverFqdn": SERVER, "databaseName": DATABASE, "connectionString": f"Server={SERVER};Database=other"}), "conflict"),
    ],
)
def test_database_lookup_identity_is_verified_without_logging_sensitive_response(database, message, caplog):
    client, _ = _client([_graph(), _graph(items=[], edges=[]), database])
    caplog.set_level(logging.DEBUG)

    with pytest.raises(RuntimeError, match=message):
        client.resolve_managed_database(WORKSPACE_ID, APP_ID)

    assert SERVER not in caplog.text
    assert DATABASE not in caplog.text


def test_database_property_identity_does_not_require_persisting_full_connection_string():
    database = _database(properties={"serverFqdn": SERVER, "databaseName": DATABASE})
    client, _ = _client([_graph(), _graph(items=[], edges=[]), database])

    assert client.resolve_managed_database(WORKSPACE_ID, APP_ID).server == SERVER
