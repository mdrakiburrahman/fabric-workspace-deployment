# Entitlements

Assert that a principal holds the Entra ID group memberships it needs, verified against Microsoft
Graph during `dryRun` — before any resource is touched.

## Configuration

`endpoint.graph` and `scope.graph` are **required in every config file**. `common.entitlements` is
optional and defaults to `[]`.

```jsonc
{
  "common": {
    "endpoint": { "graph": "https://graph.microsoft.com" },
    "scope": { "graph": "https://graph.microsoft.com" },
    "entitlements": [
      {
        "reason": "User needs to be part of the A, B, C entitlement",
        "objectId": "{user-oid}",
        "match": "any",
        "groupIds": [
          "...",
          "..."
        ]
      }
    ]
  }
}
```

| Key        | Type     | Description                                                                            |
| ---------- | -------- | -------------------------------------------------------------------------------------- |
| `reason`   | string   | Non-empty justification. Echoed **verbatim** in the failure message.                   |
| `objectId` | string   | Principal to check. A GUID, or a placeholder such as `{user-oid}`.                     |
| `match`    | string   | `"any"` (member of at least one group) or `"all"` (member of every group). No default. |
| `groupIds` | string[] | Entra group object IDs to test against. May be empty.                                  |

All four keys are required. Membership is **transitive**. `objectId` and `groupIds` are not
cross-referenced against `common.identities`.

> **Breaking change.** `endpoint.graph` and `scope.graph` are required even without entitlements —
> a config missing either fails with `Missing required field in configuration: 'graph'`. Both are
> configurable to support sovereign clouds, e.g. `https://graph.microsoft.us`.

Nothing is checked and no Graph call is made when `entitlements` is empty, or when an entry's
`groupIds` is empty (logged as skipped, treated as satisfied).

## Authentication

The Graph token comes from `FAB_TOKEN_GRAPH` if set, otherwise
`az account get-access-token --resource <scope.graph>` — mirroring `FAB_TOKEN` (Power BI) and
`FAB_TOKEN_AZURE` (ARM). It is **optional**, but when set must be a well-formed JWT.

The token needs `User.Read` to check the signed-in user, or `Directory.Read.All` for another
principal; insufficient permission surfaces as an HTTP 403.

## Running

```bash
fabric-workspace-deployment --config-file-absolute-path /abs/path/config.json --operation dryRun
```

Every entitlement is evaluated — the run does not stop at the first failure — and all failures are
reported in one aggregated error:

```
1 entitlement check(s) failed:
  - User needs to be part of the A, B, C entitlement
      objectId: b99a3530-636d-4621-8662-bc5c8022b125
      match: any
      missing groupIds: 00000000-1111-2222-3333-444444444444
```

## Fail-fast validation

Static validation runs on **every** operation with no network I/O, and fails if `graph` endpoint or
scope is missing/not `https://`, `reason` is empty, `objectId` or any `groupIds` entry is not a
GUID, `match` is missing or not `any`/`all`, or `FAB_TOKEN_GRAPH` is set but malformed.

Unresolved `{placeholder}` values skip the GUID check; duplicate `groupIds` are removed
case-insensitively. The **live** Graph call happens only during `dryRun`, keeping every other
operation free of a Graph dependency.

## Implementation

Transport and policy are separated so further directory operations (e.g. adding a member) can be
added without reworking callers:

| Concern                                  | Interface            | Implementation                 |
| ---------------------------------------- | -------------------- | ------------------------------ |
| Graph transport, auth, batching, retries | `GraphClient`        | `client/graph_membership.py`   |
| Match semantics, skip rules, reporting   | `EntitlementManager` | `manager/azure/entitlement.py` |

Uses `POST /v1.0/directoryObjects/{objectId}/checkMemberGroups` — principal-type agnostic, so users,
groups, and service principals all work. Requests batch at Graph's 20-ID limit and are wrapped in
`HttpRetryHandler`.
