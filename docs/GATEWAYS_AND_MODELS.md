# Gateway connections, model bindings, and RLS

`deployGateway` manages existing gateway/cloud connections independently of workspaces.
`deployModel` applies semantic-model settings, declared connection mappings, and membership
of existing RLS roles. Neither operation creates a gateway connection or an RLS role.

## Configuration

This is an excerpt to merge with the existing mandatory common/workspace configuration.
Every GUID and principal below is fictitious.

```json
{
  "common": {
    "identities": [
      {
        "givenName": "Deployment Owner",
        "objectId": "44444444-4444-4444-4444-444444444444",
        "principalType": "User",
        "userPrincipalName": "owner@example.invalid"
      },
      {
        "givenName": "Report Readers",
        "objectId": "33333333-3333-3333-3333-333333333333",
        "principalType": "Group"
      }
    ],
    "fabric": {
      "gateways": [
        {
          "connectionId": "11111111-1111-1111-1111-111111111111",
          "displayName": "Example ADLS Connection",
          "dryRun": true,
          "users": [
            { "identity": "Deployment Owner", "role": "Owner", "datasourceAccessRight": "Read" },
            { "identity": "Report Readers", "role": "User", "datasourceAccessRight": "Read" }
          ]
        }
      ],
      "workspaces": [
        {
          "name": "Example Workspace",
          "model": [
            {
              "displayName": "Example Model",
              "directLakeAutoSync": false,
              "dryRun": true,
              "connections": [
                { "moniker": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "connectionId": "11111111-1111-1111-1111-111111111111" }
              ],
              "security": {
                "Dynamic Row-Level Security": ["Report Readers"]
              }
            }
          ]
        }
      ]
    }
  }
}
```

| Field                              | Contract                                                                                                                                                         |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fabric.gateways`                  | Optional; defaults to `[]`. Refers to existing connections, not clusters to create.                                                                              |
| Gateway `connectionId`             | Unique stable connection GUID. Used directly by model references; parent cluster IDs are discovered at runtime. |
| Gateway `displayName`              | Desired mutable remote name, used only for rename reconciliation, never for lookup. |
| Gateway `users`                    | Complete desired direct-access list. Supported desired values: Group/User, Owner/User roles, and `datasourceAccessRight: "Read"`.                                |
| Identity names                     | Exact, unique `common.identities[].givenName` references; no fuzzy matching or Graph lookups.                                                                    |
| `userPrincipalName`                | Optional for legacy identities; required for User identities used by these features. Gateway Users use UPN identifiers; Groups and RLS members use object GUIDs. |
| Model `connections`                | Optional moniker-to-connection mappings. Each `connectionId` must reference exactly one declared gateway connection and be a candidate for that moniker and its resolved cluster. |
| Model `security`                   | Exact existing role names mapped to arrays of Group/User identity names.                                                                                         |
| Gateway/model `dryRun`             | Optional strict boolean, default `false`; controls only that entry.                                                                                              |

Startup validates references, GUIDs, metadata, duplicate declarations, and supported values.
`deployGateway` permits `workspaces: []`; other existing common requirements still apply.
Existing operations retain their workspace requirements.

Connection GUIDs remain authoritative. The gateway datasource inventory is queried by exact
`id` to resolve its `clusterId` at runtime for gateway API routes and model binding requests.
No gateway alias or cluster ID is configured, and display names are never used for fuzzy lookup.
Missing/ambiguous connections and malformed inventory cluster IDs fail before writes.

## Authoritative access and ownership

Gateway ACLs exactly match `users`: missing grants are added, changed supported grants are
replaced, and unlisted direct grants are removed. Connection credentials, SSO, privacy, and
other properties are not managed. Unlisted connections are untouched.

At least one desired Owner is required. New owners are granted before obsolete access is removed.
Removing/demoting/replacing the deployer's explicit Owner grant is rejected using the actual
API token's identity. Transitive group ownership cannot be inferred without Graph: retain the
deployer's ownership path when replacing group-owner grants. Safeguards fail rather than
silently retaining unlisted grants.

Only configured RLS roles are authoritative. `"Existing role": []` clears that role's members.
Omitted `security`, `{}`, and unlisted roles leave membership untouched. Omitted/empty
`connections` leave bindings untouched, not unbound. Role definitions and DAX filters must
already exist; service-principal RLS grants are not supported.

## Previews and deployment order

Set `dryRun: true` on every gateway/model that should preview. Gateway previews do not rename
or change access. Model previews do not publish, update settings, bind connections, or update
RLS. Other entries can still apply when their own flags are false. Flags do not cascade through
gateway references.

```bash
fabric-workspace-deployment --config-file-absolute-path /absolute/path/config.json --operation deployGateway
fabric-workspace-deployment --config-file-absolute-path /absolute/path/config.json --operation deployModel
```

Review previews, then set gateway flags to `false` and successfully apply `deployGateway`
first. Set intended model flags to `false` before applying `deployModel`. Model deployment
validates connection existence/candidates but never invokes gateway ACL reconciliation.
The existing `--operation dryRun` remains the configuration/entitlement check. There is no
new CLI dry-run flag.

Missing models retain unique-source `.SemanticModel` publishing during apply. Preview validates
the source and reports a planned publish; binding/RLS preflight is explicitly deferred until
the model exists. Missing/ambiguous roles and invalid candidates fail before their model
settings/binding/RLS writes. Applied changes are verified by bounded read-back checks.

Matched binding/RLS state produces no write to those endpoints. The established
`directLakeAutoSync` settings POST remains unchanged during apply.

## Authentication and compatibility

Use `FAB_TOKEN` with the Power BI scope and Azure CLI fallback; see
[environment variables](ENV_VARS.md). `FAB_SKIP_GATEWAY_DEPLOYMENT=1` skips selected gateway
execution, not model-side connection reads.

New diagnostics identify configured principals by indices/config paths and current extra
access by redacted indices, not raw names, IDs, or UPNs. Tokens, response bodies, and
credential-bearing datasource references are not logged.

These features use the issue-captured internal Power BI UI contracts. Unsupported response
shapes and permission failures surface explicitly. Preview with real identifiers to check
tenant compatibility before applying. REST updates are not atomic across entities; failures
are reported and deterministic reruns converge rather than returning success-shaped fallbacks.

Replace example connection/moniker IDs and Owner object ID/UPN. A nonexistent example
connection fails preflight safely; preview never creates it. Supply the complete intended
ACL before applying because unlisted access will be removed.
