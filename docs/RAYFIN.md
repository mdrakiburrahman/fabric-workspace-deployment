# Rayfin deployments

`fabric-workspace-deployment` (FWD) can build and deploy one or more Rayfin applications with the explicit `deployRayfin` operation. Rayfin support is opt-in: an existing FWD configuration with no top-level `rayfin` property remains valid, and `deployRayfin` completes as a no-op.

## Prerequisites

- Docker Engine with the Docker Compose plugin (`docker compose`)
- Fabric CLI (`fab`) authenticated for the target workspace
- Either:
  - `RAYFIN_TOKEN` containing a pre-acquired Fabric/Power BI token, or
  - Azure CLI authentication that can acquire a token for the configured `common.scope.analysisService`
- `RAYFIN_WORKSPACE_ID` set to the only Fabric workspace ID the invocation is allowed to deploy into
- A committed `package.json` and `package-lock.json` in each app root
- `@microsoft/rayfin-cli` pinned exactly in the app dependency graph to the version declared by the app manifest

FWD uses its packaged `Compose.rayfin.yaml` and the rolling MCR-hosted `mcr.microsoft.com/azurelinux/base/nodejs:24` image. This tracks the latest serviced Node.js 24 build on Azure Linux 3.0 without requiring a customer Dockerfile.

## FWD configuration contract

Add an optional top-level `rayfin` list alongside `common`:

```json
{
  "common": {
    "...": "existing FWD configuration"
  },
  "rayfin": [
    {
      "rootPath": "apps/sales-insights",
      "workspaceName": "Analytics Production",
      "semanticModels": {
        "sales": {
          "workspaceName": "Shared Models Production",
          "itemName": "Sales Model"
        },
        "inventory": {
          "workspaceName": "Supply Chain Production",
          "itemName": "Inventory Model"
        }
      }
    }
  ]
}
```

Each entry has exactly these fields:

| Field            | Type   | Meaning                                                                                                                                                                                |
| ---------------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `rootPath`       | string | App root relative to `common.local.rootFolder`. The resolved path must remain under that root.                                                                                         |
| `workspaceName`  | string | Friendly display name of the target workspace where the Rayfin AppBackend is deployed. FWD resolves it to an ID and applies the `RAYFIN_WORKSPACE_ID` safety guard.                    |
| `semanticModels` | object | Mapping from app connection alias to a strict `{ "workspaceName", "itemName" }` binding. Each semantic model can come from a different friendly workspace. Empty mappings are allowed. |

Each semantic-model binding has exactly these fields:

| Field           | Type   | Meaning                                                                |
| --------------- | ------ | ---------------------------------------------------------------------- |
| `workspaceName` | string | Environment-specific friendly workspace containing the semantic model. |
| `itemName`      | string | Friendly semantic-model display name in that workspace.                |

Unknown or missing binding fields are rejected. Aliases must start with a letter and contain only letters, digits, `_`, or `-`. The aliases must match the app manifest's `connections.semanticModels` list exactly.

## App-root manifest contract

Every configured app root must contain `fabric-workspace-deployment.json`:

```json
{
  "schemaVersion": "1.0",
  "kind": "rayfin",
  "app": {
    "id": "sales-insights",
    "name": "Sales Insights",
    "version": "2.4.0"
  },
  "rayfin": {
    "version": "1.35.1"
  },
  "connections": {
    "semanticModels": [
      "sales",
      "inventory"
    ]
  },
  "build": {
    "command": "npm run build",
    "outputPath": "dist",
    "indexDocument": "index.html"
  },
  "data": {
    "enabled": true,
    "dialect": "mssql"
  }
}
```

The manifest is intentionally strict: all shown sections and fields except the backward-compatible optional `data` block are required, and unknown fields are rejected.

| Field                        | Requirements                                                                                                                              |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `schemaVersion`              | Must be exactly `"1.0"`.                                                                                                                  |
| `kind`                       | Must be exactly `"rayfin"`.                                                                                                               |
| `app.id`                     | Lowercase kebab-case Rayfin app ID.                                                                                                       |
| `app.name`                   | Fabric AppBackend display name.                                                                                                           |
| `app.version`                | Exact semantic version.                                                                                                                   |
| `rayfin.version`             | Exact Rayfin CLI semantic version; ranges such as `^1.35.1`, tags such as `latest`, and partial versions are rejected.                    |
| `connections.semanticModels` | Unique logical aliases required by the app.                                                                                               |
| `build.command`              | Command placed in `services.staticHosting.buildCommand`.                                                                                  |
| `build.outputPath`           | App-relative static output folder. Absolute paths and `..` are rejected.                                                                  |
| `build.indexDocument`        | App-relative default document. Absolute paths and `..` are rejected.                                                                      |
| `data`                       | Optional managed data-service block. When omitted, FWD defaults to `{ "enabled": false, "dialect": "mssql" }` for backward compatibility. |
| `data.enabled`               | Boolean enabling managed Rayfin data.                                                                                                     |
| `data.dialect`               | Must be exactly `"mssql"`; Fabric deployments do not accept PostgreSQL.                                                                   |

FWD runs `npm ci`, then invokes `./node_modules/.bin/rayfin`. It checks the installed CLI's `--version` output against `rayfin.version` before deployment, so `package.json` and `package-lock.json` must resolve that exact version.

When `data.enabled` is `true`, `<app-root>/rayfin/data/schema.ts` must exist and be non-empty. FWD uses the canonical `rayfin up` workflow, which provisions the managed SQL Database and applies pending schema changes together with the application deployment.

## Generated configuration

FWD never writes deployment-generated files into the configured app source. It copies the app into a private staging directory and generates:

- `<staging-app>/fabric.yaml`
- `<staging-app>/rayfin/rayfin.yml`
- `<staging-app>/Compose.rayfin.yaml` from the packaged resource

`fabric.yaml` contains a `deployment` profile with the resolved workspace and semantic-model item IDs. `rayfin/rayfin.yml` contains the app identity, static-hosting build settings, and Fabric SSO configuration:

```yaml
services:
  auth:
    enabled: true
    password:
      enabled: false
    fabric:
      enabled: true
  data:
    enabled: true
    dialect: mssql
```

FWD does not author `allowedRedirectUris`; Rayfin deployment owns registration of the deployed hosting origin. The application frontend initializes embedded Fabric authentication with `initEmbeddedAuth`.

By default, transient staging is created beneath the configured source root so Docker-outside-of-Docker (DooD) can resolve the bind source on the host. Persistent deployment state remains in the invoking user's home directory:

```text
<common.local.rootFolder>/.fabric-workspace-deployment/rayfin-staging/
~/.fabric-workspace-deployment/rayfin-state/
```

Add `.fabric-workspace-deployment/` to the consuming repository's `.gitignore`. FWD excludes that directory when copying an app into staging, including when the app root is the same as `common.local.rootFolder`, which prevents recursive staging copies.

The Rayfin deployment registry is persisted under `rayfin-state` with owner-only permissions so later runs can reuse the same Fabric AppBackend without committing `rayfin/.deployments.json` to the app source. Successful runs remove the staged app directory; failed runs retain it beneath the source root for diagnostics.

## Deployment workflow

When a Rayfin semantic-model binding targets a model managed by the same FWD configuration, run `deployModel` before `deployRayfin`. If the configured model is absent, `deployModel` now publishes the matching `<displayName>.SemanticModel` directory from the workspace template's `artifactsFolder`, waits for it to become visible, and then applies the configured model settings. Missing or ambiguous source directories fail the operation instead of being logged as a successful warning.

Run:

```bash
RAYFIN_WORKSPACE_ID=39db68f1-2443-43ee-bb56-ebf4ba4e39a7 \
fabric-workspace-deployment \
  --config-file-absolute-path /absolute/path/to/config.json \
  --operation deployRayfin
```

For each configured app, FWD:

1. Loads and validates the app manifest.
2. Resolves the friendly workspace with Fabric CLI and fails unless its ID exactly matches the required `RAYFIN_WORKSPACE_ID` safety guard.
3. Independently resolves each semantic model's configured friendly workspace and item name; semantic-model workspaces may differ from the guarded AppBackend target workspace.
4. Creates isolated staging and generates `fabric.yaml` and `rayfin/rayfin.yml`.
5. Injects `RAYFIN_TOKEN`, verified `RAYFIN_WORKSPACE_ID`, and `RAYFIN_TENANT_ID` into the Compose service.
6. Runs `npm ci`.
7. Verifies the local Rayfin CLI exact version.
8. Runs canonical unattended `./node_modules/.bin/rayfin up --yes`, which confirms reuse of an existing AppBackend without prompting, deploys the app, and provisions/applies the managed MSSQL data schema when enabled.
9. Loads `rayfin/.deployments.json`, validates the reported data-service state when present, and directly retrieves the recorded `fabricItemId` within the guarded workspace through Fabric API. Display names are not used for this assertion.
10. Persists the verified deployment registry outside the app source.
11. Runs `./node_modules/.bin/rayfin up status --json`, validating the reported data-service state when present.

Successful runs remove their staging directory. Failed runs retain staging and log its path for diagnostics. Tokens are passed only in the process environment and are redacted from FWD environment and Docker command logs.

The packaged Compose service remains Node-only (`mcr.microsoft.com/azurelinux/base/nodejs:24`). FWD does not add or operate a separate SQL Docker service; managed SQL provisioning belongs to Rayfin and Microsoft Fabric.
