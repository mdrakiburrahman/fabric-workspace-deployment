# Rayfin deployments

`fabric-workspace-deployment` (FWD) can build and deploy one or more Rayfin applications with the explicit `deployRayfin` operation. Rayfin support is opt-in per workspace: an existing FWD configuration with no `common.fabric.workspaces[].rayfins` properties remains valid, and `deployRayfin` completes as a no-op.

## Prerequisites

- Docker Engine with the Docker Compose plugin (`docker compose`)
- Fabric CLI (`fab`) authenticated for the target workspace
- Either:
  - `RAYFIN_TOKEN` containing a pre-acquired Fabric/Power BI token, or
  - Azure CLI authentication that can acquire a token for the configured `common.scope.analysisService`
- A committed `package.json` and `package-lock.json` in each app root
- `@microsoft/rayfin-cli` pinned exactly in the app dependency graph to the version declared by the app manifest

FWD uses its packaged `Compose.rayfin.yaml` and the rolling MCR-hosted `mcr.microsoft.com/azurelinux/base/nodejs:24` image. This tracks the latest serviced Node.js 24 build on Azure Linux 3.0 without requiring a customer Dockerfile.

## FWD configuration contract

Add an optional `rayfins` list to each parent entry in `common.fabric.workspaces`:

```json
{
  "common": {
    "fabric": {
      "workspaces": [
        {
          "name": "Analytics Production",
          "...": "existing workspace configuration",
          "rayfins": [
            {
              "rootPath": "apps/sales-insights",
              "force": false,
              "semanticModels": {
                "sales": {
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
      ]
    }
  }
}
```

Each Rayfin entry inherits its deployment workspace from the parent workspace's `name` and has exactly these fields:

| Field            | Type    | Meaning                                                                                                                                                                                                                                                                              |
| ---------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `rootPath`       | string  | App root relative to `common.local.rootFolder`. The resolved path must remain under that root. Root paths must be unique across every workspace in the configuration.                                                                                                                 |
| `force`          | boolean | Optional, defaults to `false`. When `true`, FWD passes Rayfin's `--force` flag to allow destructive managed-data schema migrations that may result in data loss.                                                                                                                       |
| `semanticModels` | object  | Mapping from app connection alias to a binding containing required `itemName` and optional `workspaceName`. When `workspaceName` is omitted, the semantic model is resolved in the parent workspace; an explicit value preserves cross-workspace resolution. Empty mappings are allowed. |

Each semantic-model binding supports these fields:

| Field           | Type   | Required | Meaning                                                                                                  |
| --------------- | ------ | -------- | -------------------------------------------------------------------------------------------------------- |
| `workspaceName` | string | No       | Friendly workspace containing the semantic model. Defaults to the parent workspace when omitted.         |
| `itemName`      | string | Yes      | Friendly semantic-model display name in the inherited or explicitly configured semantic-model workspace. |

Unknown or missing Rayfin fields are rejected. Aliases must start with a letter and contain only letters, digits, `_`, or `-`. The aliases must match the app manifest's `connections.semanticModels` list exactly. Manifest app IDs must be unique within each parent workspace, but the same app ID may be used by a different parent workspace. A parent workspace with `skipDeploy: true` skips all of its nested Rayfin applications.

There is no compatibility period for the former Fabric-scoped location. Configurations containing `common.fabric.rayfins` are rejected with a migration error directing callers to `common.fabric.workspaces[].rayfins`.

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

The manifest is intentionally strict: all shown sections and fields except the backward-compatible optional `data` block are required, and unknown fields are rejected. An optional `functions` block is described below.

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

### Functions

Add this optional app-root manifest block to enable server-side Functions:

```json
{
  "functions": {
    "enabled": true,
    "path": "rayfin/functions",
    "buildCommand": "npm run build"
  }
}
```

When the block is absent, or `enabled` is `false`, Functions are disabled and FWD omits `services.functions` from generated YAML. A disabled block may contain just `{ "enabled": false }`. An enabled block requires all three fields and renders:

```yaml
services:
  functions:
    enabled: true
    auth:
      type: application
    path: rayfin/functions
    buildCommand: npm run build
```

The Functions directory must be an existing subdirectory inside the app root, including after resolving symlinks. It must contain non-empty `package.json`, `host.json`, `tsconfig.json`, and `src/function_app.ts` files. Required files and local `file:` dependencies must remain within the Functions project so they can be packaged by Rayfin; absolute paths, parent traversal, missing local dependencies, and escaping symlinks are rejected. The host configuration must declare `"version": "2.0"`. The build command must be non-empty; an `npm run` command must reference an existing non-empty script in the Functions package.

FWD uses a conservative same-release pin policy: the Functions runtime dependency `@microsoft/fabric-user-data-functions` must be pinned exactly to `rayfin.version`, and its lockfile metadata must depend on that same exact `@microsoft/rayfin-client` release. Mutable tags such as `experimental`, ranges, conflicting pins, and different release versions are rejected. This is a reproducibility policy, not a claim that arbitrary cross-version combinations are compatible.

The root package must include the Functions directory in npm workspaces:

```json
{
  "private": true,
  "workspaces": ["rayfin/functions"],
  "devDependencies": {
    "@microsoft/rayfin-cli": "1.35.1"
  }
}
```

The Functions package declares its own runtime dependency and build script:

```json
{
  "name": "sales-functions",
  "private": true,
  "type": "module",
  "main": "dist/src/function_app.js",
  "scripts": {
    "build": "tsc --build"
  },
  "dependencies": {
    "@microsoft/fabric-user-data-functions": "1.35.1"
  }
}
```

Generate and commit the root `package-lock.json` using npm; do not hand-author lock entries. Workspace patterns must be app-relative and must not use exclusions. FWD validates the workspace entry, workspace link, and the SDK resolution nearest to the Functions project, including nested and hoisted dependencies. Root `npm ci` installs both projects from a clean checkout, with no manual Functions install. After installation, FWD uses `npm exec --workspace <functions-path> --no -- tsc --showConfig --project tsconfig.json` to validate the Functions configuration with that workspace's installed compiler before invoking Rayfin; it does not download an extra compiler. Rayfin's canonical full `up` owns the Functions build and deployment; FWD does not run a separate UDF deploy command.

Build scripts are trusted application code executed inside the existing staging container, not a sandbox for hostile code.

The existing `dryRun` operation reports locally validated Rayfin app, Functions, managed-data, and deployment-order intent without acquiring Rayfin/SQL credentials, creating staging, installing dependencies, or invoking Docker for this report. Skipped workspaces are reported without reading their app roots. Existing global CLI checks and entitlement verification still run; this does not turn the entire `dryRun` operation into an offline command. TypeScript configuration is checked by the installed compiler during an actual deployment, not by executing a build in dry-run.

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
```

Add `.fabric-workspace-deployment/` to the consuming repository's `.gitignore`. FWD excludes that directory when copying an app into staging, including when the app root is the same as `common.local.rootFolder`, which prevents recursive staging copies.

FWD never seeds or persists `rayfin/.deployments.json`. Every run starts without cached deployment state and resolves the AppBackend directly through Fabric. Successful runs remove the staged app directory; failed runs retain it beneath the source root for diagnostics.

## AppBackend RBAC

A deployed Rayfin application is a Fabric `AppBackend`. Configure its direct access through the target workspace's existing `common.fabric.workspaces[].rbac.items[]` collection:

```json
{
  "type": "AppBackend",
  "displayName": "sales-insights",
  "detail": [
    {
      "permissions": 65,
      "objectId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
      "purpose": "Run the Rayfin application"
    }
  ]
}
```

| Portal permissions | `detail[].permissions` |
| --- | ---: |
| Read, Execute | `65` |
| Read, Write, Execute | `67` |
| Read, Reshare, Execute | `69` |
| Read, Write, Reshare, Execute | `71` |

`artifactPermissions` must be omitted or `0`. FWD reads and writes AppBackend access through Fabric's artifacts access API using Entra object IDs for groups, users, and service principals. With authoritative purge enabled, unmatched direct grants are removed while rows carrying a workspace-role `accessSource` are preserved. Explicit desired entries are still reconciled.

Removing AppBackend access does not modify the Rayfin-managed SQLDatabase or SQLEndpoint children. Configure a child item separately when it needs direct access. See [Fabric RBAC](RBAC.md) for validation, purge safety, SemanticModel permissions, and missing-item behavior.

## Deployment workflow

When a Rayfin semantic-model binding targets a model managed by the same FWD configuration, run `deployModel` before `deployRayfin`. A binding without `workspaceName` resolves in its Rayfin app's parent workspace. If the configured model is absent, `deployModel` now publishes the matching `<displayName>.SemanticModel` directory from the workspace template's `artifactsFolder`, waits for it to become visible, and then applies the configured model settings. Missing or ambiguous source directories fail the operation instead of being logged as a successful warning.

Run:

```bash
fabric-workspace-deployment \
  --config-file-absolute-path /absolute/path/to/config.json \
  --operation deployRayfin
```

For each configured app, FWD:

1. Loads and validates the app manifest.
2. Resolves the parent workspace's friendly `name` with Fabric CLI. Workspaces with `skipDeploy: true` are skipped before any app resolution or token acquisition.
3. Resolves each semantic model's friendly item name in the parent workspace by default, or in its explicit `workspaceName` when cross-workspace resolution is configured.
4. Creates isolated staging and generates `fabric.yaml` and `rayfin/rayfin.yml`.
5. Injects `RAYFIN_TOKEN`, the resolved `RAYFIN_WORKSPACE_ID`, and `RAYFIN_TENANT_ID` into the Compose service.
6. Runs `npm ci`.
7. Verifies the local Rayfin CLI exact version.
8. Runs canonical unattended `./node_modules/.bin/rayfin up --yes`, adding `--force` only when the workspace-scoped Rayfin binding explicitly sets `force: true`. This confirms reuse of an existing AppBackend without prompting, deploys the app, and provisions/applies the managed MSSQL data schema when enabled.
9. Loads `rayfin/.deployments.json`, validates the reported data-service state when present, and directly retrieves the recorded `fabricItemId` within the guarded workspace through Fabric API. Display names are not used for this assertion.
10. Runs `./node_modules/.bin/rayfin up status --json`, validating the reported data-service state when present.

Successful runs remove their staging directory. Failed runs retain staging and log its path for diagnostics. Tokens are passed only in the process environment and are redacted from FWD environment and Docker command logs.

When FWD itself runs in a container against a Docker daemon outside that container, it resolves the staging directory through the current container's Docker mount metadata before creating the Rayfin bind mount. This supports bind mounts and Docker volumes without runner-specific path assumptions. The staging root must be under a mount exported to the active daemon; otherwise deployment fails before `npm ci` with an actionable path error.

The packaged Compose service remains Node-only (`mcr.microsoft.com/azurelinux/base/nodejs:24`). FWD does not add or operate a separate SQL Docker service; managed SQL provisioning belongs to Rayfin and Microsoft Fabric.
