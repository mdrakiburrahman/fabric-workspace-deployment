# Copilot instructions: fabric-workspace-deployment

A Python CLI that deploys and reconciles Microsoft Fabric estates (capacities, workspaces,
RBAC, Spark pools/environments, OneLake shortcuts, semantic models, monitoring, alert
contacts). It is config-file driven, runs **one operation per invocation**, and works by
driving the `fab` (Fabric) and `az` (Azure) CLIs plus direct Fabric REST calls. It ships as
a PyInstaller binary and as a package on an internal Azure DevOps PyPI feed.

## Build, run, lint, type-check

Nx wraps Hatch; both are available. Prefer these:

- Format (the lint gate): `npx nx lint` → `black --line-length 2000 .`
- Type check: `hatch run types:check` → `mypy src/fabric_workspace_deployment tests`
- Build the onefile binary: `npx nx build` (clean + `hatch run build:binary`, uses `fabric-workspace-deployment.spec`)
- Build + invoke the binary from `dist/`: `npx nx run fabric-workspace-deployment:run -- --config-file-absolute-path <abs.json> --operation <op>`
- Publish (lint → `contrib/publish.sh` → hatch build + twine to the ADO feed): `npx nx publish`

Invoke the installed console script directly with two required args:

```
fabric-workspace-deployment --config-file-absolute-path <ABSOLUTE path to config JSON> --operation <operation>
```

`--operation` values are the `Operation` enum strings: `dryRun`, `deployFabricCapacity`,
`deployFabricWorkspace`, `deployTemplate`, `deployRbac`, `deploySeed`, `deployShortcut`,
`deploySpark`, `deployModel`, `deployMonitoring`, `deployAlert`, `deployGitLink`.

There is **no automated test suite yet** — `tests/` holds only a package marker, and there is
no pytest config. `mypy` is the main static gate.

## Architecture (the parts that span files)

- **Entry point** (`main.py`): `parse_config()` builds an `OperationParams` (loads the config
  JSON, resolves magic placeholders, validates), then `await CentralOperator(params).execute()`.
  Logging goes to a timestamped `app_<ts>.log` file plus a filtered stream handler.
- **Dispatch** (`operations/operators.py`): `CentralOperator` (an `EntryPointOperator`) eagerly
  builds **every** manager in its constructor via the factory, then `execute()` `match`es on the
  `Operation` enum and delegates to exactly one manager's `execute()`.
- **Dependency wiring** (`factories/management_factory.py`): `ContainerizedManagementFactory`
  is the DI seam — it constructs each manager/client and injects shared collaborators
  (`AzCli`, `FabricCli`, `HttpRetryHandler`, and other managers). Add wiring here, not inline.
- **Contract hub** (`operations/operation_interfaces.py`, ~150 KB — grep, don't full-read):
  contains the `Operation` enum, **all** config `@dataclass` models (the typed mirror of the
  config file), and **all** ABC interfaces (`Manager`, the per-domain `*Manager`s, the `*Client`s).
- **Implementations**: concrete managers in `manager/fabric/` and `manager/azure/`; Fabric REST
  API clients in `client/`; the `az`/`fab` CLI wrappers in `manager/azure/cli.py` and
  `manager/fabric/cli.py`; string helpers in `static/transformers.py`.
- **External I/O**: two paths — (a) shell out to `fab`/`az` through `FabricCli`/`AzCli`; (b) direct
  Fabric REST via `requests`, always wrapped in `HttpRetryHandler` (exponential backoff + jitter
  on 408/429/5xx).

## Config model

- A single JSON config passed by **absolute path**. Its top-level `common` block maps to
  `CommonParams` (`local`, `endpoint`, `scope`, `arm`, `fabric.{workspaces,storages}`,
  `identities`, `contacts`).
- Parsing is **manual**, not auto-mapped: `OperationParams._parse_*` methods convert camelCase
  JSON keys into snake_case dataclass fields. When you add a config field, update both the
  dataclass in `operation_interfaces.py` and its `_parse_*` method.
- **Magic placeholders** in the config (and in referenced `*.tmpl` files) are resolved when
  `replace_placeholders=True`: `{unique-env-id}`, `{user-oid}`, `{user-appid}`,
  `{user-display-name}`, `{user-principal-type}`, `{user-fabric-admin-func()}`, `{git-root}`,
  `{timestamp}`. Each resolves from an env var first, then falls back to an `az` lookup. Register
  new ones in `_get_placeholder_resolvers`.

## Conventions specific to this repo

- **Formatting**: `black --line-length 2000` — effectively "never wrap". Do not hand-wrap long
  lines; run `npx nx lint` before pushing. `# fmt: skip` / `# noqa` are used for the rare exceptions.
- **License header**: every source file starts with the SPDX block
  (`# SPDX-FileCopyrightText: 2025-present Raki Rahman ...` / `# SPDX-License-Identifier: MIT`).
- **Adding an operation**: add an `Operation` enum value + a `match` case in `CentralOperator`,
  a new ABC in `operation_interfaces.py`, the concrete manager under `manager/…`, and a
  `create_*` method in `ManagementFactory`.
- **Manager shape**: each implements `async def execute(self) -> None`. Workspace-scoped work
  fans out with `asyncio.gather` over `common_params.fabric.workspaces` and honors `skipDeploy`.
- **Auth via env vars** (checked before calling `az`): `FAB_TOKEN` (Power BI/Fabric scope),
  `FAB_TOKEN_AZURE` (ARM scope), `FAB_TOKEN_CICD` (fabric-cicd), `FAB_PATH` (dir of the `fab` binary).
- **Fabric CLI errors go to stdout, not stderr** — `FabricCliError` captures both streams; read
  `.stdout` to recover the real cause.
- **Versioning**: package version is `<epoch>.<git-hash-int>.0`, computed once via
  `PACKAGE_VERSION` so sdist and wheel match within a build (`__about__.py`, `contrib/publish.sh`).

## Dev environment

- Work happens inside the pinned devcontainer image
  (`rakirahman.azurecr.io/devcontainer/spark:…`), which mounts `~/.azure` for `az` auth and
  provides `fab`, `hatch`, `uv`, and Python deps.
- One-time host bootstrap: `contrib/bootstrap-dev-env.sh` (installs docker + native-Linux `az` +
  node, and writes `.npmrc` with an `az`-derived token for the internal npm feed). See
  `contrib/README.md` for the full WSL/devcontainer flow.

## Git / PRs

- PR titles follow [Conventional Commits](https://www.conventionalcommits.org/); fill in the
  `.github/pull_request_template.md`.
- Feature branches: `dev/<alias>/<short-topic>`; PRs target `main`; link issues with `Fixes #N`.
