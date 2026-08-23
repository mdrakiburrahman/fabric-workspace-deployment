# Environment variables

This is the canonical reference for environment variables read directly by
`fabric-workspace-deployment`.

## Authentication and tooling

| Environment variable | Purpose                                                                                                     |
| -------------------- | ----------------------------------------------------------------------------------------------------------- |
| `FAB_TOKEN`          | Power BI/Fabric access token. Falls back to `az account get-access-token` when unset.                       |
| `FAB_TOKEN_AZURE`    | Azure Resource Manager access token. Falls back to Azure CLI authentication when unset.                     |
| `FAB_TOKEN_GRAPH`    | Microsoft Graph access token used by entitlement checks. Falls back to Azure CLI authentication when unset. |
| `FAB_TOKEN_CICD`     | Token used to construct the `fabric-cicd` credential. Falls back to `AzureCliCredential` when unset.        |
| `FAB_PATH`           | Directory containing the `fab` executable.                                                                  |
| `GIT_ROOT`           | Git-root override used by `{git-root}` placeholder and logging path resolution.                             |
| `PACKAGE_VERSION`    | Build-time package version override.                                                                        |

## Identity placeholders

| Environment variable  | Placeholder or behavior                                                                                 |
| --------------------- | ------------------------------------------------------------------------------------------------------- |
| `UNIQUE_ENV_ID`       | Supplies `{unique-env-id}`; falls back to the resolved user alias.                                      |
| `USER_APP_ID`         | Supplies `{user-appid}`; falls back to Azure CLI lookup.                                                |
| `USER_DISPLAY_NAME`   | Supplies `{user-display-name}` and user-alias resolution; falls back to Azure CLI or Git configuration. |
| `USER_OBJECT_ID`      | Supplies `{user-oid}`; falls back to Azure CLI lookup.                                                  |
| `USER_PRINCIPAL_TYPE` | Supplies `{user-principal-type}`; defaults to `User`.                                                   |

## Manager execution skips

Every operation-facing manager supports an invocation-level environment variable that skips its
top-level execution when the trimmed value is exactly `1`.

| Manager purpose                         | Environment variable                   |
| --------------------------------------- | -------------------------------------- |
| Entitlement validation used by `dryRun` | `FAB_SKIP_ENTITLEMENT_CHECK`           |
| Alert deployment                        | `FAB_SKIP_ALERT_DEPLOYMENT`            |
| Fabric capacity deployment              | `FAB_SKIP_FABRIC_CAPACITY_DEPLOYMENT`  |
| Fabric workspace deployment             | `FAB_SKIP_FABRIC_WORKSPACE_DEPLOYMENT` |
| Git-link deployment                     | `FAB_SKIP_GIT_LINK_DEPLOYMENT`         |
| Template/CICD deployment                | `FAB_SKIP_TEMPLATE_DEPLOYMENT`         |
| RBAC deployment                         | `FAB_SKIP_RBAC_DEPLOYMENT`             |
| Seed deployment                         | `FAB_SKIP_SEED_DEPLOYMENT`             |
| Shortcut deployment                     | `FAB_SKIP_SHORTCUT_DEPLOYMENT`         |
| Spark deployment                        | `FAB_SKIP_SPARK_DEPLOYMENT`            |
| Semantic-model deployment               | `FAB_SKIP_MODEL_DEPLOYMENT`            |
| Monitoring deployment                   | `FAB_SKIP_MONITORING_DEPLOYMENT`       |

For example:

```bash
FAB_SKIP_ENTITLEMENT_CHECK=1 \
  fabric-workspace-deployment \
  --config-file-absolute-path /abs/path/config.json \
  --operation dryRun
```

Values other than `1`, including `true`, leave execution enabled. Static configuration validation,
manager construction, and global startup checks still run. Only the selected manager's top-level
`execute()` body is bypassed; helper methods remain available when another operation uses that
manager as a collaborator.

## Config-selected secrets

Storage RBAC in `jwt` mode reads the environment variable named by
`common.fabric.storages[].rbac.auth.jwt.env`. That variable name is configuration-defined rather
than statically defined by the package.
