# Externally managed shortcuts

With `enable_shortcut_publish`, [fabric-cicd 1.4.0 deletes every deployed shortcut](https://github.com/microsoft/fabric-cicd/blob/v1.4.0/src/fabric_cicd/_items/_lakehouse.py#L239-L242) missing from `shortcuts.metadata.json` for any Lakehouse that defines at least one. Shortcuts created at runtime (for example by a Spark job) are therefore removed on each deployment.

List such Lakehouses per workspace in `template.externallyManagedShortcuts`:

```json
{
  "template": {
    "itemTypesInScope": ["DataPipeline", "Lakehouse"],
    "featureFlags": ["enable_shortcut_publish"],
    "externallyManagedShortcuts": [
      {
        "lakehouse": "my_lakehouse",
        "pathPrefixes": ["/Tables/dbo/"],
        "reason": "Created at runtime by a Spark job"
      }
    ]
  }
}
```

| Field          | Required | Meaning                                                                                             |
| -------------- | -------- | --------------------------------------------------------------------------------------------------- |
| `lakehouse`    | Yes      | Lakehouse name; must match a `.Lakehouse` in `template.artifactsFolder`, listed once.               |
| `pathPrefixes` | No       | Paths exempt from cleanup, prefix-matched (`/Tables/dbo/<name>`, `/Files/<name>`). Default `["/"]`. |
| `reason`       | No       | Free text, logged when shortcuts are preserved.                                                     |

- Omitted or `[]` keeps upstream behavior. Shortcuts in `shortcuts.metadata.json` are still published.
- Only deployed shortcuts absent from the repository and under a prefix are exempt; other orphans are still deleted.
- Config is validated before publishing: unknown fields, non-string values, duplicates, prefixes not starting with `/`, and unknown Lakehouses fail the deployment. A warning is logged if `Lakehouse` is not in scope or `enable_shortcut_publish` is not set.
- FWD wraps a private fabric-cicd function (only when rules exist, and only for workspaces carrying rules); revisit on upgrade.
- Verify: look for `Preserving N externally managed shortcut(s) on Lakehouse` in the log and no `DELETE .../shortcuts//Tables/...` for preserved paths.
