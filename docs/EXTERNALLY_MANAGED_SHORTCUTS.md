# Externally managed shortcuts

`deployTemplate` publishes Lakehouse shortcuts when `template.featureFlags` contains `enable_shortcut_publish`. For every Lakehouse that defines at least one shortcut in `shortcuts.metadata.json`, [fabric-cicd 1.4.0 deletes every deployed shortcut that is not in that file](https://github.com/microsoft/fabric-cicd/blob/v1.4.0/src/fabric_cicd/_items/_lakehouse.py#L239-L242). No fabric-cicd feature flag disables this, and `shortcut_exclude_regex` only filters shortcuts defined in the repository from publishing. Shortcuts that a workload creates at runtime, for example table shortcuts created by a Spark driver, are therefore deleted by every deployment.

List those Lakehouses in `template.externallyManagedShortcuts`:

```json
{
  "template": {
    "itemTypesInScope": ["DataPipeline", "Lakehouse"],
    "featureFlags": ["enable_shortcut_publish"],
    "externallyManagedShortcuts": [
      {
        "lakehouse": "protobus_db_eh",
        "pathPrefixes": ["/Tables/dbo/"],
        "reason": "Created at runtime by MirrorMakerBronzeDriver"
      }
    ]
  }
}
```

This is a fragment of a workspace configuration; retain the other required template fields. Set it independently under each entry in `common.fabric.workspaces[].template`. Omitted or `[]` keeps the upstream behavior.

| Field          | Required | Meaning                                                                                                                       |
| -------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `lakehouse`    | Yes      | Lakehouse display name. Must match a `.Lakehouse` item in `template.artifactsFolder`, and appear once.                        |
| `pathPrefixes` | No       | Shortcut paths to exempt, matched as prefixes against `/Tables/dbo/<name>` or `/Files/<name>`. Defaults to `["/"]` (all).     |
| `reason`       | No       | Free text, because JSON has no comments. Logged when shortcuts are preserved.                                                 |

Shortcuts defined in `shortcuts.metadata.json` are still published, including under a preserved prefix. Only deployed shortcuts that are absent from the repository and whose path starts with a prefix are exempt from cleanup; every other orphan is deleted as before. With the minimal entry `{ "lakehouse": "protobus_db_eh" }` the whole Lakehouse is exempt.

The configuration is validated before any item is published. Unknown fields, non-string values, duplicate Lakehouses, prefixes that do not start with `/`, and Lakehouses that are not in the artifacts folder fail the deployment, because a typo would otherwise silently re-enable deletion. A warning is logged when the rules cannot take effect because `Lakehouse` is not in `template.itemTypesInScope` or `enable_shortcut_publish` is not in `template.featureFlags`.

fabric-cicd lists deployed shortcuts through a module-level function, so FWD wraps it once per process and removes the preserved paths from the cleanup candidates. The wrapper only acts on workspaces that carry rules, so other workspaces in the same invocation keep upstream behavior. It is installed only when at least one rule is configured. The wrapper depends on a private fabric-cicd function; revisit it when upgrading fabric-cicd.

To verify, search the deployment log for `Preserving N externally managed shortcut(s) on Lakehouse` and confirm there is no `DELETE .../shortcuts//Tables/...` request for the preserved paths.
