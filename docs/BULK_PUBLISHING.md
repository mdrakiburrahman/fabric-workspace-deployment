# Bulk publishing

`deployTemplate` and missing-model publishing in `deployModel` use fabric-cicd's bulk import API only when opted in. The default is standard (sequential) publishing.

Opt in per workspace under `common.fabric.workspaces[].template`:

```json
{
  "template": {
    "bulkPublish": true,
    "itemTypesInScope": ["DataPipeline", "Lakehouse"]
  }
}
```

- Omitted or `false` keeps standard publishing; non-boolean values are rejected.
- `enable_bulk_publish` in `template.featureFlags` is ignored (with a warning); the workspace opt-in is authoritative, so other workspaces in the same run are unaffected.
- Bulk publishing is experimental upstream and not guaranteed to be faster. Compare end-to-end timings on identical scope.

## Behavior

- Scoped `.platform` logical IDs are validated locally (UUID format, nonzero, unique). Invalid metadata fails with the file path; FWD never rewrites IDs or silently falls back.
- Existing lakehouses keep their current default schema in the bulk payload, matching sequential publishing.
- Spark Job Definition attachment IDs are resolved like sequential publishing. For dependencies that don't exist yet, use filtered `$items.*` replacements.
- fabric-cicd falls back to standard publishing for `Warehouse`/`DataBuildToolJob` scopes, or unfiltered current-workspace `$items.*` replacements.
- Limits: 1,000 items and 128 MB per request. API failures remain deployment errors.
- Shortcuts, environment activation, model bindings, orphan deletion, and Spark post-processing still run separately.

See the [fabric-cicd reference](https://microsoft.github.io/fabric-cicd/latest/how_to/optional_feature/#bulk-publish).
