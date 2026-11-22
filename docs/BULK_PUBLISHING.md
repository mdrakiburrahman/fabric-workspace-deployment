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
- Shortcuts, environment activation, model bindings, and orphan deletion run separately. The legacy Spark post-processing hook runs in nonselective mode.

See the [fabric-cicd reference](https://microsoft.github.io/fabric-cicd/latest/how_to/optional_feature/#bulk-publish).

## Selective item publishing

For `deployTemplate`, set `template.itemsToInclude` to a non-empty list of `name.Type` strings. Each type must also appear in `itemTypesInScope`, and `unpublishOrphans` must be `false`. Omit the field for the existing behavior. The publisher automatically enables fabric-cicd's experimental `enable_items_to_include` feature and skips the legacy Environment Spark settings post-hook in this mode.

To restrict workspace folder publishing as well, set `template.folderPathsToInclude` to a non-empty list of absolute Fabric folder paths such as `/Team/Reports`, without trailing slashes. It requires `itemsToInclude`, cannot be combined with `disable_workspace_folder_publish`, and enables fabric-cicd's `enable_include_folder` feature. The upstream filter includes root items and only items in the exact specified paths, not their subfolders. It may create the specified folders and their ancestors if they are missing; omitting it allows all repository folders to be published.

These filters do not limit workspace fan-out, custom-library staging, or template generation. Set `skipDeploy` on other workspaces and review those configurations separately. Non-empty `sparkJobDefinitions` is rejected in selective mode because its creation and update hooks run outside these filters. Selection is experimental, may omit dependencies, and does not prove that the selected items exist or that the deployment had no other side effects. Feature flags are process-global in fabric-cicd; do not enable `disable_workspace_folder_publish` in any workspace in the same run when using folder inclusion.
