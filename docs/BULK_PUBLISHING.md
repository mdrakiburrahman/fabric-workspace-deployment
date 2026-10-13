# Bulk publishing

`deployTemplate` and missing-model publishing in `deployModel` use fabric-cicd 1.4.0. Publishing is standard (sequential) by default. Opt in independently for each workspace using `template.bulkPublish`:

```json
{
  "template": {
    "bulkPublish": true,
    "itemTypesInScope": ["DataPipeline", "Lakehouse"]
  }
}
```

This is a fragment of a workspace configuration; retain the other required template fields. Omitted or `false` selects standard publishing; non-boolean values are rejected. `template.featureFlags` remains effective for other upstream features, but `enable_bulk_publish` there does not substitute for the workspace opt-in. Workspace-level gating prevents upstream's process-global bulk flag from enabling bulk on other workspaces in the same invocation.

To explicitly opt out:

```json
{
  "template": {
    "bulkPublish": false,
    "itemTypesInScope": ["DataPipeline", "Lakehouse"]
  }
}
```

To use the default, omit `bulkPublish`:

```json
{
  "template": {
    "itemTypesInScope": ["DataPipeline", "Lakehouse"]
  }
}
```

Set this independently under each entry in `common.fabric.workspaces[].template`, not at the top level or in `common`. For example, ETL can opt in while Engineering and Insights retain standard publishing.

Bulk publishing is not guaranteed to be faster. Compare successful runs with identical workspace, item scope, artifact contents, and shortcut configuration. Include preparation and post-processing in end-to-end timings, distinguish those from bulk-import long-running-operation time, and report failures or upstream fallbacks separately.

Bulk mode imports item definitions together and supports filtered dynamic `$items.*` replacements using dependency-aware batches. The existing DataPipeline-before-Reflex ordering and Spark post-processing are preserved. Fabric-cicd logs a fallback to standard publishing when a scope includes `Warehouse` or `DataBuildToolJob`, or a current-workspace `$items.*` replacement has none of the `item_type`, `item_name`, or `file_path` filters.

Bulk publishing is experimental upstream. Items need valid nonzero logical IDs, each request is limited to 1,000 items and 128 MB, and every item must support the deployment identity. API failures and invalid dependencies remain deployment errors, not silent fallbacks. Lakehouse shortcuts, environment activation, model connection bindings, orphan deletion, and custom Spark post-processing still run separately.

Before bulk publishing, FWD validates scoped items' `.platform` logical IDs for UUID format, nonzero values, and workspace uniqueness. Invalid metadata fails locally with the offending file path instead of Fabric's generic "system files could not be interpreted" response. Correct the source metadata explicitly; FWD does not regenerate logical IDs or silently switch to sequential publishing, since logical IDs can be referenced by other items.

For existing lakehouses, FWD reads their current default schemas from the paginated Lakehouse API and preserves those values in the required `lakehouse.metadata.json` bulk payload, logging the preserved state. Schema-less lakehouses retain metadata without `defaultSchema`. This matches sequential publishing: deployment cannot migrate an existing schema-less lakehouse to schemas or change its default schema. New lakehouses retain their source schema metadata, and shortcut publishing still runs for both new and existing lakehouses. Source artifacts are not modified, and failures to read existing schema state remain deployment errors.

Spark Job Definition attachment IDs in `SparkJobDefinitionV1.json` are resolved to deployed item IDs using the same logical-ID replacement as sequential publishing. The workload does not accept an unresolved logical ID for its default lakehouse attachment. For dependencies that do not exist yet, use filtered dynamic `$items.*` replacements so fabric-cicd can publish the dependency before resolving its deployed ID. Missing deployed dependencies remain errors rather than skipped attachments.

See the [fabric-cicd bulk publishing reference](https://microsoft.github.io/fabric-cicd/latest/how_to/optional_feature/#bulk-publish) for compatibility details.
