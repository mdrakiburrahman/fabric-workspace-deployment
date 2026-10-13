# Bulk publishing

`deployTemplate` and missing-model publishing in `deployModel` use fabric-cicd 1.4.0 with `enable_experimental_features` and `enable_bulk_publish` enabled by default. Existing `template.featureFlags` remain effective; no monitoring configuration flag changes are required.

Bulk mode imports item definitions together and supports filtered dynamic `$items.*` replacements using dependency-aware batches. The existing DataPipeline-before-Reflex ordering and Spark post-processing are preserved. Fabric-cicd logs a fallback to standard publishing when a scope includes `Warehouse` or `DataBuildToolJob`, or a current-workspace `$items.*` replacement has none of the `item_type`, `item_name`, or `file_path` filters.

Bulk publishing is experimental upstream. Items need valid nonzero logical IDs, each request is limited to 1,000 items and 128 MB, and every item must support the deployment identity. API failures and invalid dependencies remain deployment errors, not silent fallbacks. Lakehouse shortcuts, environment activation, model connection bindings, orphan deletion, and custom Spark post-processing still run separately.

See the [fabric-cicd bulk publishing reference](https://microsoft.github.io/fabric-cicd/latest/how_to/optional_feature/#bulk-publish) for compatibility details.
