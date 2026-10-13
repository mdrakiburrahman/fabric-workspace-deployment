# Fabric Workspace Deployment

A production-ready Fabric Workspace Deployment application

[![PyPI - Version](https://img.shields.io/pypi/v/fabric-workspace-deployment.svg)](https://pypi.org/project/fabric-workspace-deployment)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/fabric-workspace-deployment.svg)](https://pypi.org/project/fabric-workspace-deployment)

---

## Table of Contents

- [Installation](#installation)
- [Documentation](#documentation)
- [License](#license)

## Installation

```console
pip install fabric-workspace-deployment
```

## Documentation

- [Environment variables](docs/ENV_VARS.md)
- [Entitlements](docs/ENTITLEMENTS.md)
- [Fabric RBAC](docs/RBAC.md)
- [Gateway connections, model bindings, and RLS](docs/GATEWAYS_AND_MODELS.md)
- [Logging](docs/LOGGING.md)
- [Rayfin deployments](docs/RAYFIN.md)
- [Headless devcontainer operations](docs/devcontainer/headless-operations.md)
- [Contributing](contrib/README.md)

### Bulk publishing

`deployTemplate` and missing-model publishing in `deployModel` use fabric-cicd 1.4.0 with `enable_experimental_features` and `enable_bulk_publish` enabled by default. Existing `template.featureFlags` remain effective; no monitoring configuration flag changes are required.

Bulk mode imports item definitions together and supports filtered dynamic `$items.*` replacements using dependency-aware batches. The existing DataPipeline-before-Reflex ordering and Spark post-processing are preserved. Fabric-cicd logs a fallback to standard publishing when a scope includes `Warehouse` or `DataBuildToolJob`, or a current-workspace `$items.*` replacement has none of the `item_type`, `item_name`, or `file_path` filters.

Bulk publishing is experimental upstream. Items need valid nonzero logical IDs, each request is limited to 1,000 items and 128 MB, and every item must support the deployment identity. API failures and invalid dependencies remain deployment errors, not silent fallbacks. Lakehouse shortcuts, environment activation, model connection bindings, orphan deletion, and custom Spark post-processing still run separately.

See the [fabric-cicd bulk publishing reference](https://microsoft.github.io/fabric-cicd/latest/how_to/optional_feature/#bulk-publish) for compatibility details.

## License

`fabric-workspace-deployment` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
