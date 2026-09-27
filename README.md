# Fabric Workspace Deployment

A production-ready Fabric Workspace Deployment application

[![PyPI - Version](https://img.shields.io/pypi/v/fabric-workspace-deployment.svg)](https://pypi.org/project/fabric-workspace-deployment)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/fabric-workspace-deployment.svg)](https://pypi.org/project/fabric-workspace-deployment)

-----

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
- [Logging](docs/LOGGING.md)
- [Headless devcontainer operations](docs/devcontainer/headless-operations.md)

## Development

The repository-owned Ubuntu 24.04 devcontainer is the development and deterministic CI source of truth. It mounts only the WSL host's `~/.azure` directory and intentionally excludes Spark, Delta, Livy, Java, Scala, SBT/Maven, FUSE/blobfuse, ODBC, and Docker-in-Docker.

```console
npm ci
npx nx run devcontainer:build
npx nx run devcontainer:up
npx nx run devcontainer:test
npx nx run devcontainer:down
```

The verification target runs the pytest logging suite, mypy, formatting checks, wheel/sdist packaging, PyInstaller packaging, and offline smoke checks. See [contributor setup](contrib/README.md) for the destructive Windows/WSL bootstrap warning, local authentication, and publishing guidance.

## License

`fabric-workspace-deployment` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
