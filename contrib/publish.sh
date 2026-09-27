#!/usr/bin/env bash
# Publishes the package with a stable version across sdist and wheel.
# Computes PACKAGE_VERSION once so both artifacts share the same version.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

bash contrib/build-package.sh
uv run --frozen twine upload --non-interactive --disable-progress-bar --config-file .pypirc --repository monitoring dist/*.whl dist/*.tar.gz
