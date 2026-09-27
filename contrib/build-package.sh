#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

rm -f dist/*.whl dist/*.tar.gz
source contrib/package-version.sh
echo "Building package version ${PACKAGE_VERSION}."
hatch_version_output="$(uv run --frozen hatch version 2>&1)"
resolved_version="$(grep -oE '[0-9]+(\.[0-9]+){2}' <<<"${hatch_version_output}" | tail -n 1)"
if [[ "${resolved_version}" != "${PACKAGE_VERSION}" ]]; then
    echo "Hatch resolved '${resolved_version}', expected '${PACKAGE_VERSION}'." >&2
    exit 1
fi
uv run --frozen python -m build --no-isolation --sdist --wheel
