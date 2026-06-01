#!/usr/bin/env bash
# Publishes the package with a stable version across sdist and wheel.
# Computes PACKAGE_VERSION once so both artifacts share the same version.
set -euo pipefail

cp .pypirc ~/.pypirc
hatch env remove publish

git_root=$(git rev-parse --show-toplevel)
hash_hex=$(git -C "$git_root" ls-files | xargs sha256sum | sha256sum | cut -d' ' -f1 | cut -c1-7)
hash_int=$((16#${hash_hex}))
export PACKAGE_VERSION="$(date +%s).${hash_int}.0"

echo "Publishing version: ${PACKAGE_VERSION}"
hatch run publish:release
