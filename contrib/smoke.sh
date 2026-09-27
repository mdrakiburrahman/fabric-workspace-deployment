#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

test -x dist/fabric-workspace-deployment
dist/fabric-workspace-deployment --help >/dev/null
bash contrib/verify-tools.sh
bash contrib/assert-minimal-image.sh
