#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

npm ci --ignore-scripts --no-audit --no-fund --registry=https://registry.npmjs.org/
uv sync --frozen --all-groups --no-install-project
bash contrib/verify-tools.sh
