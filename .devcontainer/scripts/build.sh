#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

bash .devcontainer/scripts/initialize.sh
npx devcontainer build \
    --workspace-folder "${repo_root}" \
    --config "${repo_root}/.devcontainer/devcontainer.json" \
    --frozen-lockfile \
    --image-name fabric-workspace-deployment-devcontainer:local
