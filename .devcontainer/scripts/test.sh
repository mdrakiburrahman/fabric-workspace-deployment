#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

container_id="$(bash .devcontainer/scripts/container-id.sh)"
echo "Running verification in container ${container_id}."

container_workspace="$(
    docker inspect "${container_id}" \
        | jq -r --arg source "${repo_root}" '.[0].Mounts[] | select(.Source == $source) | .Destination'
)"
if [[ -z "${container_workspace}" ]]; then
    echo "Could not resolve the repository mount in container ${container_id}." >&2
    exit 1
fi

docker exec \
    --user vscode \
    --workdir "${container_workspace}" \
    "${container_id}" \
    bash -lc 'export PATH="$PWD/.venv/bin:$PATH"; export PYTHONPATH="$PWD/src"; npx nx run fabric-workspace-deployment:verify'
