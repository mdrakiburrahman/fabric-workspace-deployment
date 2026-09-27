#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

bash .devcontainer/scripts/initialize.sh
up_result="$(
    npx devcontainer up \
        --workspace-folder "${repo_root}" \
        --config "${repo_root}/.devcontainer/devcontainer.json" \
        --frozen-lockfile
)"
printf '%s\n' "${up_result}"

container_id="$(jq -r '.containerId // empty' <<<"${up_result}")"
if [[ -z "${container_id}" ]]; then
    echo "Dev Container CLI did not return a container ID." >&2
    exit 1
fi

docker inspect "${container_id}" --format 'Started {{.Name}} ({{.Id}})'
