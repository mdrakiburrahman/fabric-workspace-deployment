#!/usr/bin/env bash
set -euo pipefail

label="com.github.mdrakiburrahman.fabric-workspace-deployment.devcontainer=true"
mapfile -t container_ids < <(docker ps --no-trunc --quiet --filter "label=${label}")

if [[ "${#container_ids[@]}" -ne 1 ]]; then
    echo "Expected exactly one running FWD devcontainer, found ${#container_ids[@]}." >&2
    exit 1
fi

printf '%s\n' "${container_ids[0]}"
