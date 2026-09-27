#!/usr/bin/env bash
set -euo pipefail

label="com.github.mdrakiburrahman.fabric-workspace-deployment.devcontainer=true"

mapfile -t container_ids < <(docker ps --all --quiet --filter "label=${label}")
if ((${#container_ids[@]})); then
    docker rm --force "${container_ids[@]}"
fi

mapfile -t volume_names < <(docker volume ls --quiet --filter "label=${label}")
if ((${#volume_names[@]})); then
    docker volume rm --force "${volume_names[@]}"
fi

mapfile -t network_ids < <(docker network ls --quiet --filter "label=${label}")
if ((${#network_ids[@]})); then
    docker network rm "${network_ids[@]}"
fi
