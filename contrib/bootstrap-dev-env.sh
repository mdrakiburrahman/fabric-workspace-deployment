#!/usr/bin/env bash
set -euo pipefail

readonly DOCKER_CE_VERSION="5:29.8.1-1~ubuntu.24.04~noble"
readonly DOCKER_CE_CLI_VERSION="5:29.8.1-1~ubuntu.24.04~noble"
readonly CONTAINERD_VERSION="2.3.6-1~ubuntu.24.04~noble"
readonly DOCKER_BUILDX_VERSION="0.37.1-1~ubuntu.24.04~noble"
readonly DOCKER_COMPOSE_VERSION="5.5.1-1~ubuntu.24.04~noble"
readonly NODE_VERSION="24.21.0-1nodesource1"
readonly DEVCONTAINER_CLI_VERSION="0.89.0"
readonly AZURE_CLI_VERSION="2.90.0-1~noble"

readonly DOCKER_KEY_SHA256="1500c1f56fa9e26b9b8f42452a553675796ade0807cdce11975eb98170b3a570"
readonly NODESOURCE_KEY_SHA256="b42e0321dabdc24e892115da705cf061167eac12a317f23d329862d0aa0a271d"
readonly MICROSOFT_KEY_SHA256="2fa9c05d591a1582a9aba276272478c262e95ad00acf60eaee1644d93941e3c6"

repo_root="$(git rev-parse --show-toplevel)"

if [[ "$(id -u)" -eq 0 ]]; then
    echo "Run this script as a regular WSL user with sudo access, not as root." >&2
    exit 1
fi

source /etc/os-release
if [[ "${ID}" != "ubuntu" || "${VERSION_ID}" != "24.04" ]]; then
    echo "Ubuntu 24.04 is required; detected ${PRETTY_NAME}." >&2
    exit 1
fi

cat <<'WARNING'

DESTRUCTIVE DOCKER RESET
This script removes every Docker container, every Docker volume, and every
custom Docker network on this WSL distribution. Docker images and BuildKit
cache are deliberately preserved.

WARNING

install_key() {
    local url="$1"
    local expected_sha256="$2"
    local ascii_path="$3"
    local keyring_path="$4"

    curl --fail --show-error --silent --location "${url}" | sudo tee "${ascii_path}" >/dev/null
    echo "${expected_sha256}  ${ascii_path}" | sha256sum --check
    sudo gpg --dearmor --yes --output "${keyring_path}" "${ascii_path}"
    sudo rm --force "${ascii_path}"
    sudo chmod a+r "${keyring_path}"
}

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends ca-certificates curl git gnupg jq wslu

sudo install --directory --mode 0755 /etc/apt/keyrings

install_key \
    "https://download.docker.com/linux/ubuntu/gpg" \
    "${DOCKER_KEY_SHA256}" \
    "/etc/apt/keyrings/docker.asc" \
    "/etc/apt/keyrings/docker.gpg"

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu noble stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install --yes --allow-downgrades --allow-change-held-packages \
    "docker-ce=${DOCKER_CE_VERSION}" \
    "docker-ce-cli=${DOCKER_CE_CLI_VERSION}" \
    "containerd.io=${CONTAINERD_VERSION}" \
    "docker-buildx-plugin=${DOCKER_BUILDX_VERSION}" \
    "docker-compose-plugin=${DOCKER_COMPOSE_VERSION}"
sudo apt-mark hold docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

sudo install --directory /etc/docker
sudo tee /etc/docker/daemon.json >/dev/null <<'JSON'
{
  "max-concurrent-downloads": 32,
  "max-concurrent-uploads": 32,
  "default-ulimits": {
    "nofile": { "Name": "nofile", "Hard": 1048576, "Soft": 1048576 },
    "nproc": { "Name": "nproc", "Hard": 1048576, "Soft": 1048576 },
    "memlock": { "Name": "memlock", "Hard": -1, "Soft": -1 }
  },
  "features": { "buildkit": true },
  "log-driver": "json-file",
  "log-opts": { "max-size": "50m", "max-file": "3" }
}
JSON

sudo systemctl enable docker
sudo systemctl reset-failed docker.service 2>/dev/null || true
sudo systemctl restart docker
sudo usermod --append --groups docker "${USER}"
sudo chmod 666 /var/run/docker.sock
docker info >/dev/null

mapfile -t container_ids < <(docker ps --all --quiet)
if ((${#container_ids[@]})); then
    docker rm --force "${container_ids[@]}"
fi

mapfile -t volume_names < <(docker volume ls --quiet)
if ((${#volume_names[@]})); then
    docker volume rm --force "${volume_names[@]}"
fi

mapfile -t custom_network_ids < <(docker network ls --quiet --filter type=custom)
if ((${#custom_network_ids[@]})); then
    docker network rm "${custom_network_ids[@]}"
fi

echo "Docker images and BuildKit cache were preserved."

install_key \
    "https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key" \
    "${NODESOURCE_KEY_SHA256}" \
    "/etc/apt/keyrings/nodesource.asc" \
    "/etc/apt/keyrings/nodesource.gpg"

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_24.x nodistro main" \
    | sudo tee /etc/apt/sources.list.d/nodesource.list >/dev/null

install_key \
    "https://packages.microsoft.com/keys/microsoft.asc" \
    "${MICROSOFT_KEY_SHA256}" \
    "/etc/apt/keyrings/microsoft.asc" \
    "/etc/apt/keyrings/microsoft.gpg"

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/repos/azure-cli/ noble main" \
    | sudo tee /etc/apt/sources.list.d/azure-cli.list >/dev/null

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install --yes --allow-downgrades --allow-change-held-packages \
    "nodejs=${NODE_VERSION}" \
    "azure-cli=${AZURE_CLI_VERSION}"
sudo apt-mark hold nodejs azure-cli

cd "${repo_root}"
npm ci --ignore-scripts --no-audit --no-fund --registry=https://registry.npmjs.org/
sudo ln --symbolic --force "${repo_root}/node_modules/.bin/devcontainer" /usr/local/bin/devcontainer

export PATH="$(printf '%s' "${PATH}" | tr ':' '\n' | grep -Ev '^/mnt/[[:alpha:]]/' | paste -sd ':' -)"
mkdir -p "${HOME}/.azure"
chmod 700 "${HOME}/.azure"
if ! az account show >/dev/null 2>&1; then
    az login
fi

test "$(docker version --format '{{.Client.Version}}')" = "29.8.1"
test "$(node --version)" = "v24.21.0"
test "$(npm --version)" = "11.19.0"
test "$(devcontainer --version)" = "${DEVCONTAINER_CLI_VERSION}"
test "$(az version --query '"azure-cli"' --output tsv)" = "2.90.0"

docker --version
node --version
npm --version
devcontainer --version
az version --output table
