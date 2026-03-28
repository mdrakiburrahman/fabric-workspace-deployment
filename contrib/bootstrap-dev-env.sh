#!/bin/bash
#
#
#       Sets up a dev env with all pre-reqs. This script is idempotent, it will
#       only attempt to install dependencies, if not exists.   
#
# ---------------------------------------------------------------------------------------
#

set -eou pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
NPMRC_TMPL="$REPO_ROOT/.npmrc.tmpl"
NPMRC="$REPO_ROOT/.npmrc"

echo ""
echo "┌──────────────────────┐"
echo "│ Installing CLI tools │"
echo "└──────────────────────┘"
echo ""

DOCKER_VERSION="5:27.5.1-1~ubuntu.24.04~noble"

if ! [ -x "$(command -v docker)" ]; then
  echo "docker is not installed on your devbox, installing..."
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
  sudo add-apt-repository -y "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
  sudo apt-get update -q
  sudo apt-get install -y apt-transport-https ca-certificates curl
  sudo apt-get install -y --allow-downgrades docker-ce="$DOCKER_VERSION" docker-ce-cli="$DOCKER_VERSION" containerd.io
else
  echo "docker is already installed."
fi

sudo mkdir -p /etc/docker
echo '{"max-concurrent-downloads": 32}' | sudo tee /etc/docker/daemon.json > /dev/null

echo "docker is installed, restarting..."
sudo systemctl reset-failed docker.service 2>/dev/null || true
sudo systemctl restart docker

sudo chmod 666 /var/run/docker.sock
docker container ls
docker ps -q | xargs -r docker kill

# Remove Windows paths from PATH to avoid using Windows az CLI
# This allows us to mount ~/.azure from WSL.
#
export PATH=$(echo "$PATH" | tr ':' '\n' | grep -v "/mnt/c" | tr '\n' ':' | sed 's/:$//')
AZ_PATH=$(which az 2>/dev/null || true)
if [[ -z "$AZ_PATH" || "$AZ_PATH" == *"/mnt/c"* ]]; then
  echo "Native Linux Azure CLI not found, installing..."
  curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
  export PATH="$HOME/bin:$PATH"
  [[ -f "$HOME/.bashrc" ]] && (set +u; source "$HOME/.bashrc") || true
else
  echo "Native Linux Azure CLI already installed at: $AZ_PATH"
fi

if ! [ -x "$(command -v npm)" ]; then
  echo "npm is not installed on your devbox, installing..."
  curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash -
  sudo apt-get update 2>&1 > /dev/null
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y nodejs
else
  echo "npm is already installed."
fi

echo ""
echo "┌────────────────┐"
echo "│ Authentication │"
echo "└────────────────┘"
echo ""

if ! az account get-access-token --query "expiresOn" -o tsv >/dev/null 2>&1; then
    echo "az is not logged in, logging in..."
    az login >/dev/null
fi

cp $NPMRC_TMPL $NPMRC
sed -i "s/_authToken=.*/_authToken=$(az account get-access-token --resource '499b84ac-1321-427f-aa17-267ca6975798' --query accessToken --output tsv --tenant '72f988bf-86f1-41af-91ab-2d7cd011db47')/" $NPMRC

echo ""
echo "┌──────────┐"
echo "│ Versions │"
echo "└──────────┘"
echo ""

echo "Docker: $(docker --version)"
echo "Azure CLI: $(az version)"
echo "npm: $(npm version)"