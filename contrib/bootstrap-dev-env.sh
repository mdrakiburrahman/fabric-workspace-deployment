#!/bin/bash
#
#
#       Sets up a dev env with all pre-reqs. This script is idempotent, it will
#       only attempt to install dependencies, if not exists.   
#
# ---------------------------------------------------------------------------------------
#

set -e
set -m

REPO_ROOT=$(git rev-parse --show-toplevel)
NPMRC_TMPL="$REPO_ROOT/.npmrc.tmpl"
NPMRC="$REPO_ROOT/.npmrc"

echo ""
echo "┌──────────────────────┐"
echo "│ Installing CLI tools │"
echo "└──────────────────────┘"
echo ""

if ! command -v docker &> /dev/null; then
    echo "docker not found - installing..."
    curl -sL https://get.docker.com | sudo bash
fi
sudo chmod 666 /var/run/docker.sock

if ! command -v az &> /dev/null; then
    echo "az not found - installing..."
    curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
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

az account get-access-token --query "expiresOn" -o tsv >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
    echo "az is not logged in, logging in..."
    az login --use-device-code >/dev/null
fi

az acr login --name monitoringdev

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