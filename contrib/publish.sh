#!/usr/bin/env bash
# Publishes the package with a stable version across sdist and wheel.
# Computes PACKAGE_VERSION once so both artifacts share the same version.
set -euo pipefail

readonly AZURE_DEVOPS_RESOURCE_ID="${AZURE_DEVOPS_RESOURCE_ID:-499b84ac-1321-427f-aa17-267ca6975798}"
readonly AZURE_DEVOPS_TENANT_ID="${AZURE_DEVOPS_TENANT_ID:-72f988bf-86f1-41af-91ab-2d7cd011db47}"
readonly GIT_ROOT="$(git rev-parse --show-toplevel)"
export TWINE_USERNAME="${TWINE_USERNAME:-az}"

run_hatch() {
  bash "${GIT_ROOT}/contrib/run-hatch.sh" "$@"
}

if [[ -z "${TWINE_PASSWORD:-}" ]]; then
  if ! command -v az >/dev/null 2>&1; then
    echo "Azure CLI is required to publish without a PAT." >&2
    exit 1
  fi

  if ! az account show >/dev/null 2>&1; then
    echo "Azure CLI is not authenticated. Run: az login --tenant ${AZURE_DEVOPS_TENANT_ID}" >&2
    exit 1
  fi

  TWINE_PASSWORD=$(az account get-access-token --resource "$AZURE_DEVOPS_RESOURCE_ID" --tenant "$AZURE_DEVOPS_TENANT_ID" --query accessToken --output tsv)
  if [[ -z "$TWINE_PASSWORD" ]]; then
    echo "Azure CLI returned an empty Azure DevOps access token." >&2
    exit 1
  fi
  export TWINE_PASSWORD
  echo "Using a short-lived Azure CLI token for Azure Artifacts."
fi

cp .pypirc ~/.pypirc
run_hatch env remove publish

hash_hex=$(git -C "$GIT_ROOT" ls-files | xargs sha256sum | sha256sum | cut -d' ' -f1 | cut -c1-7)
hash_int=$((16#${hash_hex}))
export PACKAGE_VERSION="$(date +%s).${hash_int}.0"

echo "Publishing version: ${PACKAGE_VERSION}"
run_hatch run publish:release
