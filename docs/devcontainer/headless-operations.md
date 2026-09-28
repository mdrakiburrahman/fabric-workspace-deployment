# Headless Devcontainer Operations

Run these commands from the WSL host. The existing devcontainer configuration mounts the host Azure CLI state from `~/.azure` at `/home/vscode/.azure`, allowing the container to publish with the host identity and no PAT.

> 💡 This guide is great for autonomous agentic operations.

## Health check

Run the host bootstrap once before starting the devcontainer, then authenticate to the Azure tenant that owns the Azure Artifacts feed:

```bash
repo_root="$(git rev-parse --show-toplevel)"
"$repo_root/contrib/bootstrap-dev-env.sh"
az login --tenant 72f988bf-86f1-41af-91ab-2d7cd011db47
```

This installs or validates the host prerequisites, restarts Docker, and intentionally kills all running Docker containers. The signed-in identity must have permission to publish to the `monitoring` feed.

## Publish and keep the devcontainer

```bash
(
  set -euo pipefail
  repo_root="$(git rev-parse --show-toplevel)"
  container_id="$(npx --no-install devcontainer up --workspace-folder "$repo_root" | jq -er '.containerId')"
  npx --no-install devcontainer exec --workspace-folder "$repo_root" --container-id "$container_id" az account show --query user.name --output tsv
  npx --no-install devcontainer exec --workspace-folder "$repo_root" --container-id "$container_id" npx --no-install nx publish
)
```

`up` starts or reuses the container. Both `exec` calls use its exact ID, stream stdout/stderr, and return the command's exit status. The publish script requests a short-lived Azure DevOps token through the mounted Azure CLI session. The container remains running.

## Publish and remove the devcontainer

```bash
(
  set -euo pipefail
  repo_root="$(git rev-parse --show-toplevel)"
  container_id="$(npx --no-install devcontainer up --workspace-folder "$repo_root" | jq -er '.containerId')"
  trap 'docker rm -f "$container_id" >/dev/null' EXIT
  npx --no-install devcontainer exec --workspace-folder "$repo_root" --container-id "$container_id" npx --no-install nx publish
)
```

The `EXIT` trap removes only that container and preserves the command's exit status. Replace the final command as needed; see `devcontainer up --help` and `devcontainer exec --help`.
