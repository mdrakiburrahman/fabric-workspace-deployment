# Headless devcontainer operations

The repository-owned devcontainer is the Linux development and CI source of truth. It uses a digest-pinned Ubuntu 24.04 base, exact Dev Container Features, a committed feature lock, public npm packages, and a frozen `uv.lock`.

The only explicit host mount is WSL `~/.azure` to `/home/vscode/.azure`. No Docker socket, credential helper, private npm configuration, or external Spark image is mounted or required.

## Pinned toolchain

| Tool | Pin |
| --- | --- |
| Ubuntu-compatible devcontainer base | `sha256:d94c97dd9cacf183d0a6fd12a8e87b526e9e928307674ae9c94139139c0c6eae` |
| Ubuntu package snapshot | `20260927T000000Z` |
| Python | `3.12.14` |
| Node.js / npm | `24.21.0` / `11.19.0` |
| Azure CLI | `2.90.0` |
| `uv` | `0.12.19` |
| Hatch / Hatchling | `1.18.1` / `1.27.0` |
| Black / pytest / mypy | `26.5.1` / `9.1.1` / `2.3.1` |
| PyInstaller | `6.22.3` |
| Twine / keyring / artifacts-keyring | `7.0.0` / `25.7.0` / `1.0.0` |
| Nx / Dev Container CLI | `23.2.1` / `0.89.0` |
| Fabric CLI | `1.7.0` source at commit `0183fbf1809826040ed4805e6163cb51c1613cb9` |

The published `ms-fabric-cli` 1.7.0 wheel predates Azure CLI authentication. The exact upstream source commit is locked because it includes `fab auth login --azure-cli`; the smoke target verifies that flag without authenticating.

## Prerequisites

Run these commands from Ubuntu 24.04 under WSL:

```bash
cd /workspaces/fabric-workspace-deployment
mkdir -p "${HOME}/.azure"
chmod 700 "${HOME}/.azure"
npm ci --ignore-scripts --no-audit --no-fund --registry=https://registry.npmjs.org/
```

`contrib/bootstrap-dev-env.sh` installs the exact host Docker, Node.js, Dev Container CLI, and native Azure CLI versions. It is destructive: it removes every Docker container, volume, and custom network on that WSL distribution while preserving images and BuildKit cache.

## Raw Dev Container CLI flow

Build the locked image:

```bash
npx devcontainer build \
  --workspace-folder "$(pwd)" \
  --config "$(pwd)/.devcontainer/devcontainer.json" \
  --frozen-lockfile \
  --image-name fabric-workspace-deployment-devcontainer:local
```

Start it and capture the exact container ID:

```bash
UP_RESULT="$(
  npx devcontainer up \
    --workspace-folder "$(pwd)" \
    --config "$(pwd)/.devcontainer/devcontainer.json" \
    --frozen-lockfile
)"
printf '%s\n' "${UP_RESULT}"
CONTAINER_ID="$(jq -r '.containerId' <<<"${UP_RESULT}")"
docker inspect "${CONTAINER_ID}" --format '{{.Id}}'
```

Run the complete frozen verification inside that same workspace container:

```bash
npx devcontainer exec \
  --workspace-folder "$(pwd)" \
  --config "$(pwd)/.devcontainer/devcontainer.json" \
  npx nx run fabric-workspace-deployment:verify
```

Run a follow-up command to prove reuse:

```bash
test "${CONTAINER_ID}" = "$(bash .devcontainer/scripts/container-id.sh)"
npx devcontainer exec \
  --workspace-folder "$(pwd)" \
  --config "$(pwd)/.devcontainer/devcontainer.json" \
  bash -lc 'ls -l dist/fabric-workspace-deployment dist/*.whl dist/*.tar.gz reports/pytest.xml'
```

Remove only labeled FWD resources:

```bash
bash .devcontainer/scripts/down.sh
```

The equivalent Nx lifecycle is:

```bash
npx nx run devcontainer:build
npx nx run devcontainer:up
npx nx run devcontainer:test
npx nx run devcontainer:down
```

## Local authenticated use

Authentication is deliberately separate from deterministic build verification.

1. On the WSL host, authenticate the native Linux Azure CLI:

   ```bash
   az login
   ```

2. Start the devcontainer.
3. Inside the devcontainer, reuse the mounted Azure CLI session:

   ```bash
   fab auth login --azure-cli
   ```

CI never runs `az login`, `fab auth login`, a Fabric operation, deployment, or publishing command.
