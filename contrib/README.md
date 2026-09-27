# Contributing

Development uses Ubuntu 24.04 under WSL and the repository-owned devcontainer.

## Destructive bootstrap boundary

Both bootstrap scripts intentionally follow the Monitoring reset contract:

- `bootstrap-dev-env.ps1` removes Docker Desktop, unregisters **every** WSL distribution, rewrites `%USERPROFILE%\.wslconfig`, configures Windows Defender exclusions, and creates a new Ubuntu 24.04 distribution.
- `bootstrap-dev-env.sh` removes **every** Docker container, volume, and custom network in that WSL distribution. Docker images and BuildKit cache are preserved.

Do not run either script on a machine containing state you need. These scripts are never invoked by devcontainer startup, Nx verification, or CI.

## Rebuild Windows and WSL

Run PowerShell 7 as Administrator:

```powershell
Set-Location C:\path\to\fabric-workspace-deployment
.\contrib\bootstrap-dev-env.ps1
```

After Ubuntu 24.04 is created, clone the repository in WSL:

```bash
sudo mkdir -p /workspaces
sudo chown "${USER}:${USER}" /workspaces
cd /workspaces
git clone https://github.com/mdrakiburrahman/fabric-workspace-deployment.git
cd fabric-workspace-deployment
```

## Bootstrap Ubuntu 24.04

The Linux bootstrap installs exact Docker, Node.js, `@devcontainers/cli`, and native Azure CLI versions from their public repositories, resets Docker state, installs the public npm lock, and runs `az login` for local use.

```bash
bash contrib/bootstrap-dev-env.sh
```

It does not pull the old external Spark image and does not create a private npm token or `.npmrc`.

## Devcontainer lifecycle

```bash
npx nx run devcontainer:build
npx nx run devcontainer:up
npx nx run devcontainer:test
npx nx run devcontainer:down
```

See [headless devcontainer operations](../docs/devcontainer/headless-operations.md) for the equivalent raw Dev Container CLI commands, exact container reuse check, and scoped teardown.

## Development targets

Run these inside the devcontainer:

```bash
npx nx run fabric-workspace-deployment:sync
npx nx run fabric-workspace-deployment:format
npx nx run fabric-workspace-deployment:format-check
npx nx run fabric-workspace-deployment:test
npx nx run fabric-workspace-deployment:type-check
npx nx run fabric-workspace-deployment:build:package
npx nx run fabric-workspace-deployment:build:binary
npx nx run fabric-workspace-deployment:build
npx nx run fabric-workspace-deployment:smoke
npx nx run fabric-workspace-deployment:verify
```

`verify` is deterministic and non-live. It performs frozen dependency sync, format checking, pytest, mypy, wheel/sdist build, PyInstaller build, binary/tool smoke checks, and confirms the excluded Spark/Java/Scala/Livy/FUSE/ODBC/Docker toolchains are absent.

## Local authentication and deployment

The only explicit host mount is `~/.azure`.

```bash
# WSL host
az login

# Inside the devcontainer
fab auth login --azure-cli
```

After both commands succeed, run an end-user operation with the binary or console script. Never add login, deployment, or publishing to deterministic CI.

## Internal ADO PyPI publishing

Publishing still targets the `monitoring` repository in `.pypirc`. Supply credentials through Twine-compatible environment variables, then run:

```bash
export TWINE_USERNAME="msdata"
export TWINE_PASSWORD="<ADO PAT>"
npx nx run fabric-workspace-deployment:publish
```

The publish script computes `PACKAGE_VERSION` once, resolves it through Hatch, builds matching wheel and sdist versions with the locked Hatchling backend, and uploads them with Twine and `artifacts-keyring`.
