# Headless devcontainer operations

The repository-owned devcontainer has one explicit host mount: WSL `~/.azure`
at `/home/vscode/.azure`.

```bash
npx --no-install nx run devcontainer:build
npx --no-install nx run devcontainer:up
npx --no-install nx run devcontainer:test
npx --no-install nx run devcontainer:down
```

Authenticate on the host:

```bash
az login
```

Then authenticate Fabric inside the container:

```bash
fab auth login --azure-cli
```
