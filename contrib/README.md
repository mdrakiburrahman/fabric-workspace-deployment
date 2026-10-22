# Contributing

## How to use, on a Linux machine

1. Clone the repo on Windows, then check out and push the branch you want to work on:

   ```powershell
   git clone https://github.com/mdrakiburrahman/fabric-workspace-deployment.git
   ```

1. From an **elevated PowerShell 7** prompt in the checkout, install [Task](https://taskfile.dev) and run [`bootstrap-dev-env.yaml`](bootstrap-dev-env.yaml):

   > ⚠️ This **deletes every WSL distribution** on the machine before recreating `Ubuntu-24.04`. Push any work inside WSL first.

   ```powershell
   winget install Task.Task
   $env:PATH = [Environment]::GetEnvironmentVariable('PATH', 'Machine') + ';' + [Environment]::GetEnvironmentVariable('PATH', 'User')
   task --taskfile contrib/bootstrap-dev-env.yaml
   ```

   Without any prompts, this recreates WSL on the drive with the most free space and clones your current branch into `/workspaces/fabric-workspace-deployment` with this checkout's git identity. It then runs [`bootstrap-dev-env.sh`](bootstrap-dev-env.sh), which signs in the Azure CLI through your signed-in browser with single sign-on (no clicks), then builds and starts the devcontainer and opens it in VS Code. After a restart, run `task --taskfile contrib/bootstrap-dev-env.yaml up` instead; it keeps WSL and reopens the devcontainer.

   Or run the same environment headlessly from `/workspaces/fabric-workspace-deployment` in WSL:

   ```bash
   npx --no-install nx run devcontainer:build
   npx --no-install nx run devcontainer:up
   npx --no-install nx run devcontainer:test
   npx --no-install nx run devcontainer:down
   ```

1. Install GitHub tooling:

   ```bash
   curl -fsSL https://gh.io/copilot-install | bash
   $HOME/.local/bin/copilot --yolo
   ```

1. Install `gh` CLI:

   ```bash
   (type -p wget >/dev/null || (sudo apt update && sudo apt install wget -y)) \
	&& sudo mkdir -p -m 755 /etc/apt/keyrings \
	&& out=$(mktemp) && wget -nv -O$out https://cli.github.com/packages/githubcli-archive-keyring.gpg \
	&& cat $out | sudo tee /etc/apt/keyrings/githubcli-archive-keyring.gpg > /dev/null \
	&& sudo chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
	&& sudo mkdir -p -m 755 /etc/apt/sources.list.d \
	&& echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
	&& sudo apt update \
	&& sudo apt install gh -y

   gh auth login
   ```

1. With an authenticated Azure CLI session, publish:

```bash
npx nx publish
```
