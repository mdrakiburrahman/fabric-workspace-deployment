# Contributing

## How to use, on a Linux machine

1. Get a fresh new WSL machine up:

   ```powershell
   $GIT_ROOT = git rev-parse --show-toplevel
   & "$GIT_ROOT\contrib\bootstrap-dev-env.ps1"
   ```

1. Clone the repo, and open VSCode in it:

   ```bash
   sudo mkdir -p /workspaces && sudo chmod 777 /workspaces && cd /workspaces

   git config --global user.name "Raki Rahman"
   git config --global user.email "mdrakiburrahman@gmail.com"
   git clone https://github.com/mdrakiburrahman/fabric-workspace-deployment.git

   cd fabric-workspace-deployment/
   code .
   ```

1. Run the bootstrapper script, that installs all tools idempotently:

   ```bash
   GIT_ROOT=$(git rev-parse --show-toplevel)
   chmod +x ${GIT_ROOT}/contrib/bootstrap-dev-env.sh && ${GIT_ROOT}/contrib/bootstrap-dev-env.sh
   ```

1. Launch devcontainer:

   ```bash
   cd /workspaces/fabric-workspace-deployment
   HEX=$(printf '%s' "$(wslpath -w .)" | xxd -ps -c 256)
   code --folder-uri "vscode-remote://dev-container+${HEX}/workspaces/fabric-workspace-deployment"
   ```

1. Install GitHub tooling:

   ```bash
   curl -fsSL https://gh.io/copilot-install | bash
   $HOME/.local/bin/copilot --yolo

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

1. If ADO gives you a hard time, generate a PAT:

```bash
read -sp "Enter TWINE_PASSWORD: " TWINE_PASSWORD
echo
export TWINE_PASSWORD
export TWINE_USERNAME="msdata"

cat > "$(git rev-parse --show-toplevel)/.env.publish" << EOF
TWINE_USERNAME=${TWINE_USERNAME}
TWINE_PASSWORD=${TWINE_PASSWORD}
UV_EXTRA_INDEX_URL=https://${TWINE_USERNAME}:${TWINE_PASSWORD}@msdata.pkgs.visualstudio.com/Tina/_packaging/monitoring/pypi/simple/
PIP_EXTRA_INDEX_URL=https://${TWINE_USERNAME}:${TWINE_PASSWORD}@msdata.pkgs.visualstudio.com/Tina/_packaging/monitoring/pypi/simple/
EOF

npx nx publish
```
