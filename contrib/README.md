# Contributing

## How to use, on a Linux machine

1. Get a fresh new WSL machine up:

   ```powershell
   # Delete old WSL
   wsl --unregister Ubuntu-24.04

   # Create new WSL
   wsl --install -d Ubuntu-24.04
   ```

1. Clone the repo, and open VSCode in it:

   ```bash
   cd ~/

   git config --global user.name "Raki Rahman"
   git config --global user.email "mdrakiburrahman@gmail.com"
   git clone https://github.com/mdrakiburrahman/fabric-workspace-deployment.git

   cd fabric-workspace-deployment/
   code .
   ```

1. Reset your docker WSL integration since this is a new VM:

   > `Docker Desktop: Settings > Resources > WSL Integration > Turn off/on Ubuntu-24.04`

1. Run the bootstrapper script, that installs all tools idempotently:

   ```bash
   GIT_ROOT=$(git rev-parse --show-toplevel)
   chmod +x ${GIT_ROOT}/contrib/bootstrap-dev-env.sh && ${GIT_ROOT}/contrib/bootstrap-dev-env.sh
   ```

1. `Ctrl + Shift + P` > `Reopen in Container`

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
