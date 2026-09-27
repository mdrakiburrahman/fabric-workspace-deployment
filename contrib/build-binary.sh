#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

rm -rf build dist/fabric-workspace-deployment
export PYTHONPATH="${repo_root}/src${PYTHONPATH:+:${PYTHONPATH}}"
uv run --frozen pyinstaller --clean --noconfirm fabric-workspace-deployment.spec
