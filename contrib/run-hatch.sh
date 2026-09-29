#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
# SPDX-License-Identifier: MIT
set -euo pipefail

readonly HATCH_VERSION="${HATCH_VERSION:-1.18.1}"
readonly REPO_ROOT="$(git -C "$(dirname -- "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
readonly UV_TOOL_BIN="${UV_TOOL_BIN_DIR:-${HOME}/.local/bin}"

export PATH="${UV_TOOL_BIN}:/usr/local/python/current/bin:/usr/local/share/nvm/current/bin:${PATH}"

hatch_path="$(command -v hatch || true)"
if [[ -z "$hatch_path" || ! -x "$hatch_path" ]]; then
  if ! command -v uv >/dev/null 2>&1; then
    echo "Hatch is unavailable and uv is required to install hatch==${HATCH_VERSION}." >&2
    exit 1
  fi

  echo "Hatch is unavailable; installing hatch==${HATCH_VERSION} with uv."
  uv tool install --force --python python3.12 "hatch==${HATCH_VERSION}"
  hatch_path="$(command -v hatch || true)"
fi

if [[ -z "$hatch_path" || ! -x "$hatch_path" ]]; then
  echo "Hatch installation completed without creating an executable in ${UV_TOOL_BIN}." >&2
  exit 1
fi

if [[ -e "${REPO_ROOT}/.venv" ]] && ! "${REPO_ROOT}/.venv/bin/python" -c 'import sys' >/dev/null 2>&1; then
  echo "Removing stale project virtual environment that is not executable in this runtime."
  rm -rf "${REPO_ROOT}/.venv"
fi

unset VIRTUAL_ENV
unset UV_PROJECT_ENVIRONMENT
unset HATCH_ENV_ACTIVE

exec env -u VIRTUAL_ENV -u UV_PROJECT_ENVIRONMENT -u HATCH_ENV_ACTIVE "$hatch_path" "$@"
