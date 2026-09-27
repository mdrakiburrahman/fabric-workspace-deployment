#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

assert_equal() {
    local actual="$1"
    local expected="$2"
    local name="$3"
    if [[ "${actual}" != "${expected}" ]]; then
        echo "${name}: expected '${expected}', got '${actual}'." >&2
        exit 1
    fi
    printf '%s: %s\n' "${name}" "${actual}"
}

assert_equal "$(python --version 2>&1)" "Python 3.12.14" "Python"
assert_equal "$(node --version)" "v24.21.0" "Node.js"
assert_equal "$(npm --version)" "11.19.0" "npm"
assert_equal "$(node -p "require('nx/package.json').version")" "23.2.1" "Nx"
npx nx show projects >/dev/null
assert_equal "$(npx devcontainer --version)" "0.89.0" "Dev Container CLI"
assert_equal "$(uv --version | awk '{print $1, $2}')" "uv 0.12.19" "uv"
assert_equal "$(az version --query '"azure-cli"' --output tsv)" "2.90.0" "Azure CLI"

uv run --frozen python - <<'PY'
import importlib.metadata
import json

expected = {
    "artifacts-keyring": "1.0.0",
    "black": "26.5.1",
    "build": "1.6.1",
    "hatch": "1.18.1",
    "hatchling": "1.27.0",
    "keyring": "25.7.0",
    "ms-fabric-cli": "1.7.0",
    "mypy": "2.3.1",
    "pyinstaller": "6.22.3",
    "pytest": "9.1.1",
    "twine": "7.0.0",
}

for distribution_name, expected_version in expected.items():
    distribution = importlib.metadata.distribution(distribution_name)
    actual_version = distribution.version
    if actual_version != expected_version:
        raise SystemExit(f"{distribution_name}: expected {expected_version}, got {actual_version}")
    print(f"{distribution_name}: {actual_version}")

fabric_cli = importlib.metadata.distribution("ms-fabric-cli")
direct_url_text = fabric_cli.read_text("direct_url.json")
if direct_url_text is None:
    raise SystemExit("ms-fabric-cli direct_url.json is missing")
direct_url = json.loads(direct_url_text)
commit_id = direct_url.get("vcs_info", {}).get("commit_id")
expected_commit = "0183fbf1809826040ed4805e6163cb51c1613cb9"
if commit_id != expected_commit:
    raise SystemExit(f"ms-fabric-cli: expected commit {expected_commit}, got {commit_id}")
print(f"ms-fabric-cli commit: {commit_id}")
PY

fab --version
fab auth login --help | grep --fixed-strings -- "--azure-cli" >/dev/null
git --version
