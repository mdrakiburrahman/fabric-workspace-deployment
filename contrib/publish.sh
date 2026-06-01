#!/usr/bin/env bash
# Publishes the package with a stable version across sdist and wheel.
# Computes PACKAGE_VERSION once so both artifacts share the same version.
set -euo pipefail

cp .pypirc ~/.pypirc
hatch env remove publish

export PACKAGE_VERSION
PACKAGE_VERSION=$(python -c "
import time, subprocess
git_root = subprocess.check_output(['git', 'rev-parse', '--show-toplevel']).decode().strip()
result = subprocess.run(
    'git ls-files | xargs sha256sum | sha256sum | cut -d\" \" -f1 | cut -c1-7',
    shell=True, cwd=git_root, capture_output=True, text=True
)
hash_int = int(result.stdout.strip(), 16) if result.returncode == 0 else 1
print(f'{int(time.time())}.{hash_int}.0')
")

echo "Publishing version: ${PACKAGE_VERSION}"
hatch run publish:release
