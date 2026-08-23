# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import os
import runpy
import subprocess
import time

PACKAGE_VERSION_ENV_VAR = runpy.run_path(os.path.join(os.path.dirname(__file__), "environment_variables.py"))["PACKAGE_VERSION_ENV_VAR"]


def get_version():
    # Allow CI to pin a stable version via environment variable, ensuring
    # sdist and wheel get the same version in a single build invocation.
    env_version = os.environ.get(PACKAGE_VERSION_ENV_VAR)
    if env_version:
        return env_version

    try:
        timestamp = int(time.time())
        git_root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], cwd=os.path.dirname(__file__)).decode().strip()
        result = subprocess.run("git ls-files | xargs sha256sum | sha256sum | cut -d' ' -f1 | cut -c1-7", shell=True, cwd=git_root, capture_output=True, text=True)
        if result.returncode == 0:
            version_hash = result.stdout.strip()
            hash_int = int(version_hash, 16)
            return f"{timestamp}.{hash_int}.0"
        else:
            return f"{timestamp}.1.0"
    except:
        return "0.0.1"


__version__ = get_version()
