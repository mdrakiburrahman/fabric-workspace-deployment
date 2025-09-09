# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT
import subprocess
import os
import time

def get_version():
    try:
        timestamp = int(time.time())
        git_root = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], 
                                         cwd=os.path.dirname(__file__)).decode().strip()
        result = subprocess.run(
            'git ls-files | xargs sha256sum | sha256sum | cut -d\' \' -f1 | cut -c1-7',
            shell=True,
            cwd=git_root,
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            version_hash = result.stdout.strip()
            hash_int = int(version_hash, 16)
            return f"{timestamp}.{hash_int}.0"
        else:
            return f"{timestamp}.1.0"
    except:
        return "0.0.1"

__version__ = get_version()
