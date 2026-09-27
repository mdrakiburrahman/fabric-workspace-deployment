#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${PACKAGE_VERSION:-}" ]]; then
    git_root="$(git rev-parse --show-toplevel)"
    hash_hex="$(
        cd "${git_root}"
        while IFS= read -r -d '' tracked_file; do
            if [[ -f "${tracked_file}" ]]; then
                sha256sum "${tracked_file}"
            else
                printf 'deleted  %s\n' "${tracked_file}"
            fi
        done < <(git ls-files -z) | sha256sum | cut -d' ' -f1 | cut -c1-7
    )"
    hash_int=$((16#${hash_hex}))
    export PACKAGE_VERSION="$(date +%s).${hash_int}.0"
fi

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    printf '%s\n' "${PACKAGE_VERSION}"
fi
