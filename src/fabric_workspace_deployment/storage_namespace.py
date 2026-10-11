# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable

_BINARY_SUFFIXES = {
    ".7z",
    ".avro",
    ".bz2",
    ".dll",
    ".gz",
    ".ico",
    ".jar",
    ".jpeg",
    ".jpg",
    ".parquet",
    ".pdf",
    ".png",
    ".pyc",
    ".so",
    ".tar",
    ".tgz",
    ".whl",
    ".xz",
    ".zip",
}


def normalize_storage_relative_path(value: str, field_name: str) -> str:
    """Normalize and validate a storage-relative POSIX path."""
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")

    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} cannot be empty")
    if "\\" in normalized:
        raise ValueError(f"{field_name} must use POSIX '/' separators")
    if normalized.startswith("/"):
        raise ValueError(f"{field_name} must be relative")

    normalized = normalized.rstrip("/")
    parts = normalized.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise ValueError(f"{field_name} cannot contain empty, '.' or '..' path segments")

    return PurePosixPath(*parts).as_posix()


def join_storage_path(path_prefix: str, file_path: str) -> str:
    """Join a namespace prefix and logical storage path."""
    normalized_prefix = normalize_storage_relative_path(path_prefix, "deploymentNamespace.pathPrefix")
    normalized_file_path = normalize_storage_relative_path(file_path, "storageAccountFile.filePath")
    return f"{normalized_prefix}/{normalized_file_path}"


@dataclass(frozen=True)
class DeploymentNamespace:
    """Optional namespace applied to a storage entry's seeded deployment files."""

    path_prefix: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "path_prefix", normalize_storage_relative_path(self.path_prefix, "deploymentNamespace.pathPrefix"))


@dataclass(frozen=True)
class SeedPathBinding:
    """A logical seeded file and its storage namespace."""

    account: str
    container: str
    logical_path: str
    path_prefix: str

    def __post_init__(self) -> None:
        if not self.account:
            raise ValueError("Storage account cannot be empty")
        if not self.container:
            raise ValueError("Storage container cannot be empty")
        object.__setattr__(self, "logical_path", normalize_storage_relative_path(self.logical_path, "storageAccountFile.filePath"))
        object.__setattr__(self, "path_prefix", normalize_storage_relative_path(self.path_prefix, "deploymentNamespace.pathPrefix"))

    @property
    def effective_path(self) -> str:
        """Return the namespaced destination path."""
        return join_storage_path(self.path_prefix, self.logical_path)


class StorageNamespaceRewriter:
    """Resolve namespaced seed destinations and rewrite exact references to them."""

    def __init__(self, bindings: Iterable[SeedPathBinding]):
        self._effective_paths: dict[tuple[str, str, str], str] = {}
        replacements: dict[str, str] = {}
        final_destinations: set[tuple[str, str, str]] = set()

        for binding in bindings:
            key = (binding.account.casefold(), binding.container.casefold(), binding.logical_path)
            if key in self._effective_paths:
                raise ValueError(f"Duplicate seeded storage path: {binding.account}/{binding.container}/{binding.logical_path}")

            destination_key = (binding.account.casefold(), binding.container.casefold(), binding.effective_path)
            if destination_key in final_destinations:
                raise ValueError(f"Duplicate namespaced seed destination: {binding.account}/{binding.container}/{binding.effective_path}")

            self._effective_paths[key] = binding.effective_path
            final_destinations.add(destination_key)

            for source, target in self._reference_pairs(binding):
                current_target = replacements.get(source)
                if current_target is not None and current_target != target:
                    raise ValueError(f"Ambiguous seeded storage reference '{source}' maps to both '{current_target}' and '{target}'")
                replacements[source] = target

        self._replacements = tuple(sorted(replacements.items(), key=lambda pair: len(pair[0]), reverse=True))

    @property
    def active(self) -> bool:
        """Return whether any namespace bindings are configured."""
        return bool(self._effective_paths)

    def effective_path(self, account: str, container: str, logical_path: str) -> str:
        """Return the effective destination for one seed file."""
        normalized_path = normalize_storage_relative_path(logical_path, "storageAccountFile.filePath")
        return self._effective_paths.get((account.casefold(), container.casefold(), normalized_path), normalized_path)

    def rewrite_text(self, content: str) -> str:
        """Rewrite exact references to namespaced seed files."""
        rewritten = content
        for source, target in self._replacements:
            rewritten = rewritten.replace(source, target)
        return rewritten

    def materialize_file(self, source: Path, destination: Path) -> Path:
        """Write a rewritten text copy when needed, otherwise return the source path."""
        if not self.active or self._is_binary(source):
            return source

        try:
            content = source.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            return source

        rewritten = self.rewrite_text(content)
        if rewritten == content:
            return source

        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(rewritten, encoding="utf-8")
        shutil.copystat(source, destination)
        return destination

    def copy_tree(self, source: Path, destination: Path) -> int:
        """Copy a tree and rewrite every UTF-8 file in the copied tree."""
        shutil.copytree(source, destination)
        rewritten_count = 0

        for path in destination.rglob("*"):
            if not path.is_file() or self._is_binary(path):
                continue
            try:
                content = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue

            rewritten = self.rewrite_text(content)
            if rewritten == content:
                continue

            path.write_text(rewritten, encoding="utf-8")
            rewritten_count += 1

        return rewritten_count

    @staticmethod
    def _is_binary(path: Path) -> bool:
        return path.suffix.casefold() in _BINARY_SUFFIXES

    @staticmethod
    def _reference_pairs(binding: SeedPathBinding) -> tuple[tuple[str, str], ...]:
        logical_path = binding.logical_path
        effective_path = binding.effective_path
        account = binding.account
        container = binding.container

        return (
            (
                f"abfss://{container}@{account}.dfs.core.windows.net/{logical_path}",
                f"abfss://{container}@{account}.dfs.core.windows.net/{effective_path}",
            ),
            (
                f"https://{account}.dfs.core.windows.net/{container}/{logical_path}",
                f"https://{account}.dfs.core.windows.net/{container}/{effective_path}",
            ),
            (
                f"https://{account}.blob.core.windows.net/{container}/{logical_path}",
                f"https://{account}.blob.core.windows.net/{container}/{effective_path}",
            ),
            (
                f"{container}/{logical_path}",
                f"{container}/{effective_path}",
            ),
        )
