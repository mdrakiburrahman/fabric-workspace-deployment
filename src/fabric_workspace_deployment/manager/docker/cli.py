# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import json
import logging
import os
import re
import subprocess

from pathlib import Path


class DockerCliError(RuntimeError):
    """Raised when a Docker CLI command exits unsuccessfully."""

    def __init__(self, message: str, *, returncode: int | None = None, stdout: str = "", stderr: str = ""):
        super().__init__(message)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class DockerCli:
    """Small, mockable wrapper around Docker Compose."""

    _CONTAINER_ID_PATTERN = re.compile(r"(?<![0-9a-f])([0-9a-f]{64})(?![0-9a-f])")

    def __init__(self, *, logger: logging.Logger | None = None):
        self.logger = logger or logging.getLogger(__name__)

    def run(self, commands: list[str], *, timeout: int | None = None, env: dict[str, str] | None = None) -> tuple[str, str]:
        """
        Run a Docker command and capture its output.

        Sensitive values are supplied only through the process environment and are
        redacted from diagnostic output.
        """
        command = list(commands)
        if not command or command[0] != "docker":
            command.insert(0, "docker")

        process_env = os.environ.copy()
        if env:
            process_env.update(env)

        self.logger.debug(f"Executing Docker command: {' '.join(command)}")
        try:
            completed = subprocess.run(command, capture_output=True, text=True, timeout=timeout, env=process_env, check=False)  # noqa: S603
        except subprocess.TimeoutExpired as e:
            raise DockerCliError(f"Docker command timed out after {timeout} seconds: {' '.join(command)}", stdout=self._redact(e.stdout or "", env), stderr=self._redact(e.stderr or "", env)) from e
        except FileNotFoundError as e:
            raise DockerCliError("Docker CLI was not found. Install Docker with the Compose plugin and ensure 'docker' is on PATH.") from e

        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
        redacted_stdout = self._redact(stdout, env)
        redacted_stderr = self._redact(stderr, env)
        self.logger.debug(f"Docker stdout: {redacted_stdout}")
        self.logger.debug(f"Docker stderr: {redacted_stderr}")

        if completed.returncode != 0:
            detail = redacted_stderr.strip() or redacted_stdout.strip() or f"exit code {completed.returncode}"
            raise DockerCliError(f"Docker command failed: {detail}", returncode=completed.returncode, stdout=redacted_stdout, stderr=redacted_stderr)

        return stdout, stderr

    def compose_run(self, compose_file: Path, project_name: str, service: str, command: list[str], *, timeout: int | None = None, env: dict[str, str] | None = None) -> tuple[str, str]:
        """Run a one-off command in a packaged Compose service."""
        return self.run(
            [
                "compose",
                "--file",
                str(compose_file),
                "--project-name",
                project_name,
                "run",
                "--rm",
                "--no-deps",
                service,
                *command,
            ],
            timeout=timeout,
            env=env,
        )

    def resolve_daemon_path(self, path: Path) -> Path:
        """Return the Docker daemon-visible path for a path in the current process."""
        resolved_path = path.resolve()
        if not self._is_containerized():
            return resolved_path

        mounts = self._current_container_mounts()
        if mounts is None:
            self.logger.debug("Current container is not visible to the active Docker daemon; using caller-visible path %s", resolved_path)
            return resolved_path

        matches = []
        for mount in mounts:
            destination = mount.get("Destination")
            source = mount.get("Source")
            if not isinstance(destination, str) or not isinstance(source, str) or not destination or not source:
                continue
            destination_path = Path(destination)
            try:
                relative_path = resolved_path.relative_to(destination_path)
            except ValueError:
                continue
            matches.append((len(destination_path.parts), Path(source) / relative_path))

        if not matches:
            raise DockerCliError(f"Path '{resolved_path}' is inside the current container but is not under a mount exposed to the active Docker daemon. Configure the staging root on a bind mount or Docker volume.")

        daemon_path = max(matches, key=lambda match: match[0])[1]
        self.logger.debug("Translated caller-visible Docker path %s to daemon-visible path %s", resolved_path, daemon_path)
        return daemon_path

    def _is_containerized(self) -> bool:
        return Path("/.dockerenv").exists() or Path("/run/.containerenv").exists()

    def _current_container_mounts(self) -> list[dict[str, object]] | None:
        for container_id in self._current_container_candidates():
            try:
                stdout, _ = self.run(["inspect", container_id, "--format", "{{json .Mounts}}"], timeout=30)
            except DockerCliError:
                continue
            try:
                mounts = json.loads(stdout)
            except json.JSONDecodeError as e:
                raise DockerCliError(f"Docker inspect returned invalid mount metadata for current container '{container_id}'") from e
            if not isinstance(mounts, list):
                raise DockerCliError(f"Docker inspect returned invalid mount metadata for current container '{container_id}'")
            return mounts
        return None

    def _current_container_candidates(self) -> list[str]:
        candidates = []
        hostname = os.getenv("HOSTNAME", "").strip()
        if hostname:
            candidates.append(hostname)

        for metadata_path in (Path("/proc/self/cgroup"), Path("/proc/self/mountinfo")):
            try:
                metadata = metadata_path.read_text(encoding="utf-8")
            except OSError:
                continue
            candidates.extend(self._CONTAINER_ID_PATTERN.findall(metadata))

        return list(dict.fromkeys(candidates))

    def _redact(self, value: str | bytes, env: dict[str, str] | None) -> str:
        text = value.decode(errors="replace") if isinstance(value, bytes) else value
        if not env:
            return text
        for key, secret in env.items():
            if "TOKEN" in key.upper() and secret:
                text = text.replace(secret, "******")
        return text
