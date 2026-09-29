# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging
import os
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

    def _redact(self, value: str | bytes, env: dict[str, str] | None) -> str:
        text = value.decode(errors="replace") if isinstance(value, bytes) else value
        if not env:
            return text
        for key, secret in env.items():
            if "TOKEN" in key.upper() and secret:
                text = text.replace(secret, "******")
        return text
