# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging
import sys
from subprocess import PIPE, Popen, TimeoutExpired
import os


class FabricCli:
    """
    A Fabric CLI wrapper.
    """

    def __init__(self, *, exit_on_error: bool = True, logger: logging.Logger | None = None):
        """
        Initialize the FabricCli wrapper.

        Args:
            exit_on_error: Whether to raise exceptions on command failure
            logger: Optional logger instance. If None, creates a default logger.
        """
        self.exit_on_error = exit_on_error
        self.logger = logger or logging.getLogger(__name__)

    def run(self, commands: list[str], timeout: int | None = None, env: dict[str, str] | None = None) -> tuple[str, str]:
        """
        Execute a command with the given arguments.

        Args:
            commands: List of command arguments
            timeout: Optional timeout in seconds
            env: Optional environment variables

        Returns:
            Tuple of (stdout, stderr) as strings

        Raises:
            Exception: If command fails and exit_on_error is True
            TimeoutExpired: If command times out
        """
        if not commands or commands[0] != "fab":
            commands.insert(0, "fab")

        fab_path = os.getenv("FAB_PATH")
        if fab_path:
            commands[0] = os.path.join(fab_path, "fab")

        self.logger.debug(f"Executing command: {' '.join(commands)}")
        proc = Popen(commands, stdout=PIPE, stderr=PIPE, env=env)  # noqa: S603

        try:
            (stdout, stderr) = proc.communicate(timeout=timeout)
        except TimeoutExpired:
            self.logger.warning(f"Command execution timeout: {timeout}")
            proc.kill()
            (stdout, stderr) = proc.communicate(timeout=timeout)
            sys.stdout.buffer.write(stdout)
            sys.stderr.buffer.write(stderr)
            raise

        self.logger.debug(f"Exited command: {' '.join(commands)}")

        stdout_str = stdout.decode(sys.stdout.encoding)
        stderr_str = stderr.decode(sys.stderr.encoding)

        self.logger.debug(f"Stdout: {stdout_str}")
        self.logger.debug(f"Stderr: {stderr_str}")

        if proc.returncode != 0 and self.exit_on_error:
            raise Exception(stderr_str)

        return (stdout_str, stderr_str)

    def run_fab(self, command_string: str, timeout: int | None = None) -> tuple[str, str]:
        """
        Execute a fabric command from a command string.

        Args:
            command_string: Space-separated command string
            timeout: Optional timeout in seconds

        Returns:
            Tuple of (stdout, stderr) as strings
        """
        cmd = command_string.split()
        return self.run(cmd, timeout)

    def run_command(self, command: str, timeout: int | None = None) -> str:
        """
        Execute a fabric command and return only stdout.

        Args:
            command: Fabric command to execute (without 'fab' prefix)
            timeout: Optional timeout in seconds

        Returns:
            stdout as string

        Raises:
            Exception: If command fails and exit_on_error is True
        """
        try:
            stdout, stderr = self.run_fab(f"fab {command}", timeout)
            return stdout.strip()
        except Exception as e:
            self.logger.error(f"Error running Fabric CLI command: {command}")
            if hasattr(e, "stderr"):
                self.logger.error(e.stderr)
            if self.exit_on_error:
                raise
            return str(e).strip()
