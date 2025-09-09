# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import base64
import functools
import json
import logging
import sys
from subprocess import PIPE, Popen, TimeoutExpired


class AzCli:
    """
    An Azure CLI wrapper.
    """

    def __init__(self, *, exit_on_error: bool = True, logger: logging.Logger | None = None):
        """
        Initialize the AzCli wrapper.

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
        if not commands or commands[0] != "az":
            commands.insert(0, "az")

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

        if proc.returncode != 0 and self.exit_on_error:
            raise Exception(stderr.decode(sys.stderr.encoding))

        return (stdout.decode(sys.stdout.encoding), stderr.decode(sys.stderr.encoding))

    def run_az(self, command_string: str, timeout: int | None = None) -> tuple[str, str]:
        """
        Execute an Azure CLI command from a command string.

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
        Execute an Azure CLI command and return only stdout.

        Args:
            command: Azure CLI command to execute (without 'az' prefix)
            timeout: Optional timeout in seconds

        Returns:
            stdout as string

        Raises:
            Exception: If command fails and exit_on_error is True
        """
        try:
            stdout, stderr = self.run_az(f"az {command}", timeout)
            return stdout.strip()
        except Exception as e:
            self.logger.error(f"Error running Azure CLI command: {command}")
            if hasattr(e, "stderr"):
                self.logger.error(e.stderr)
            if self.exit_on_error:
                raise
            return str(e).strip()

    @functools.cache  # noqa: B019
    def get_access_token(self, scope: str) -> str:
        """
        Get an access token for the specified scope/resource.

        Args:
            scope: The resource scope for the access token (e.g., "https://analysis.windows.net/powerbi/api")

        Returns:
            str: The access token

        Raises:
            RuntimeError: If the token cannot be retrieved or is empty
        """
        try:
            token = self.run_command(f"account get-access-token --resource {scope} --query accessToken -o tsv", timeout=60)
        except Exception as e:
            error = f"Failed to get access token for scope {scope}. Please ensure you are logged in with 'az login'"
            raise RuntimeError(error) from e

        if not token:
            error = "Access token is empty"
            raise RuntimeError(error)

        return token

    @functools.cache  # noqa: B019
    def get_user_oid(self) -> str:
        """
        Get the user's object ID (oid) from the access token.

        Returns:
            str: The user's object ID

        Raises:
            RuntimeError: If the token cannot be retrieved, decoded, or oid claim is not found
        """
        try:
            token = self.get_access_token("https://analysis.windows.net/powerbi/api")
            parts = token.split(".")
            if len(parts) != 3:  # noqa: PLR2004
                raise RuntimeError("Invalid JWT token format")  # noqa: EM101
            payload = parts[1]
            padding = len(payload) % 4
            if padding:
                payload += "=" * (4 - padding)
            decoded_payload = base64.b64decode(payload)
            claims = json.loads(decoded_payload)
            oid = claims.get("oid")
            if not oid:
                raise RuntimeError("oid claim not found in token")  # noqa: EM101
            return oid

        except Exception as e:
            error = f"Failed to get user object ID: {e}"
            raise RuntimeError(error) from e