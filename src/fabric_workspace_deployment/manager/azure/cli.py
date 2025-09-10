# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import base64
import functools
import json
import logging
import sys
from subprocess import PIPE, Popen, TimeoutExpired
import os


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

        stdout_str = stdout.decode(sys.stdout.encoding)
        stderr_str = stderr.decode(sys.stderr.encoding)

        self.logger.debug(f"Stdout: {stdout_str}")
        self.logger.debug(f"Stderr: {stderr_str}")

        if proc.returncode != 0 and self.exit_on_error:
            raise Exception(stderr_str)

        return (stdout_str, stderr_str)

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
        if scope == "https://analysis.windows.net/powerbi/api" and os.getenv("FAB_TOKEN"):
            return os.getenv("FAB_TOKEN")
        elif scope == "https://management.azure.com" and os.getenv("FAB_TOKEN_AZURE"):
            return os.getenv("FAB_TOKEN_AZURE")

        try:
            token = self.run_command(
                f"account get-access-token --resource {scope} --query accessToken -o tsv", timeout=60)
        except Exception as e:
            error = f"Failed to get access token for scope {scope}. Please ensure you are logged in with 'az login'"
            raise RuntimeError(error) from e

        if not token:
            error = "Access token is empty"
            raise RuntimeError(error)

        return token

    @functools.cache  # noqa: B019
    def get_claim(self, claim_name: str) -> str:
        """
        Get a specific claim from the access token.

        Args:
            claim_name: The name of the claim to extract from the JWT token

        Returns:
            str: The claim value

        Raises:
            RuntimeError: If the token cannot be retrieved, decoded, or claim is not found

        Here is a standard set of claims:

        >>> az account get-access-token --resource 'https://analysis.windows.net/powerbi/api' --query accessToken --output tsv

        ```
        {
          "typ": "JWT",
          "alg": "RS256",
          "x5t": "JYhAcTPMZ_LX6DBlOWQ7Hn0NeXE",
          "kid": "JYhAcTPMZ_LX6DBlOWQ7Hn0NeXE"
        }.{
          "aud": "https://analysis.windows.net/powerbi/api",
          "iss": "https://sts.windows.net/72f988bf-86f1-41af-91ab-2d7cd011db47/",
          "iat": 1757451397,
          "nbf": 1757451397,
          "exp": 1757456730,
          "acct": 0,
          "acr": "1",
          "aio": "...",
          "amr": [
            "fido",
            "rsa",
            "mfa"
          ],
          "appid": "04b0...7b46",
          "appidacr": "0",
          "controls": [
            "app_res"
          ],
          "controls_auds": [
            "00000...0000",
            "00000...00"
          ],
          "deviceid": "64f9c...fa0e3",
          "family_name": "Rahman",
          "given_name": "Raki",
          "idtyp": "user",
          "ipaddr": "1...2",
          "name": "Raki Rahman",
          "oid": "b99...25",
          "onprem_sid": "S-1...96",
          "puid": "10...DD6",
          "rh": "1.ARo...AA.",
          "scp": "user_impersonation",
          "sid": "00..c",
          "signin_state": [
            "dvc_mngd",
            "dvc_cmp"
          ],
          "sub": "2m...A",
          "tid": "72...47",
          "unique_name": "mdrrahman@microsoft.com",
          "upn": "mdrrahman@microsoft.com",
          "uti": "O...A",
          "ver": "1.0",
          "wids": [
            "b7...09"
          ],
          "xms_cc": [
            "CP1"
          ],
          "xms_ftd": "Z_...bXM",
          "xms_idrel": "1 30"
        }.[Signature]
        ```

        """
        try:
            token = self.get_access_token(
                "https://analysis.windows.net/powerbi/api")
            parts = token.split(".")
            if len(parts) != 3:  # noqa: PLR2004
                raise RuntimeError("Invalid JWT token format")  # noqa: EM101
            payload = parts[1]
            padding = len(payload) % 4
            if padding:
                payload += "=" * (4 - padding)
            decoded_payload = base64.b64decode(payload)
            claims = json.loads(decoded_payload)
            claim_value = claims.get(claim_name)
            if not claim_value:
                raise RuntimeError(f"{claim_name} claim not found in token")  # noqa: EM101
            return claim_value

        except Exception as e:
            error = f"Failed to get {claim_name} claim: {e}"
            raise RuntimeError(error) from e

    @functools.cache  # noqa: B019
    def get_user_oid(self) -> str:
        """
        Get the user's object ID (oid) from the access token.

        Returns:
            str: The user's object ID

        Raises:
            RuntimeError: If the token cannot be retrieved, decoded, or oid claim is not found
        """
        return self.get_claim("oid")

    @functools.cache  # noqa: B019
    def get_user_appid(self) -> str:
        """
        Get the user's application ID (appid) from the access token.

        Returns:
            str: The user's application ID

        Raises:
            RuntimeError: If the token cannot be retrieved, decoded, or appid claim is not found
        """
        return self.get_claim("appid")
