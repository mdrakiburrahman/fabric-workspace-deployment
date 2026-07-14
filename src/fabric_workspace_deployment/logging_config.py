# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

"""
Logging configuration driven by the mandatory ``common.local.logging`` config block.

Provides fail-fast parsing/validation of the logging config and a native
``RotatingFileHandler`` based setup that writes each invocation's logs into its own
timestamped subfolder under the configured log root folder.
"""

import json
import logging
import os
import subprocess

from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"

_VALID_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})

_GIT_ROOT_PLACEHOLDER = "{git-root}"


class _ExcludeByNamePrefix(logging.Filter):
    """Reject log records whose logger name starts with a given prefix."""

    def __init__(self, prefix: str) -> None:
        super().__init__()
        self._prefix = prefix

    def filter(self, record: logging.LogRecord) -> bool:
        return not record.name.startswith(self._prefix)


@dataclass
class LoggingParams:
    """Mandatory logging configuration parameters (``common.local.logging``)."""

    root_folder: str
    max_size_mb: float
    backup_count: int
    level: str


def _resolve_git_root(logger: logging.Logger) -> str:
    """
    Resolve the git root directory for the ``{git-root}`` placeholder.

    Returns:
        str: The git root, from the GIT_ROOT env var or ``git rev-parse --show-toplevel``.

    Raises:
        RuntimeError: If no git root can be resolved.
    """
    git_root = os.getenv("GIT_ROOT")
    if git_root:
        return git_root

    err_str = "No git root found. Please ensure you are in a git repository or set GIT_ROOT."
    try:
        result = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True, timeout=10)  # fmt: skip # noqa: E501, S603, S607
        return result.stdout.strip()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError) as e:
        logger.error(f"Failed to get git root: {e}")
        raise RuntimeError(err_str) from e


def _resolve_git_root_placeholder(value: Any, logger: logging.Logger) -> Any:
    """Replace the ``{git-root}`` placeholder in a string value, if present."""
    if isinstance(value, str) and _GIT_ROOT_PLACEHOLDER in value:
        return value.replace(_GIT_ROOT_PLACEHOLDER, _resolve_git_root(logger))
    return value


def parse_logging_params(config_file_absolute_path: str, logger: logging.Logger | None = None) -> LoggingParams:
    """
    Load and parse the mandatory ``common.local.logging`` block from the config file.

    The ``rootFolder`` value supports the ``{git-root}`` placeholder.

    Args:
        config_file_absolute_path: Absolute path to the configuration file.
        logger: Optional logger instance.

    Returns:
        LoggingParams: The parsed logging parameters.

    Raises:
        FileNotFoundError: If the config file doesn't exist.
        json.JSONDecodeError: If the JSON is malformed.
        ValueError: If the logging block or any mandatory key is missing.
    """
    logger = logger or logging.getLogger(__name__)

    with open(config_file_absolute_path, encoding="utf-8") as file:
        config_data = json.load(file)

    try:
        logging_data = config_data["common"]["local"]["logging"]
    except (KeyError, TypeError) as e:
        msg = f"Missing mandatory 'common.local.logging' config block: {e}"
        raise ValueError(msg) from e

    try:
        root_folder = logging_data["rootFolder"]
        max_size_mb = logging_data["maxSizeMB"]
        backup_count = logging_data["backupCount"]
        level = logging_data["level"]
    except (KeyError, TypeError) as e:
        msg = f"Missing mandatory key in 'common.local.logging': {e}"
        raise ValueError(msg) from e

    root_folder = _resolve_git_root_placeholder(root_folder, logger)
    return LoggingParams(root_folder=root_folder, max_size_mb=max_size_mb, backup_count=backup_count, level=level)


def validate_logging_params(params: LoggingParams) -> None:
    """
    Statically validate logging parameters and fail fast on any invalid value.

    Args:
        params: The logging parameters to validate.

    Raises:
        ValueError: If any parameter is missing or out of range.
    """
    if not isinstance(params.root_folder, str) or not params.root_folder.strip():
        msg = f"logging.rootFolder must be a non-empty string, got: {params.root_folder!r}"
        raise ValueError(msg)

    if isinstance(params.max_size_mb, bool) or not isinstance(params.max_size_mb, (int, float)) or params.max_size_mb <= 0:
        msg = f"logging.maxSizeMB must be a number greater than 0, got: {params.max_size_mb!r}"
        raise ValueError(msg)

    if isinstance(params.backup_count, bool) or not isinstance(params.backup_count, int) or params.backup_count < 1:
        msg = f"logging.backupCount must be an integer greater than or equal to 1, got: {params.backup_count!r}"
        raise ValueError(msg)

    if not isinstance(params.level, str) or params.level.upper() not in _VALID_LEVELS:
        msg = f"logging.level must be one of {sorted(_VALID_LEVELS)}, got: {params.level!r}"
        raise ValueError(msg)


def configure_logging(params: LoggingParams, timestamp: str) -> Path:
    """
    Configure logging with a per-invocation subfolder and native size-based rotation.

    Creates ``<root_folder>/app_<timestamp>/`` and installs a ``RotatingFileHandler``
    (writing ``app.log`` and rolling over to ``app.log.1``, ``app.log.2``, ... up to
    ``backup_count`` files) plus a console stream handler at the configured level.

    Args:
        params: The validated logging parameters.
        timestamp: The invocation timestamp used to name the subfolder.

    Returns:
        Path: The per-invocation log subfolder that was created.
    """
    invocation_dir = Path(params.root_folder) / f"app_{timestamp}"
    invocation_dir.mkdir(parents=True, exist_ok=True)
    log_file = invocation_dir / "app.log"

    formatter = logging.Formatter(_LOG_FORMAT)

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=int(params.max_size_mb * 1024 * 1024),
        backupCount=params.backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(getattr(logging, params.level.upper()))
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(_ExcludeByNamePrefix("fabric_cicd"))

    logging.basicConfig(level=logging.DEBUG, handlers=[file_handler, stream_handler], force=True)
    return invocation_dir


def setup_logging_from_config(config_file_absolute_path: str, timestamp: str, logger: logging.Logger | None = None) -> Path:
    """
    Parse, validate, and apply the logging configuration from the config file.

    Args:
        config_file_absolute_path: Absolute path to the configuration file.
        timestamp: The invocation timestamp used to name the subfolder.
        logger: Optional logger instance used during parsing.

    Returns:
        Path: The per-invocation log subfolder that was created.

    Raises:
        ValueError: If the logging configuration is missing or invalid.
    """
    params = parse_logging_params(config_file_absolute_path, logger=logger)
    validate_logging_params(params)
    return configure_logging(params, timestamp)
