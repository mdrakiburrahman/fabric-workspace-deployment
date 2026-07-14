# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import json
import logging

from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest

from fabric_workspace_deployment.logging_config import (
    LoggingParams,
    configure_logging,
    parse_logging_params,
    validate_logging_params,
)


@pytest.fixture(autouse=True)
def reset_root_logging():
    """Save and restore the root logger's handlers/level around each test."""
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    yield
    for handler in root.handlers[:]:
        handler.close()
        root.removeHandler(handler)
    for handler in saved_handlers:
        root.addHandler(handler)
    root.setLevel(saved_level)


def _write_config(path: Path, logging_block: object) -> Path:
    config = {"common": {"local": {"rootFolder": "/src", "logging": logging_block}}}
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------- #
# ----------------------------- validate_logging_params ---------------------- #
# ---------------------------------------------------------------------------- #


def test_validate_accepts_valid_params():
    validate_logging_params(LoggingParams(root_folder="/tmp/logs", max_size_mb=10, backup_count=5, level="INFO"))


def test_validate_accepts_lowercase_level():
    validate_logging_params(LoggingParams(root_folder="/tmp/logs", max_size_mb=1, backup_count=1, level="debug"))


@pytest.mark.parametrize("root_folder", ["", "   ", 123])
def test_validate_rejects_bad_root_folder(root_folder):
    with pytest.raises(ValueError, match="rootFolder"):
        validate_logging_params(LoggingParams(root_folder=root_folder, max_size_mb=1, backup_count=1, level="INFO"))


@pytest.mark.parametrize("max_size_mb", [0, -1, -0.5, True, "10"])
def test_validate_rejects_bad_max_size(max_size_mb):
    with pytest.raises(ValueError, match="maxSizeMB"):
        validate_logging_params(LoggingParams(root_folder="/tmp/logs", max_size_mb=max_size_mb, backup_count=1, level="INFO"))


@pytest.mark.parametrize("backup_count", [0, -1, True, 2.5])
def test_validate_rejects_bad_backup_count(backup_count):
    with pytest.raises(ValueError, match="backupCount"):
        validate_logging_params(LoggingParams(root_folder="/tmp/logs", max_size_mb=1, backup_count=backup_count, level="INFO"))


@pytest.mark.parametrize("level", ["", "TRACE", "verbose", 10])
def test_validate_rejects_bad_level(level):
    with pytest.raises(ValueError, match="level"):
        validate_logging_params(LoggingParams(root_folder="/tmp/logs", max_size_mb=1, backup_count=1, level=level))


# ---------------------------------------------------------------------------- #
# ------------------------------ parse_logging_params ------------------------ #
# ---------------------------------------------------------------------------- #


def test_parse_reads_all_fields(tmp_path):
    cfg = _write_config(tmp_path / "config.json", {"rootFolder": "/var/logs", "maxSizeMB": 25, "backupCount": 7, "level": "WARNING"})
    params = parse_logging_params(str(cfg))
    assert params.root_folder == "/var/logs"
    assert params.max_size_mb == 25
    assert params.backup_count == 7
    assert params.level == "WARNING"


def test_parse_resolves_git_root_placeholder(tmp_path, monkeypatch):
    monkeypatch.setenv("GIT_ROOT", "/repo/root")
    cfg = _write_config(tmp_path / "config.json", {"rootFolder": "{git-root}/logs", "maxSizeMB": 1, "backupCount": 1, "level": "INFO"})
    params = parse_logging_params(str(cfg))
    assert params.root_folder == "/repo/root/logs"


def test_parse_missing_logging_block_raises(tmp_path):
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps({"common": {"local": {"rootFolder": "/src"}}}), encoding="utf-8")
    with pytest.raises(ValueError, match="common.local.logging"):
        parse_logging_params(str(cfg))


def test_parse_missing_key_raises(tmp_path):
    cfg = _write_config(tmp_path / "config.json", {"rootFolder": "/var/logs", "maxSizeMB": 1, "level": "INFO"})
    with pytest.raises(ValueError, match="backupCount"):
        parse_logging_params(str(cfg))


# ---------------------------------------------------------------------------- #
# ------------------------------- configure_logging -------------------------- #
# ---------------------------------------------------------------------------- #


def test_configure_creates_invocation_subfolder(tmp_path):
    params = LoggingParams(root_folder=str(tmp_path / "logs"), max_size_mb=10, backup_count=5, level="INFO")
    log_dir = configure_logging(params, "20260714_135201")

    assert log_dir == tmp_path / "logs" / "app_20260714_135201"
    assert log_dir.is_dir()
    assert (log_dir / "app.log").exists()


def test_configure_installs_rotating_handler(tmp_path):
    params = LoggingParams(root_folder=str(tmp_path / "logs"), max_size_mb=10, backup_count=5, level="INFO")
    log_dir = configure_logging(params, "ts")

    handlers = [h for h in logging.getLogger().handlers if isinstance(h, RotatingFileHandler)]
    assert len(handlers) == 1
    handler = handlers[0]
    assert handler.maxBytes == 10 * 1024 * 1024
    assert handler.backupCount == 5
    assert Path(handler.baseFilename) == log_dir / "app.log"


def test_configure_rolls_over_past_threshold(tmp_path):
    params = LoggingParams(root_folder=str(tmp_path / "logs"), max_size_mb=0.001, backup_count=3, level="INFO")
    log_dir = configure_logging(params, "ts")

    logger = logging.getLogger("rollover_test")
    for i in range(200):
        logger.info("%s %d", "x" * 50, i)

    assert (log_dir / "app.log.1").exists()
