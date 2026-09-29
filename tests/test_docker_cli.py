# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging
import subprocess

from pathlib import Path
from types import SimpleNamespace

import pytest

from fabric_workspace_deployment.manager.docker.cli import DockerCli, DockerCliError


def test_compose_run_builds_argv_and_injects_environment(monkeypatch, tmp_path):
    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    compose_file = tmp_path / "Compose.rayfin.yaml"
    compose_file.write_text("services: {}", encoding="utf-8")

    stdout, stderr = DockerCli().compose_run(
        compose_file,
        "project",
        "rayfin",
        ["npm", "ci"],
        timeout=123,
        env={"RAYFIN_TOKEN": "secret-token", "RAYFIN_WORKSPACE_ID": "workspace-id"},
    )

    assert stdout == "ok"
    assert stderr == ""
    assert captured["command"] == [
        "docker",
        "compose",
        "--file",
        str(compose_file),
        "--project-name",
        "project",
        "run",
        "--rm",
        "--no-deps",
        "rayfin",
        "npm",
        "ci",
    ]
    assert captured["kwargs"]["timeout"] == 123
    assert captured["kwargs"]["env"]["RAYFIN_TOKEN"] == "secret-token"


def test_failure_redacts_token_from_logs_and_exception(monkeypatch, caplog):
    def fake_run(command, **kwargs):
        return SimpleNamespace(returncode=1, stdout="token=secret-token", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    caplog.set_level(logging.DEBUG)

    with pytest.raises(DockerCliError) as exc_info:
        DockerCli().run(["version"], env={"RAYFIN_TOKEN": "secret-token"})

    assert "secret-token" not in str(exc_info.value)
    assert "secret-token" not in caplog.text
    assert "******" in caplog.text
