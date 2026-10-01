# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import json
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


def test_resolve_daemon_path_returns_resolved_path_outside_container(monkeypatch, tmp_path):
    docker_cli = DockerCli()
    monkeypatch.setattr(docker_cli, "_is_containerized", lambda: False)

    assert docker_cli.resolve_daemon_path(tmp_path / ".." / tmp_path.name) == tmp_path.resolve()


def test_resolve_daemon_path_uses_longest_matching_mount(monkeypatch):
    docker_cli = DockerCli()
    monkeypatch.setattr(docker_cli, "_is_containerized", lambda: True)
    monkeypatch.setattr(
        docker_cli,
        "_current_container_mounts",
        lambda: [
            {"Type": "bind", "Source": "/host/work", "Destination": "/work"},
            {"Type": "volume", "Source": "/var/lib/docker/volumes/repo/_data", "Destination": "/work/repo"},
        ],
    )

    assert docker_cli.resolve_daemon_path(Path("/work/repo/staging/app")) == Path("/var/lib/docker/volumes/repo/_data/staging/app")


def test_resolve_daemon_path_keeps_path_when_current_container_is_not_visible(monkeypatch, tmp_path):
    docker_cli = DockerCli()
    monkeypatch.setattr(docker_cli, "_is_containerized", lambda: True)
    monkeypatch.setattr(docker_cli, "_current_container_mounts", lambda: None)

    assert docker_cli.resolve_daemon_path(tmp_path) == tmp_path.resolve()


def test_resolve_daemon_path_rejects_path_outside_exported_mounts(monkeypatch):
    docker_cli = DockerCli()
    monkeypatch.setattr(docker_cli, "_is_containerized", lambda: True)
    monkeypatch.setattr(docker_cli, "_current_container_mounts", lambda: [{"Type": "bind", "Source": "/host/work", "Destination": "/work"}])

    with pytest.raises(DockerCliError, match="not under a mount exposed"):
        docker_cli.resolve_daemon_path(Path("/tmp/staging/app"))


def test_current_container_mounts_uses_first_visible_candidate(monkeypatch):
    docker_cli = DockerCli()
    calls = []
    expected_mounts = [{"Type": "bind", "Source": "/host/work", "Destination": "/work"}]

    monkeypatch.setattr(docker_cli, "_current_container_candidates", lambda: ["missing", "current"])

    def fake_run(command, **kwargs):
        calls.append(command)
        if command[1] == "missing":
            raise DockerCliError("not found")
        return json.dumps(expected_mounts), ""

    monkeypatch.setattr(docker_cli, "run", fake_run)

    assert docker_cli._current_container_mounts() == expected_mounts
    assert calls == [
        ["inspect", "missing", "--format", "{{json .Mounts}}"],
        ["inspect", "current", "--format", "{{json .Mounts}}"],
    ]
