# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import json
import logging
import subprocess
import traceback

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


def test_docker_logs_redact_connection_values_and_inherited_secrets_longest_first(monkeypatch, caplog):
    monkeypatch.setenv("API_PASSWORD", "db-long-secret-token")
    environment = {"FWD_RAYFIN_SQL_DATABASE_NAME": "db", "FWD_RAYFIN_SQL_SERVER": "private.database.fabric.microsoft.com", "FWD_RAYFIN_SQL_ACCESS_TOKEN": "db-long-secret-token"}
    output = "db-long-secret-token private.database.fabric.microsoft.com db"
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=1, stdout=output, stderr=output))
    caplog.set_level(logging.DEBUG)

    with pytest.raises(DockerCliError) as failure:
        DockerCli().run(["compose", "run", "rayfin", "sh", "-c", output], env=environment)

    diagnostic = caplog.text + str(failure.value) + failure.value.stdout + failure.value.stderr
    assert "db-long-secret-token" not in diagnostic
    assert "long-secret-token" not in diagnostic
    assert "private.database.fabric.microsoft.com" not in diagnostic


def test_docker_timeout_sanitizes_streams_and_suppresses_raw_exception_chain(monkeypatch, caplog):
    def run(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], 30, output=b"private-token", stderr=b"private-token")

    monkeypatch.setattr(subprocess, "run", run)
    caplog.set_level(logging.DEBUG)
    environment = {"FWD_RAYFIN_SQL_ACCESS_TOKEN": "private-token"}

    with pytest.raises(DockerCliError) as failure:
        DockerCli().run(["compose", "run", "rayfin"], timeout=30, env=environment)

    diagnostic = caplog.text + "".join(traceback.format_exception(failure.type, failure.value, failure.tb)) + failure.value.stdout + failure.value.stderr
    assert "private-token" not in diagnostic
    assert failure.value.__suppress_context__ is True


@pytest.mark.parametrize("timed_out", [False, True])
def test_inherited_sql_environment_token_is_redacted_from_docker_failures(monkeypatch, caplog, timed_out):
    token = "inherited-sql-env-token"
    monkeypatch.setenv("FAB_TOKEN_SQL", token)
    process_environments = []

    def run(command, **kwargs):
        process_environments.append(kwargs["env"])
        if timed_out:
            raise subprocess.TimeoutExpired(command, 30, output=token.encode(), stderr=token.encode())
        return SimpleNamespace(returncode=1, stdout=token, stderr=token)

    monkeypatch.setattr(subprocess, "run", run)
    caplog.set_level(logging.DEBUG)

    with pytest.raises(DockerCliError) as failure:
        DockerCli().run(["compose", "run", "rayfin"], timeout=30)

    assert process_environments[0]["FAB_TOKEN_SQL"] == token
    diagnostics = caplog.text + str(failure.value) + failure.value.stdout + failure.value.stderr + "".join(traceback.format_exception(failure.type, failure.value, failure.tb))
    assert token not in diagnostics
    assert failure.value.timed_out is timed_out


def test_named_migration_container_is_stopped_after_cli_timeout(monkeypatch, tmp_path):
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        if command[1] == "compose":
            raise subprocess.TimeoutExpired(command, 900)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", run)

    with pytest.raises(DockerCliError, match="timed out"):
        DockerCli().compose_run(tmp_path / "Compose.yaml", "project", "rayfin", ["sh", "-c", "npm run data:migrate"], timeout=900, container_name="project-data-migrate")

    assert "--name" in calls[0]
    assert calls[-1] == ["docker", "stop", "-t", "10", "project-data-migrate"]


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
