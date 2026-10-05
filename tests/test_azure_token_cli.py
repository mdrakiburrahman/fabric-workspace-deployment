# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging
import traceback
from subprocess import TimeoutExpired
from types import SimpleNamespace

import pytest

from fabric_workspace_deployment.manager.azure import cli


def test_sql_token_is_acquired_uncached_without_logging_secret_stdout(monkeypatch, caplog):
    calls = []

    def popen(command, **kwargs):
        calls.append(list(command))
        token = f"sql-token-{len(calls)}"
        return SimpleNamespace(returncode=0, communicate=lambda **kwargs: (token.encode(), b""))

    monkeypatch.setattr(cli, "Popen", popen)
    caplog.set_level(logging.DEBUG)
    azure = cli.AzCli()

    assert azure.get_sql_access_token() == "sql-token-1"
    assert azure.get_sql_access_token() == "sql-token-2"
    assert len(calls) == 2
    assert all(command == ["az", "account", "get-access-token", "--resource", "https://database.windows.net", "--query", "accessToken", "--output", "tsv"] for command in calls)
    assert "sql-token-1" not in caplog.text
    assert "sql-token-2" not in caplog.text


def test_token_failure_masks_both_streams_even_when_general_cli_errors_disabled(monkeypatch, caplog):
    monkeypatch.setattr(cli, "Popen", lambda *args, **kwargs: SimpleNamespace(returncode=1, communicate=lambda **kwargs: (b"private-token", b"private-token")))
    caplog.set_level(logging.DEBUG)

    with pytest.raises(RuntimeError, match="access-token acquisition failed") as failure:
        cli.AzCli(exit_on_error=False).get_sql_access_token()

    assert "private-token" not in caplog.text
    assert "private-token" not in "".join(traceback.format_exception(failure.type, failure.value, failure.tb))


def test_empty_sql_token_fails_explicitly(monkeypatch):
    monkeypatch.setattr(cli, "Popen", lambda *args, **kwargs: SimpleNamespace(returncode=0, communicate=lambda **kwargs: (b" \n", b"")))

    with pytest.raises(RuntimeError, match="empty SQL access token"):
        cli.AzCli().get_sql_access_token()


def test_sql_token_uses_the_guarded_configured_tenant(monkeypatch):
    commands = []

    def popen(command, **kwargs):
        commands.append(command)
        return SimpleNamespace(returncode=0, communicate=lambda **kwargs: (b"sql-token", b""))

    monkeypatch.setattr(cli, "Popen", popen)

    assert cli.AzCli().get_sql_access_token("11111111-1111-1111-1111-111111111111") == "sql-token"
    assert commands[0][-2:] == ["--tenant", "11111111-1111-1111-1111-111111111111"]


def test_token_timeout_does_not_write_captured_secret_streams(monkeypatch, capsys):
    calls = []

    def communicate(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise TimeoutExpired("az", 60, output=b"private-token", stderr=b"private-token")
        return b"private-token", b"private-token"

    monkeypatch.setattr(cli, "Popen", lambda *args, **kwargs: SimpleNamespace(communicate=communicate, kill=lambda: None))

    with pytest.raises(RuntimeError, match="acquisition timed out") as failure:
        cli.AzCli().get_sql_access_token()

    captured = capsys.readouterr()
    assert "private-token" not in captured.out + captured.err
    assert "private-token" not in "".join(traceback.format_exception(failure.type, failure.value, failure.tb))
