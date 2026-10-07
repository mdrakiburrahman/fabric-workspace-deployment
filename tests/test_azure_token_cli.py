# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging
import traceback
from subprocess import TimeoutExpired
from types import SimpleNamespace

import pytest

from fabric_workspace_deployment.environment_variables import FAB_TOKEN_SQL_ENV_VAR, SCOPE_TOKEN_ENV_VARS
from fabric_workspace_deployment.manager.azure import cli


@pytest.fixture(autouse=True)
def clear_ambient_sql_token(monkeypatch):
    monkeypatch.delenv(FAB_TOKEN_SQL_ENV_VAR, raising=False)


@pytest.mark.parametrize("token", ["sql-env-token", " \t sql-env-token \n"])
@pytest.mark.parametrize("tenant_id", [None, "11111111-1111-1111-1111-111111111111"])
def test_sql_environment_token_precedes_azure_cli(monkeypatch, caplog, token, tenant_id):
    monkeypatch.setenv(FAB_TOKEN_SQL_ENV_VAR, token)
    monkeypatch.setattr(cli, "Popen", lambda *args, **kwargs: pytest.fail("Azure CLI must not acquire a supplied SQL token"))
    caplog.set_level(logging.DEBUG)

    assert cli.AzCli().get_sql_access_token(tenant_id) == "sql-env-token"
    assert "sql-env-token" not in caplog.text


@pytest.mark.parametrize("scope", ["https://database.windows.net", "https://database.windows.net/"])
def test_sql_scope_maps_to_the_public_environment_token(monkeypatch, scope):
    assert FAB_TOKEN_SQL_ENV_VAR == "FAB_TOKEN_SQL"
    assert SCOPE_TOKEN_ENV_VARS["https://database.windows.net"] == FAB_TOKEN_SQL_ENV_VAR
    monkeypatch.setenv(FAB_TOKEN_SQL_ENV_VAR, " sql-env-token ")
    monkeypatch.setattr(cli, "Popen", lambda *args, **kwargs: pytest.fail("The SQL scope must use its environment token"))

    assert cli.AzCli().get_access_token(scope) == "sql-env-token"


@pytest.mark.parametrize("token", [None, "", " \t\r\n"])
@pytest.mark.parametrize("tenant_id", [None, "11111111-1111-1111-1111-111111111111"])
def test_blank_sql_environment_token_preserves_exact_azure_cli_fallback(monkeypatch, token, tenant_id):
    if token is not None:
        monkeypatch.setenv(FAB_TOKEN_SQL_ENV_VAR, token)
    commands = []
    timeouts = []

    def popen(command, **kwargs):
        commands.append(list(command))

        def communicate(**kwargs):
            timeouts.append(kwargs["timeout"])
            return b" sql-cli-token \n", b""

        return SimpleNamespace(returncode=0, communicate=communicate)

    monkeypatch.setattr(cli, "Popen", popen)

    assert cli.AzCli().get_sql_access_token(tenant_id) == "sql-cli-token"
    expected = ["az", "account", "get-access-token", "--resource", "https://database.windows.net", "--query", "accessToken", "--output", "tsv"]
    if tenant_id is not None:
        expected.extend(["--tenant", tenant_id])
    assert commands == [expected]
    assert timeouts == [60]


def test_sql_environment_token_is_re_read_and_removal_enables_fallback(monkeypatch):
    commands = []

    def popen(command, **kwargs):
        commands.append(list(command))
        return SimpleNamespace(returncode=0, communicate=lambda **kwargs: (b"sql-cli-token", b""))

    monkeypatch.setattr(cli, "Popen", popen)
    azure = cli.AzCli()
    monkeypatch.setenv(FAB_TOKEN_SQL_ENV_VAR, " sql-env-first ")
    assert azure.get_sql_access_token() == "sql-env-first"
    monkeypatch.setenv(FAB_TOKEN_SQL_ENV_VAR, "\n sql-env-second \t")
    assert azure.get_sql_access_token() == "sql-env-second"
    assert commands == []

    monkeypatch.delenv(FAB_TOKEN_SQL_ENV_VAR)
    assert azure.get_sql_access_token() == "sql-cli-token"
    assert len(commands) == 1


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
