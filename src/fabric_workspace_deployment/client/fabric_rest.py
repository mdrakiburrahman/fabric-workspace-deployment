# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import requests

from fabric_workspace_deployment.manager.azure.cli import AzCli
from fabric_workspace_deployment.operations.operation_interfaces import CommonParams, GUID_PATTERN, HttpRetryHandler

State = TypeVar("State")
VERIFY_MAX_ATTEMPTS = 5
VERIFY_DELAY_SECONDS = 1


def response_object(value: object, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f"{context} must be an object")
    return value


def response_array(value: object, context: str) -> list[Any]:
    if not isinstance(value, list):
        raise RuntimeError(f"{context} must be an array")
    return value


def response_string(value: object, context: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{context} must be a non-empty string")
    return value


def response_guid(value: object, context: str) -> str:
    result = response_string(value, context)
    if not GUID_PATTERN.fullmatch(result):
        raise RuntimeError(f"{context} must be a GUID")
    return result


def response_integer(value: object, context: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise RuntimeError(f"{context} must be a positive integer")
    return value


async def verify_reconciliation(read: Callable[[], Awaitable[State]], matches: Callable[[State], bool], context: str) -> None:
    for attempt in range(VERIFY_MAX_ATTEMPTS):
        if matches(await read()):
            return
        if attempt + 1 < VERIFY_MAX_ATTEMPTS:
            await asyncio.sleep(VERIFY_DELAY_SECONDS)
    raise RuntimeError(f"{context}: remote state did not converge to the configured state")


class FabricRestClient:
    """Shared transport with opt-in privacy-safe retry and response validation."""

    def __init__(self, common_params: CommonParams, az_cli: AzCli, http_retry_handler: HttpRetryHandler, base_url: str):
        self.common_params = common_params
        self.az_cli = az_cli
        self.http_retry = http_retry_handler
        self.base_url = base_url.rstrip("/")

    def _request(self, method: Callable[..., requests.Response], path: str, context: str, **kwargs: Any) -> requests.Response:
        return self.http_retry.execute(method, f"{self.base_url}{path}", safe_log_context=context, headers={"Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}", "Content-Type": "application/json"}, timeout=60, **kwargs)

    def _get_json(self, path: str, context: str, api_root: str | None = None, **kwargs: Any) -> object:
        if api_root is None:
            response = self._request(requests.get, path, context, **kwargs)
        else:
            response = self.http_retry.execute(requests.get, f"{api_root.rstrip('/')}{path}", safe_log_context=context, headers={"Authorization": f"Bearer {self.az_cli.get_access_token(self.common_params.scope.analysis_service)}", "Content-Type": "application/json"}, timeout=60, **kwargs)
        try:
            return response.json()
        except ValueError:
            raise RuntimeError(f"{context}: response is not valid JSON") from None
