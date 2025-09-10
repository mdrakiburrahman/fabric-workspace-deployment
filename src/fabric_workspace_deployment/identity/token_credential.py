# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

from typing import Any
import logging
from azure.core.credentials import AccessToken, TokenCredential


class StaticTokenCredential(TokenCredential):
    """A static token credential."""

    def __init__(self, token: str, expiry: int):
        if not token:
            raise ValueError("Token cannot be None or empty")

        self._token = token
        self.expiry = expiry
        self.logger = logging.getLogger(__name__)

    def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken:
        self.logger.debug(f"Static token credential - getting token for scopes: {scopes}")
        return AccessToken(self._token, self.expiry)

    def get_expire(self) -> int:
        return self.expiry
