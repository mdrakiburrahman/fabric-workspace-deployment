# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import re
from typing import Any


class StringTransformer:
    """
    String transformation operations.
    """

    @staticmethod
    def camel_to_snake(name: str) -> str:
        """
        Convert camelCase string to snake_case.
        """

        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    @staticmethod
    def convert_keys_to_snake_case(obj: Any) -> Any:
        """
        Recursively convert all dictionary keys from camelCase to snake_case.
        """

        if isinstance(obj, dict):
            return {StringTransformer.camel_to_snake(k): StringTransformer.convert_keys_to_snake_case(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [StringTransformer.convert_keys_to_snake_case(item) for item in obj]
        else:
            return obj