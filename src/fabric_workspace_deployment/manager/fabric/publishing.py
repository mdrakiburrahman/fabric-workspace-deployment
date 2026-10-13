# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import logging


def configure_publish_features(feature_flags: list[str], *additional_feature_flags: str) -> None:
    import fabric_cicd  # type: ignore[import-untyped]

    flags = list(dict.fromkeys([*feature_flags, "enable_experimental_features", "enable_bulk_publish", *additional_feature_flags]))
    logging.getLogger(__name__).info(f"Enabling Fabric publish feature flags: {flags}")
    for feature_flag in flags:
        fabric_cicd.append_feature_flag(feature_flag)
