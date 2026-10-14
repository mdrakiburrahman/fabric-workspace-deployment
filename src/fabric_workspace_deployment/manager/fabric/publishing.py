# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import base64
import json
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

from azure.core.credentials import TokenCredential

if TYPE_CHECKING:
    from fabric_cicd import FabricWorkspace  # type: ignore[import-untyped]
    from fabric_cicd._common._item import Item  # type: ignore[import-untyped]
    from fabric_cicd._items._base_publisher import ItemPublisher  # type: ignore[import-untyped]

    from fabric_workspace_deployment.operations.operation_interfaces import ExternallyManagedShortcuts


def create_publish_workspace(*, workspace_id: str, environment: str, repository_directory: str, item_type_in_scope: list[str], token_credential: TokenCredential, bulk_publish: bool = False, externally_managed_shortcuts: Sequence["ExternallyManagedShortcuts"] = ()) -> "FabricWorkspace":
    import fabric_cicd  # type: ignore[import-untyped]

    class PublishWorkspace(fabric_cicd.FabricWorkspace):
        _existing_lakehouse_schemas: dict[str, str | None] | None = None
        _bulk_publish_enabled: bool = False
        externally_managed_shortcuts: dict[str, "ExternallyManagedShortcuts"] | None = None

        @property
        def bulk_publish_enabled(self) -> bool:
            return self._bulk_publish_enabled

        @bulk_publish_enabled.setter
        def bulk_publish_enabled(self, enabled: bool) -> None:
            # Upstream flags are process-global; the workspace opt-in must remain authoritative.
            self._bulk_publish_enabled = enabled and bulk_publish

        def _get_existing_lakehouse_schemas(self) -> dict[str, str | None]:
            if self._existing_lakehouse_schemas is None:
                schemas: dict[str, str | None] = {}
                request_url = f"{self.base_api_url}/lakehouses"
                while request_url:
                    response = self.endpoint.invoke(method="GET", url=request_url)
                    for lakehouse in response["body"]["value"]:
                        properties = lakehouse.get("properties")
                        if not isinstance(properties, dict):
                            raise ValueError(f"Lakehouse '{lakehouse['id']}' response is missing properties; cannot preserve its schema for bulk publishing")
                        schema = properties.get("defaultSchema")
                        if schema is not None and not isinstance(schema, str):
                            raise ValueError(f"Lakehouse '{lakehouse['id']}' response has an invalid defaultSchema: {schema!r}")
                        schemas[lakehouse["id"]] = schema
                    request_url = response["body"].get("continuationUri") or response["header"].get("continuationUri")
                self._existing_lakehouse_schemas = schemas
            return self._existing_lakehouse_schemas

        def _prepare_bulk_item_parts(self, item: "Item", publisher: "ItemPublisher") -> list[dict[str, str]]:
            parts = super()._prepare_bulk_item_parts(item, publisher)
            if item.type == "SparkJobDefinition":
                for part in parts:
                    if part["path"].endswith("/SparkJobDefinitionV1.json"):
                        content = base64.b64decode(part["payload"]).decode("utf-8")
                        resolved_content = self._replace_logical_ids(content)
                        part["payload"] = base64.b64encode(resolved_content.encode("utf-8")).decode("utf-8")
                        if resolved_content != content:
                            logging.getLogger(__name__).info(f"Resolved Spark Job Definition '{item.name}' logical attachment IDs to deployed item IDs")
            if item.type == "Lakehouse" and item.guid:
                schemas = self._get_existing_lakehouse_schemas()
                if item.guid not in schemas:
                    raise ValueError(f"Existing Lakehouse '{item.name}' ({item.guid}) was not returned by the Lakehouse API; cannot preserve its schema for bulk publishing")
                schema = schemas[item.guid]
                for part in parts:
                    if part["path"].endswith("/lakehouse.metadata.json"):
                        metadata = json.loads(base64.b64decode(part["payload"]))
                        if not isinstance(metadata, dict):
                            raise ValueError(f"Lakehouse metadata '{part['path']}' must be a JSON object")
                        if schema:
                            metadata["defaultSchema"] = schema
                        else:
                            metadata.pop("defaultSchema", None)
                        part["payload"] = base64.b64encode(json.dumps(metadata).encode("utf-8")).decode("utf-8")
                logging.getLogger(__name__).info(f"Preserving existing Lakehouse '{item.name}' default schema: {schema!r}")
            return parts

    workspace = PublishWorkspace(workspace_id=workspace_id, environment=environment, repository_directory=repository_directory, item_type_in_scope=item_type_in_scope, token_credential=token_credential)
    if externally_managed_shortcuts:
        workspace.externally_managed_shortcuts = {rule.lakehouse: rule for rule in externally_managed_shortcuts}
        install_shortcut_preservation()
    return workspace


def install_shortcut_preservation() -> None:
    """Hide externally managed shortcuts from upstream's orphan shortcut cleanup.

    Upstream deletes every deployed Lakehouse shortcut that is absent from the repository's shortcuts.metadata.json. The patch is process-global but only acts on workspaces that carry rules, so every other workspace keeps upstream behavior. Idempotent.
    """
    from fabric_cicd._items import _lakehouse  # type: ignore[import-untyped]

    upstream = _lakehouse.list_deployed_shortcuts
    if getattr(upstream, "_preserves_externally_managed_shortcuts", False):
        return

    def list_deployed_shortcuts(fabric_workspace_obj: "FabricWorkspace", item_obj: "Item") -> list:
        deployed = upstream(fabric_workspace_obj, item_obj)
        rule = (getattr(fabric_workspace_obj, "externally_managed_shortcuts", None) or {}).get(item_obj.name)
        if rule is None:
            return deployed
        cleanup_candidates = [path for path in deployed if not path.startswith(rule.path_prefixes)]
        preserved = len(deployed) - len(cleanup_candidates)
        if preserved:
            logging.getLogger(__name__).info(f"Preserving {preserved} externally managed shortcut(s) on Lakehouse '{item_obj.name}' under {list(rule.path_prefixes)}" + (f": {rule.reason}" if rule.reason else ""))
        return cleanup_candidates

    list_deployed_shortcuts._preserves_externally_managed_shortcuts = True  # type: ignore[attr-defined]
    _lakehouse.list_deployed_shortcuts = list_deployed_shortcuts


def validate_externally_managed_shortcuts(artifacts_root: Path, item_types_in_scope: list[str], feature_flags: list[str], rules: Sequence["ExternallyManagedShortcuts"]) -> None:
    """Fail on unknown Lakehouses so a typo cannot silently re-enable shortcut deletion."""
    if not rules:
        return
    logger = logging.getLogger(__name__)
    lakehouses: set[str] = set()
    for platform_path in sorted(artifacts_root.rglob(".platform")):
        try:
            platform = json.loads(platform_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise ValueError(f"Invalid JSON in Fabric system file '{platform_path}': {error}") from error
        metadata = platform.get("metadata") if isinstance(platform, dict) else None
        if isinstance(metadata, dict) and metadata.get("type") == "Lakehouse" and isinstance(metadata.get("displayName"), str):
            lakehouses.add(metadata["displayName"])
    for rule in rules:
        if rule.lakehouse not in lakehouses:
            raise ValueError(f"template.externallyManagedShortcuts references Lakehouse '{rule.lakehouse}' which is not in '{artifacts_root}'; available Lakehouses: {sorted(lakehouses)}")
    if "Lakehouse" not in item_types_in_scope:
        logger.warning("template.externallyManagedShortcuts has no effect: Lakehouse is not in template.itemTypesInScope")
    if "enable_shortcut_publish" not in feature_flags:
        logger.warning("template.externallyManagedShortcuts has no effect: fabric-cicd only manages shortcuts when template.featureFlags includes enable_shortcut_publish")
    logger.info("Externally managed shortcuts exempt from orphan cleanup: " + ", ".join(f"{rule.lakehouse}{list(rule.path_prefixes)}" for rule in rules))


def configure_publish_features(feature_flags: list[str], *additional_feature_flags: str, bulk_publish: bool = False) -> None:
    import fabric_cicd  # type: ignore[import-untyped]

    if "enable_bulk_publish" in feature_flags and not bulk_publish:
        logging.getLogger(__name__).warning("Ignoring template.featureFlags enable_bulk_publish: opt in with template.bulkPublish=true")
    flags = list(dict.fromkeys([*(flag for flag in feature_flags if flag != "enable_bulk_publish"), "enable_experimental_features", *(["enable_bulk_publish"] if bulk_publish else []), *additional_feature_flags]))
    logging.getLogger(__name__).info(f"Enabling Fabric publish feature flags: {flags}")
    for feature_flag in flags:
        fabric_cicd.append_feature_flag(feature_flag)


def validate_publish_metadata(artifacts_root: Path, item_types_in_scope: list[str]) -> None:
    """Validate bulk-import logical IDs without modifying source metadata."""
    logical_ids: dict[UUID, Path] = {}
    for platform_path in sorted(artifacts_root.rglob(".platform")):
        try:
            platform = json.loads(platform_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise ValueError(f"Invalid JSON in Fabric system file '{platform_path}': {error}") from error
        metadata = platform.get("metadata") if isinstance(platform, dict) else None
        if not isinstance(metadata, dict):
            raise ValueError(f"Missing or invalid metadata in Fabric system file '{platform_path}'")
        if metadata.get("type") not in item_types_in_scope:
            continue
        config = platform.get("config")
        logical_id = config.get("logicalId") if isinstance(config, dict) else None
        message = f"Invalid config.logicalId {logical_id!r} in '{platform_path}': bulk publishing requires a nonzero UUID in 8-4-4-4-12 format. Correct the source .platform file; logical IDs are not regenerated automatically."
        if not isinstance(logical_id, str):
            raise ValueError(message)
        try:
            parsed_id = UUID(logical_id)
        except ValueError as error:
            raise ValueError(message) from error
        if parsed_id.int == 0 or str(parsed_id) != logical_id.lower():
            raise ValueError(message)
        if parsed_id in logical_ids:
            raise ValueError(f"Duplicate config.logicalId '{logical_id}' in '{platform_path}' and '{logical_ids[parsed_id]}': bulk publishing requires unique logical IDs within a workspace.")
        logical_ids[parsed_id] = platform_path
