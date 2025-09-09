# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import functools
import json
import logging
import os
import subprocess
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

import yaml

from fabric_workspace_deployment.manager.azure.cli import AzCli

# ---------------------------------------------------------------------------- #
# ------------------------------ DATA CLASSES -------------------------------- #
# ---------------------------------------------------------------------------- #


class Operation(Enum):
    """Enumeration of available operations."""

    DEPLOY_FABRIC_CAPACITY = "deployFabricCapacity"
    DEPLOY_FABRIC_WORKSPACE = "deployFabricWorkspace"
    DEPLOY_GIT_LINK = "deployGitLink"
    DEPLOY_RBAC = "deployRbac"
    DEPLOY_SHORTCUT = "deployShortcut"
    DEPLOY_TEMPLATE = "deployTemplate"
    DRY_RUN = "dryRun"


class PrincipalType(Enum):
    """Enumeration of principal types for RBAC."""

    GROUP = "Group"
    USER = "User"
    SERVICE_PRINCIPAL = "ServicePrincipal"


@dataclass
class LocalParams:
    """Local configuration parameters."""

    root_folder: str


@dataclass
class EndpointParams:
    """Endpoint configuration parameters."""

    analysis_service: str
    cicd: str
    power_bi: str


@dataclass
class ScopeParams:
    """Scope configuration parameters."""

    analysis_service: str


@dataclass
class ArmParams:
    """Azure Resource Manager parameters."""

    location: str
    resource_group: str
    subscription_id: str
    tenant_id: str


@dataclass
class AnalysisServiceCapacity:
    """Analysis Service capacity information."""

    id: int
    display_name: str
    description: str
    object_id: str
    parent_folder_id: int
    root_folder_id: int
    capacity_id: int
    permissions: int
    capacity_object_id: str
    capacity_sku: str
    type: int
    tenant_id: int
    contacts: list[Any]
    capacity_sku_tier: int
    default_dataset_storage_mode: int
    capacity_rollout_region: str
    capacity_rollout_name: str
    capacity_rollout_url: str
    retention_period_days: int


@dataclass
class FabricWorkspaceFolderInfo:
    """Fabric workspace folder information."""

    id: int
    display_name: str
    description: str
    object_id: str
    parent_folder_id: int
    root_folder_id: int
    capacity_id: int
    permissions: int
    capacity_object_id: str
    capacity_sku: str
    type: int
    tenant_id: int
    contacts: list[str]
    capacity_sku_tier: int
    default_dataset_storage_mode: int
    capacity_rollout_region: str
    capacity_rollout_name: str
    capacity_rollout_url: str
    retention_period_days: int


@dataclass
class FabricWorkspaceTemplateParams:
    """Fabric workspace template parameters."""

    artifacts_folder: str
    parameter_file_path: str
    item_types_in_scope: list[str]
    environment_key: str
    feature_flags: list[str]
    unpublish_orphans: bool


@dataclass
class FabricCapacityParams:
    """Fabric capacity parameters."""

    name: str
    sku: str
    administrators: list[str]


@dataclass
class FabricCapacityInfo:
    """Fabric capacity information."""

    id: str
    name: str
    type: str
    location: str
    sku_name: str
    sku_tier: str
    tags: dict[str, Any]
    fabric_id: str
    provisioning_state: str
    state: str
    administrators: list[str]


@dataclass
class OneLakeEndpoints:
    """OneLake endpoints for Fabric workspace."""

    blob_endpoint: str
    dfs_endpoint: str


@dataclass
class WorkspaceIdentity:
    """Workspace identity information."""

    application_id: str
    service_principal_id: str


@dataclass
class AutomaticLog:
    """Automatic log settings for Spark."""

    enabled: bool


@dataclass
class HighConcurrency:
    """High concurrency settings for Spark."""

    notebook_interactive_run_enabled: bool
    notebook_pipeline_run_enabled: bool


@dataclass
class DefaultPool:
    """Default pool configuration."""

    name: str
    type: str
    id: str


@dataclass
class StarterPool:
    """Starter pool configuration."""

    max_node_count: int
    max_executors: int


@dataclass
class Pool:
    """Pool configuration for Spark."""

    customize_compute_enabled: bool
    default_pool: DefaultPool
    starter_pool: StarterPool


@dataclass
class Environment:
    """Environment settings for Spark."""

    runtime_version: str


@dataclass
class Job:
    """Job settings for Spark."""

    conservative_job_admission_enabled: bool
    session_timeout_in_minutes: int


@dataclass
class SparkSettings:
    """Spark settings for Fabric workspace."""

    automatic_log: AutomaticLog
    high_concurrency: HighConcurrency
    pool: Pool
    environment: Environment
    job: Job


@dataclass
class GroupDetails:
    """Group details for role assignments."""

    group_type: str
    email: str | None


@dataclass
class UserDetails:
    """User details for role assignments."""

    user_principal_name: str


@dataclass
class ServicePrincipalDetails:
    """Service principal details for role assignments."""

    aad_app_id: str


@dataclass
class Principal:
    """Principal information for role assignments."""

    id: str
    display_name: str
    type: str
    group_details: GroupDetails | None = None
    user_details: UserDetails | None = None
    service_principal_details: ServicePrincipalDetails | None = None


@dataclass
class RoleAssignment:
    """Role assignment information."""

    id: str
    principal: Principal
    role: str


@dataclass
class FabricWorkspaceInfo:
    """Fabric workspace information."""

    id: str
    display_name: str
    description: str
    type: str
    capacity_id: str
    capacity_region: str
    one_lake_endpoints: OneLakeEndpoints
    capacity_assignment_progress: str
    workspace_identity: WorkspaceIdentity | None
    managed_private_endpoints: list[Any]
    spark_settings: SparkSettings
    role_assignments: list[RoleAssignment]


@dataclass
class FabricWorkspaceItem:
    """Fabric workspace item information."""

    id: str
    type: str
    display_name: str
    description: str
    workspace_id: str


@dataclass
class FabricWorkspaceFolderRbacDetail:
    """Fabric workspace folder RBAC detail information."""

    id: int
    user_id: int | None
    permissions: int
    given_name: str
    object_id: str
    family_name: str | None
    email_address: str | None
    aad_app_id: str | None
    user_principal_name: str | None


@dataclass
class FabricWorkspaceFolderRbacInfo:
    """Fabric workspace folder RBAC information."""

    id: int
    display_name: str
    object_id: str
    permissions: int
    shared_with_count: int
    detail: list[FabricWorkspaceFolderRbacDetail]


@dataclass
class FolderRole:
    """Folder role information."""

    id: int
    name: str
    tenant_id: str | None


@dataclass
class AccessSource:
    """Access source information for RBAC."""

    id: int
    folder_role_id: int
    folder_role: FolderRole
    artifact_link_id: str | None
    expiration: str | None
    artifact_link: str | None
    artifact_access_source_id: str | None
    artifact_access_source: str | None


@dataclass
class FabricWorkspaceItemRbacDetail:
    """Fabric workspace item RBAC detail information."""

    id: int
    user_id: int | None
    permissions: int
    given_name: str
    object_id: str
    artifact_permissions: int | None
    aad_app_id: str | None
    access_source: AccessSource | None = None


@dataclass
class FabricWorkspaceItemRbacInfo:
    """Fabric workspace item RBAC information."""

    id: int
    type: str
    display_name: str
    object_id: str
    permissions: int
    shared_with_count: int
    detail: list[FabricWorkspaceItemRbacDetail]
    artifact_permissions: int | None = None


@dataclass
class KqlDatabaseShortcut:
    """KQL Database shortcut configuration."""

    database: str
    table: str
    path: str
    query_acceleration_toggle: bool


@dataclass
class KqlDatabaseProperties:
    """KQL Database properties."""

    parent_eventhouse_item_id: str
    query_service_uri: str
    ingestion_service_uri: str
    database_type: str


@dataclass
class KqlDatabaseInfo:
    """KQL Database information."""

    id: str
    type: str
    display_name: str
    description: str
    workspace_id: str
    properties: KqlDatabaseProperties


@dataclass
class MwcScopedToken:
    """MWC scoped token information."""

    token: str
    target_uri_host: str
    capacity_object_id: str


@dataclass
class ShortcutParams:
    """Shortcut configuration parameters."""

    kql_database: list[KqlDatabaseShortcut]


@dataclass
class Identity:
    """Identity information for RBAC assignments."""

    given_name: str
    object_id: str
    principal_type: PrincipalType
    aad_app_id: str | None = None


@dataclass
class WorkspaceRbacParams:
    """Workspace RBAC assignment parameters."""

    permissions: int
    object_id: str
    purpose: str


@dataclass
class ItemRbacDetailParams:
    """Item-level RBAC detail assignment parameters."""

    permissions: int
    object_id: str
    purpose: str
    artifact_permissions: int | None = None


@dataclass
class ItemRbacParams:
    """Item-level RBAC configuration parameters."""

    type: str
    display_name: str
    detail: list[ItemRbacDetailParams]


@dataclass
class RbacParams:
    """RBAC configuration parameters."""

    purge_unmatched_role_assignments: bool
    identities: list[Identity]
    workspace: list[WorkspaceRbacParams]
    items: list[ItemRbacParams]

    def get_identity_by_object_id(self, object_id: str) -> Identity:
        """
        Get identity by object ID.

        Args:
            object_id: The object ID to lookup

        Returns:
            Identity: The identity information

        Raises:
            ValueError: If the object ID is not found in identities
        """
        for identity in self.identities:
            if identity.object_id == object_id:
                return identity
        raise ValueError(f"Identity with object ID '{object_id}' not found in identities")  # noqa: EM102

    def hydrate_workspace_rbac_with_identity(self, workspace_rbac: WorkspaceRbacParams) -> tuple[WorkspaceRbacParams, Identity]:
        """
        Hydrate workspace RBAC parameters with identity information.

        Args:
            workspace_rbac: The workspace RBAC parameters

        Returns:
            tuple: (original workspace_rbac, identity information)

        Raises:
            ValueError: If the object ID is not found in identities
        """
        identity = self.get_identity_by_object_id(workspace_rbac.object_id)
        return workspace_rbac, identity

    def hydrate_item_rbac_detail_with_identity(
        self, item_rbac_detail: ItemRbacDetailParams
    ) -> tuple[ItemRbacDetailParams, Identity]:
        """
        Hydrate item RBAC detail parameters with identity information.

        Args:
            item_rbac_detail: The item RBAC detail parameters

        Returns:
            tuple: (original item_rbac_detail, identity information)

        Raises:
            ValueError: If the object ID is not found in identities
        """
        identity = self.get_identity_by_object_id(item_rbac_detail.object_id)
        return item_rbac_detail, identity


@dataclass
class FabricWorkspaceParams:
    """Fabric workspace parameters."""

    name: str
    description: str
    dataset_storage_mode: int
    template: FabricWorkspaceTemplateParams
    capacity: FabricCapacityParams
    rbac: RbacParams
    shortcut: ShortcutParams | None = None


@dataclass
class FabricStorageParams:
    """Fabric storage parameters."""

    account: str
    container: str
    location: str
    resource_group: str
    subscription_id: str
    tenant_id: str
    shortcut_data_connection_id: str


@dataclass
class FabricParams:
    """Fabric workspace parameters."""

    workspaces: list[FabricWorkspaceParams]
    storage: FabricStorageParams


@dataclass
class CommonParams:
    """Common parameters shared across operations."""

    local: LocalParams
    endpoint: EndpointParams
    scope: ScopeParams
    arm: ArmParams
    fabric: FabricParams


# ---------------------------------------------------------------------------- #
# ----------------------------- INTERFACES ----------------------------------- #
# ---------------------------------------------------------------------------- #


class Manager(ABC):
    """Base interface for all managers."""

    def __init__(self, common_params: "CommonParams"):
        """Initialize the manager with common parameters."""
        self.common_params = common_params

    @abstractmethod
    async def execute(self) -> None:
        """Execute the manager's operation."""
        pass


class EntryPointOperator(Manager):
    """
    Interface for entry point operators.
    """

    def __init__(self, operation_params: "OperationParams"):
        """
        Initialize the entry point operator with operation parameters.
        """
        super().__init__(operation_params.common)
        self.operation_params = operation_params
        self.operation = operation_params.operation


class CicdManager(ABC):
    """
    Interface for managing Fabric CICD operations.
    """

    def __init__(self, common_params: "CommonParams"):
        """
        Initialize the CICD manager with common parameters.
        """
        self.common_params = common_params

    @abstractmethod
    async def execute(self) -> None:
        """
        Execute reconciliation for all workspaces in parallel.
        """
        pass

    @abstractmethod
    async def reconcile(self, workspace_id: str, template_params: "FabricWorkspaceTemplateParams") -> None:
        """
        Reconcile a single workspace to desired state cicd.

        Args:
            workspace_id: The Fabric workspace id
            template_params: Parameters for the fabric workspace template
        """
        pass


class ShortcutManager(ABC):
    """
    Interface for managing Fabric shortcut operations.
    """

    def __init__(self, common_params: "CommonParams"):
        """
        Initialize the shortcut manager with common parameters.
        """
        self.common_params = common_params

    @abstractmethod
    async def execute(self) -> None:
        """
        Execute reconciliation for all workspaces in parallel.
        """
        pass

    @abstractmethod
    async def reconcile(self, workspace_id: str, shortcut_params: "ShortcutParams") -> None:
        """
        Reconcile a single workspace to desired state shortcut.

        Args:
            workspace_id: The Fabric workspace id
            shortcut_params: Parameters for the fabric workspace shortcut
        """
        pass

    @abstractmethod
    async def get_all_kusto_databases(self, workspace_id: str) -> list[KqlDatabaseInfo]:
        """
        Get all KQL databases for a workspace.

        Args:
            workspace_id: The workspace ID to get KQL databases for

        Returns:
            list[KqlDatabaseInfo]: List of KQL database information
        """
        pass

    @abstractmethod
    async def get_kusto_database_mwc_token(self, workspace_id: str, database_id: str) -> MwcScopedToken:
        """
        Get MWC scoped token for a KQL database.

        Args:
            workspace_id: The workspace ID containing the database
            database_id: The KQL database ID

        Returns:
            MwcScopedToken: MWC scoped token information
        """
        pass

    @abstractmethod
    async def batch_create_kusto_database_shortcut(
        self, workspace_id: str, database_id: str, shortcut_params: "ShortcutParams"
    ) -> None:
        """
        Batch create KQL database shortcuts using MWC token.

        Args:
            workspace_id: The workspace ID containing the database
            database_id: The KQL database ID
            shortcut_params: Parameters for the shortcuts to create
        """
        pass


class RbacManager(ABC):
    """
    Interface for managing Fabric RBAC operations.
    """

    def __init__(self, common_params: "CommonParams"):
        """
        Initialize the RBAC manager with common parameters.
        """
        self.common_params = common_params

    @abstractmethod
    async def execute(self) -> None:
        """
        Execute reconciliation for all workspaces in parallel.
        """
        pass

    @abstractmethod
    async def reconcile(self, workspace_id: str, rbac_params: "RbacParams") -> None:
        """
        Reconcile a single workspace to desired state RBAC.

        Args:
            workspace_id: The Fabric workspace id
        """
        pass

    @abstractmethod
    async def get_fabric_workspace_folder_info(self, workspace_id: str) -> FabricWorkspaceFolderInfo:
        """
        Get Fabric workspace folder information.

        Args:
            workspace_id: The Fabric workspace id

        Returns:
            FabricWorkspaceFolderInfo: Fabric workspace folder information
        """
        pass

    @abstractmethod
    async def get_fabric_workspace_item_info(self, workspace_id: str) -> list[FabricWorkspaceItem]:
        """
        Get Fabric workspace item information.

        Args:
            workspace_id: The Fabric workspace id

        Returns:
            list[FabricWorkspaceItem]: List of Fabric workspace items
        """
        pass

    @abstractmethod
    async def get_fabric_workspace_folder_rbac_info(self, folder_id: int) -> FabricWorkspaceFolderRbacInfo:
        """
        Get Fabric workspace folder RBAC information.

        Args:
            folder_id: The Fabric workspace folder id

        Returns:
            FabricWorkspaceFolderRbacInfo: Fabric workspace folder RBAC information
        """
        pass

    @abstractmethod
    async def get_fabric_workspace_item_rbac_info(self, item_id: int) -> FabricWorkspaceItemRbacInfo:
        """
        Get Fabric workspace item RBAC information.

        Args:
            item_id: The Fabric workspace item id

        Returns:
            FabricWorkspaceItemRbacInfo: Fabric workspace item RBAC information
        """
        pass

    @abstractmethod
    async def update_item_role_assignment(self, item_id: str, assignment: ItemRbacDetailParams) -> None:
        """
        Update a single item role assignment via API call.

        Args:
            item_id: The workspace item ID
            assignment: The item role assignment to update
        """
        pass

    @abstractmethod
    async def update_workspace_role_assignment(self, folder_id: int, assignment: WorkspaceRbacParams) -> None:
        """
        Update a single workspace role assignment via API call.

        Args:
            folder_id: The workspace folder ID
            assignment: The role assignment to update
        """
        pass


class CapacityManager(ABC):
    """
    Interface for managing Fabric capacity operations.
    """

    def __init__(self, common_params: "CommonParams"):
        """
        Initialize the capacity manager with capacity parameters.
        """
        self.common_params = common_params

    @abstractmethod
    async def execute(self) -> None:
        """
        Execute reconciliation for all capacities in parallel.
        """
        pass

    @abstractmethod
    async def reconcile(self, capacity_params: "FabricCapacityParams") -> None:
        """
        Reconcile a single capacity to desired state.

        Args:
            capacity_params: Parameters for the fabric capacity
        """
        pass

    @abstractmethod
    async def exists(self, capacity_params: "FabricCapacityParams") -> bool:
        """
        Check if the capacity exists.

        Args:
            capacity_params: Parameters for the fabric capacity

        Returns:
            bool: True if the capacity exists, False otherwise
        """
        pass

    @abstractmethod
    async def get(self, capacity_params: "FabricCapacityParams") -> "FabricCapacityInfo":
        """
        Get capacity details.

        Args:
            capacity_params: Parameters for the fabric capacity

        Returns:
            FabricCapacityInfo: Capacity information
        """
        pass

    @abstractmethod
    async def create(self, capacity_params: "FabricCapacityParams") -> None:
        """
        Create a new capacity.

        Args:
            capacity_params: Parameters for the fabric capacity
        """
        pass

    @abstractmethod
    async def remove(self, capacity_params: "FabricCapacityParams") -> None:
        """
        Remove the capacity.

        Args:
            capacity_params: Parameters for the fabric capacity
        """
        pass

    @abstractmethod
    async def scale(self, capacity_params: "FabricCapacityParams") -> None:
        """
        Scale the capacity to a new SKU.

        Args:
            capacity_params: Parameters for the fabric capacity
        """
        pass

    @abstractmethod
    async def start(self, capacity_params: "FabricCapacityParams") -> None:
        """
        Start the capacity.

        Args:
            capacity_params: Parameters for the fabric capacity
        """
        pass

    @abstractmethod
    async def stop(self, capacity_params: "FabricCapacityParams") -> None:
        """
        Stop the capacity.

        Args:
            capacity_params: Parameters for the fabric capacity
        """
        pass

    @abstractmethod
    async def set(self, capacity_params: "FabricCapacityParams", property_path: str, value: str) -> None:
        """
        Set a property on the capacity.

        Args:
            capacity_params: Parameters for the fabric capacity
            property_path: The property path to set (e.g., "sku.name")
            value: The value to set
        """
        pass

    @abstractmethod
    async def assign(self, capacity_params: "FabricCapacityParams", workspace_name: str) -> None:
        """
        Assign the capacity to a workspace.

        Args:
            capacity_params: Parameters for the fabric capacity
            workspace_name: The workspace name
        """
        pass

    @abstractmethod
    async def unassign(self, capacity_params: "FabricCapacityParams", workspace_name: str) -> None:
        """
        Unassign the capacity from a workspace.

        Args:
            capacity_params: Parameters for the fabric capacity
            workspace_name: The workspace name
        """
        pass


class WorkspaceManager(ABC):
    """
    Interface for managing Fabric workspace operations.
    """

    def __init__(self, common_params: "CommonParams"):
        """
        Initialize the workspace manager with common parameters.
        """
        self.common_params = common_params

    @abstractmethod
    async def execute(self) -> None:
        """
        Execute reconciliation for all workspaces in parallel.
        """
        pass

    @abstractmethod
    async def reconcile(self, workspace_params: FabricWorkspaceParams) -> None:
        """
        Reconcile a single workspace to desired state.

        Args:
            workspace_params: Parameters for the fabric workspace
        """
        pass

    @abstractmethod
    async def exists(self, workspace_params: "FabricWorkspaceParams") -> bool:
        """
        Check if the workspace exists.

        Args:
            workspace_params: Parameters for the fabric workspace

        Returns:
            bool: True if the workspace exists, False otherwise
        """
        pass

    @abstractmethod
    async def get(self, workspace_params: "FabricWorkspaceParams") -> "FabricWorkspaceInfo":
        """
        Get workspace details.

        Args:
            workspace_params: Parameters for the fabric workspace

        Returns:
            FabricWorkspaceInfo: Workspace information
        """
        pass

    @abstractmethod
    async def create(self, workspace_params: "FabricWorkspaceParams") -> None:
        """
        Create a new workspace.

        Args:
            workspace_params: Parameters for the fabric workspace
        """
        pass

    @abstractmethod
    async def get_analysis_service_capacity(self, object_id: str) -> AnalysisServiceCapacity:
        """
        Get Analysis Service capacity information for a given object ID.

        Args:
            object_id: The object ID of the workspace/folder

        Returns:
            AnalysisServiceCapacity: Analysis Service capacity information

        Raises:
            RuntimeError: If the API call fails or response cannot be parsed
        """
        pass

    @abstractmethod
    async def set_analysis_service_capacity(self, object_id: str, data: str) -> None:
        """
        Set Analysis Service capacity information for a given object ID.

        Args:
            object_id: The object ID of the workspace/folder
            data: JSON string containing the data to update (e.g., '{"datasetStorageMode":2}')

        Raises:
            RuntimeError: If the API call fails
        """
        pass

    @abstractmethod
    async def create_managed_identity(self, workspace_params: "FabricWorkspaceParams") -> None:
        """
        Create a managed identity for the workspace if it doesn't exist.

        Args:
            workspace_params: Parameters for the fabric workspace

        Raises:
            RuntimeError: If the managed identity creation fails
        """
        pass

    @abstractmethod
    async def assign_workspace_storage_reader(
        self, workspace_info: "FabricWorkspaceInfo", storage_params: "FabricStorageParams"
    ) -> None:
        """
        Assign Storage Blob Data Contributor role to the workspace identity for the storage account.

        Args:
            workspace_info: Information about the fabric workspace
            storage_params: Parameters for the fabric storage

        Raises:
            RuntimeError: If the role assignment fails
        """
        pass


# ---------------------------------------------------------------------------- #
# ---------------------------------------------------------------------------- #
# ---------------------------------------------------------------------------- #


class OperationParams:
    """
    Main operation parameters container with parsing and validation capabilities.
    """

    def __init__(self, config_file_absolute_path: str, operation: str, logger: logging.Logger | None = None):
        """
        Initialize OperationParams by parsing the configuration file.

        Args:
            config_file_absolute_path: Absolute path to the configuration file
            operation: The operation to execute

        Raises:
            FileNotFoundError: If the config file doesn't exist
            json.JSONDecodeError: If the JSON is malformed
            ValueError: If required fields are missing or invalid
        """
        self.logger = logger or logging.getLogger(__name__)
        self.az_cli = AzCli(exit_on_error=True, logger=self.logger)

        try:
            self.config_data = self._load_and_process_config(
                config_file_absolute_path)
            self.operation = Operation(operation)
            self.common = self._parse_common_params(self.config_data["common"])

        except FileNotFoundError:
            self.logger.error(
                f"Configuration file not found: {config_file_absolute_path}")
            raise

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in configuration file: {e}")
            raise

        except KeyError as e:
            self.logger.error(f"Missing required field in configuration: {e}")
            msg = f"Missing required field in configuration: {e}"
            raise ValueError(msg) from e

        except Exception as e:
            self.logger.error(f"Unexpected error parsing configuration: {e}")
            raise

    def validate(self) -> bool:
        """
        Validate the operation parameters.

        Returns:
            bool: True if all parameters are valid, False otherwise
        """
        return self._validate_operation() and self._validate_common_params()

    def to_pretty_json(self) -> str:
        """
        Return a pretty formatted JSON representation of the configuration data.

        Returns:
            str: Pretty formatted JSON string of the substituted configuration data
        """
        return json.dumps(self.config_data, indent=2, ensure_ascii=False)

    @functools.cache  # noqa: B019
    def get_user_alias(self) -> str:
        """
        Gets the user's alias from their git config.

        If you always need the user's alias in a specific context,
        consider using a constant instead of calling this function directly.

        Returns:
            str: The user's alias (part before @ in their git email)

        Raises:
            RuntimeError: If no user email is found or email format is invalid
        """
        err_str = "No user email found. Please set your email in git config using 'git config --global user.email <email>'"

        try:
            result = subprocess.run(["git", "config", "user.email"], capture_output=True, text=True, check=True, timeout=10)  # fmt: skip # noqa: E501, S603, S607
            user_email = result.stdout.strip()
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            self.logger.error(f"Failed to get git user email: {e}")
            raise RuntimeError(err_str) from e

        if not user_email:
            self.logger.error("Git user email is empty")
            raise RuntimeError(err_str)

        email_parts = user_email.split("@")
        if len(email_parts) != 2:  # noqa: PLR2004
            self.logger.error(f"Invalid email format: {user_email}")
            raise RuntimeError(err_str)

        alias = email_parts[0].strip()
        if not alias:
            self.logger.error(f"Empty alias in email: {user_email}")
            raise RuntimeError(err_str)

        return alias

    # ---------------------------------------------------------------------------- #

    def _get_placeholder_resolvers(self) -> dict[str, Callable[[], str]]:
        """
        Get the mapping of placeholder names to their resolver functions.

        Add new placeholders here by mapping the placeholder name (without braces)
        to a function that returns the replacement value.

        Returns:
            dict[str, Callable[[], str]]: Mapping of placeholder names to resolver functions
        """
        return {
            "timestamp": lambda: datetime.now().isoformat(),  # noqa: DTZ005
            "unique-env-id": lambda: os.getenv("UNIQUE_ENV_ID", self.get_user_alias()),
            "user-appid": lambda: os.getenv("USER_APP_ID", self.az_cli.get_user_appid()),
            "user-display-name": lambda: os.getenv("USER_DISPLAY_NAME", f"{self.get_user_alias()}@microsoft.com"),
            "user-oid": lambda: os.getenv("USER_OBJECT_ID", self.az_cli.get_user_oid()),
            "user-principal-type": lambda: os.getenv("USER_PRINCIPAL_TYPE", "User"),
        }

    def _replace_placeholders_in_value(self, value: Any) -> Any:
        """
        Replace placeholders in a single value (string, dict, list, or primitive).

        Args:
            value: The value to process

        Returns:
            Any: The value with placeholders replaced
        """
        if isinstance(value, str):
            result = value
            resolvers = self._get_placeholder_resolvers()

            for placeholder_name, resolver_func in resolvers.items():
                placeholder_pattern = f"{{{placeholder_name}}}"
                if placeholder_pattern in result:
                    replacement_value = resolver_func()
                    result = result.replace(
                        placeholder_pattern, replacement_value)
                    self.logger.debug(
                        f"Replaced {placeholder_pattern} with {replacement_value}")

            return result

        elif isinstance(value, dict):
            return {key: self._replace_placeholders_in_value(val) for key, val in value.items()}

        elif isinstance(value, list):
            return [self._replace_placeholders_in_value(item) for item in value]

        else:
            return value

    def _load_and_process_config(self, config_file_absolute_path: str) -> dict[str, Any]:
        """
        Load configuration from file and replace magic placeholders.

        Args:
            config_file_absolute_path: Absolute path to the configuration file

        Returns:
            dict[str, Any]: Configuration data with placeholders replaced

        Raises:
            FileNotFoundError: If the config file doesn't exist
            json.JSONDecodeError: If the JSON is malformed
        """
        with open(config_file_absolute_path, encoding="utf-8") as file:
            config_data = json.load(file)
        return self._replace_placeholders_in_value(config_data)

    def _validate_operation(self) -> bool:
        """Validate the operation parameter."""
        if self.operation is None:
            self.logger.error(
                f"operation cannot be empty or left as default: {self.operation}")
            return False
        return True

    def _validate_common_params(self) -> bool:
        """Validate common parameters."""
        return (
            self._validate_local_params()
            and self._validate_endpoint_params()
            and self._validate_scope_params()
            and self._validate_arm_params()
            and self._validate_fabric_params()
        )

    def _validate_local_params(self) -> bool:
        """Validate local parameters."""
        if not self.common.local.root_folder or self._is_folder_empty(self.common.local.root_folder):
            self.logger.error(
                f"rootFolder cannot be empty: {self.common.local.root_folder}")
            return False
        return True

    def _validate_endpoint_params(self) -> bool:
        """Validate endpoint parameters."""
        if not self.common.endpoint.analysis_service:
            self.logger.error(
                f"analysisService endpoint cannot be empty: {self.common.endpoint.analysis_service}")
            return False
        if not self.common.endpoint.cicd:
            self.logger.error(
                f"cicd endpoint cannot be empty: {self.common.endpoint.cicd}")
            return False
        if not self.common.endpoint.power_bi:
            self.logger.error(
                f"powerBi endpoint cannot be empty: {self.common.endpoint.power_bi}")
            return False
        if not self.common.endpoint.analysis_service.startswith("https://"):
            self.logger.error(
                f"analysisService endpoint must be a valid HTTPS URL: {self.common.endpoint.analysis_service}")
            return False
        if not self.common.endpoint.cicd.startswith("https://"):
            self.logger.error(
                f"cicd endpoint must be a valid HTTPS URL: {self.common.endpoint.cicd}")
            return False
        if not self.common.endpoint.power_bi.startswith("https://"):
            self.logger.error(
                f"powerBi endpoint must be a valid HTTPS URL: {self.common.endpoint.power_bi}")
            return False

        return True

    def _validate_scope_params(self) -> bool:
        """Validate scope parameters."""
        if not self.common.scope.analysis_service:
            self.logger.error(
                f"analysisService scope cannot be empty: {self.common.scope.analysis_service}")
            return False
        if not self.common.scope.analysis_service.startswith("https://"):
            self.logger.error(
                f"analysisService scope must be a valid HTTPS URL: {self.common.scope.analysis_service}")
            return False

        return True

    def _validate_arm_params(self) -> bool:
        """Validate ARM parameters."""
        if (
            not self.common.arm.resource_group
            or not self.common.arm.location
            or not self.common.arm.subscription_id
            or not self.common.arm.tenant_id
        ):
            self.logger.error(
                f"resourceGroup: {self.common.arm.resource_group}, "
                f"location: {self.common.arm.location}, subscriptionId: {self.common.arm.subscription_id}, "
                f"or tenantId: {self.common.arm.tenant_id} cannot be empty"
            )
            return False
        return True

    def _validate_fabric_params(self) -> bool:
        """Validate Fabric parameters."""
        if not self.common.fabric.workspaces or len(self.common.fabric.workspaces) == 0:
            self.logger.error(
                "At least one Fabric workspace must be configured")
            return False

        for i, workspace in enumerate(self.common.fabric.workspaces):
            if not workspace.name:
                self.logger.error(
                    f"Workspace name at index {i} cannot be empty")
                return False

            if not workspace.description:
                self.logger.error(
                    f"Workspace description at index {i} cannot be empty")
                return False

            if not workspace.dataset_storage_mode:
                self.logger.error(
                    f"Workspace dataset_storage_mode at index {i} cannot be empty")
                return False

            if not workspace.template.artifacts_folder:
                self.logger.error(
                    f"Workspace template artifacts_folder at index {i} cannot be empty")
                return False

            if not workspace.template.parameter_file_path:
                self.logger.error(
                    f"Workspace template parameter_file_path at index {i} cannot be empty")
                return False

            if not workspace.template.item_types_in_scope or len(workspace.template.item_types_in_scope) < 1:
                self.logger.error(
                    f"Workspace template item_types_in_scope at index {i} cannot be empty")
                return False

            if not workspace.template.environment_key:
                self.logger.error(
                    f"Workspace template environment_key at index {i} cannot be empty")
                return False

            if not workspace.template.feature_flags:
                self.logger.error(
                    f"Workspace template feature_flags at index {i} cannot be empty")
                return False

            if not workspace.template.unpublish_orphans:
                self.logger.error(
                    f"Workspace template unpublish_orphans at index {i} cannot be empty")
                return False

            if self._is_yaml_file_invalid(str(Path(self.common.local.root_folder) / workspace.template.parameter_file_path)):
                self.logger.error(f"Workspace template parameter_file_path at index {i} is invalid: {workspace.template.parameter_file_path}")  # fmt: skip  # noqa: E501
                return False

            if not workspace.capacity.name:
                self.logger.error(
                    f"Workspace capacity name at index {i} cannot be empty")
                return False

            if (
                not workspace.capacity.name[0].islower()
                or not workspace.capacity.name.isalnum()
                or not workspace.capacity.name.islower()
            ):
                self.logger.error(
                    f"Workspace capacity name at index {i} must start with a lowercase letter and contain only lowercase letters and digits (no spaces, underscores, or dashes): {workspace.capacity.name}"  # noqa: E501
                )
                return False

            if not workspace.capacity.sku:
                self.logger.error(
                    f"Workspace capacity sku at index {i} cannot be empty")
                return False

            if not workspace.capacity.administrators or not len(workspace.capacity.administrators) >= 1:
                self.logger.error(
                    f"Workspace capacity administrators at index {i} must contain at least one administrator")
                return False

            if workspace.shortcut is not None and not self._validate_shortcut_params(workspace.shortcut, i):
                return False

            if workspace.rbac is not None and not self._validate_rbac_params(workspace.rbac, i):
                return False

        return self._validate_fabric_storage_params()

    def _validate_fabric_storage_params(self) -> bool:
        """Validate Fabric Storage parameters."""
        storage = self.common.fabric.storage
        if (
            not storage.account
            or not storage.container
            or not storage.location
            or not storage.resource_group
            or not storage.subscription_id
            or not storage.tenant_id
            or not storage.shortcut_data_connection_id
        ):
            self.logger.error(
                f"FabricStorageParams members cannot be empty: account={storage.account}, "
                f"container={storage.container}, location={storage.location}, "
                f"resourceGroup={storage.resource_group}, subscriptionId={storage.subscription_id}, "
                f"tenantId={storage.tenant_id}"
            )
            return False
        return True

    def _validate_shortcut_params(self, shortcut: ShortcutParams, workspace_index: int) -> bool:
        """Validate shortcut parameters."""
        if shortcut.kql_database is None:
            self.logger.error(
                f"Workspace shortcut kqlDatabase at index {workspace_index} cannot be None")
            return False

        for j, kql_db in enumerate(shortcut.kql_database):
            if not kql_db.database:
                self.logger.error(
                    f"Workspace shortcut kqlDatabase[{j}].database at index {workspace_index} cannot be empty")
                return False

            if not kql_db.table:
                self.logger.error(
                    f"Workspace shortcut kqlDatabase[{j}].table at index {workspace_index} cannot be empty")
                return False

            if not kql_db.path:
                self.logger.error(
                    f"Workspace shortcut kqlDatabase[{j}].path at index {workspace_index} cannot be empty")
                return False

            if kql_db.query_acceleration_toggle is None:
                self.logger.error(
                    f"Workspace shortcut kqlDatabase[{j}].queryAccelerationToggle at index {workspace_index} cannot be None"
                )
                return False

        return True

    def _validate_rbac_params(self, rbac: RbacParams, workspace_index: int) -> bool:
        """Validate RBAC parameters."""
        if rbac.purge_unmatched_role_assignments is None:
            self.logger.error(
                f"Workspace rbac purgeUnmatchedRoleAssignments at index {workspace_index} cannot be None")
            return False

        if rbac.identities is None:
            self.logger.error(
                f"Workspace rbac identities at index {workspace_index} cannot be None")
            return False

        # Create a lookup set for identity object IDs
        identity_object_ids = {
            identity.object_id for identity in rbac.identities}

        for j, identity in enumerate(rbac.identities):
            if not self._validate_identity_params(identity, workspace_index, j):
                return False

        if rbac.workspace is None:
            self.logger.error(
                f"Workspace rbac workspace at index {workspace_index} cannot be None")
            return False

        for j, workspace_rbac in enumerate(rbac.workspace):
            if not self._validate_workspace_rbac_params(workspace_rbac, workspace_index, j, identity_object_ids):
                return False

        return all(
            self._validate_item_rbac_params(
                item_rbac, workspace_index, k, identity_object_ids)
            for k, item_rbac in enumerate(rbac.items)
        )

    def _validate_identity_params(self, identity: Identity, workspace_index: int, identity_index: int) -> bool:
        """Validate identity parameters."""
        if not identity.given_name:
            self.logger.error(
                f"Workspace rbac identities[{identity_index}].givenName at index {workspace_index} cannot be empty")
            return False

        if not identity.object_id:
            self.logger.error(
                f"Workspace rbac identities[{identity_index}].objectId at index {workspace_index} cannot be empty")
            return False

        if identity.principal_type == PrincipalType.SERVICE_PRINCIPAL and not identity.aad_app_id:
            self.logger.error(
                f"Workspace rbac identities[{identity_index}].aadAppId at index {workspace_index} cannot be empty for service principals"  # noqa: E501
            )
            return False

        return True

    def _validate_workspace_rbac_params(
        self, workspace_rbac: WorkspaceRbacParams, workspace_index: int, rbac_index: int, identity_object_ids: set[str]
    ) -> bool:
        """Validate workspace RBAC parameters."""
        if workspace_rbac.permissions is None:
            self.logger.error(
                f"Workspace rbac workspace[{rbac_index}].permissions at index {workspace_index} cannot be None")
            return False

        if not workspace_rbac.object_id:
            self.logger.error(
                f"Workspace rbac workspace[{rbac_index}].objectId at index {workspace_index} cannot be empty")
            return False

        if workspace_rbac.object_id not in identity_object_ids:
            self.logger.error(
                f"Workspace rbac workspace[{rbac_index}].objectId '{workspace_rbac.object_id}' at index {workspace_index} not found in identities"  # noqa: E501
            )
            return False

        if not workspace_rbac.purpose:
            self.logger.error(
                f"Workspace rbac workspace[{rbac_index}].purpose at index {workspace_index} cannot be empty")
            return False

        return True

    def _validate_item_rbac_params(
        self, item_rbac: ItemRbacParams, workspace_index: int, item_index: int, identity_object_ids: set[str]
    ) -> bool:
        """Validate item RBAC parameters."""
        if not item_rbac.type:
            self.logger.error(
                f"Workspace rbac items[{item_index}].type at index {workspace_index} cannot be empty")
            return False

        if not item_rbac.display_name:
            self.logger.error(
                f"Workspace rbac items[{item_index}].displayName at index {workspace_index} cannot be empty")
            return False

        if item_rbac.detail is None:
            self.logger.error(
                f"Workspace rbac items[{item_index}].detail at index {workspace_index} cannot be None")
            return False

        for j, detail_rbac in enumerate(item_rbac.detail):
            if not self._validate_item_rbac_detail_params(detail_rbac, workspace_index, item_index, j, identity_object_ids):
                return False

        return True

    def _validate_item_rbac_detail_params(
        self,
        detail_rbac: ItemRbacDetailParams,
        workspace_index: int,
        item_index: int,
        detail_index: int,
        identity_object_ids: set[str],
    ) -> bool:
        """Validate item RBAC detail parameters."""
        if detail_rbac.permissions is None:
            self.logger.error(
                f"Workspace rbac items[{item_index}].detail[{detail_index}].permissions at index {workspace_index} cannot be None"
            )
            return False

        if not detail_rbac.object_id:
            self.logger.error(
                f"Workspace rbac items[{item_index}].detail[{detail_index}].objectId at index {workspace_index} cannot be empty"
            )
            return False

        if detail_rbac.object_id not in identity_object_ids:
            self.logger.error(
                f"Workspace rbac items[{item_index}].detail[{detail_index}].objectId '{detail_rbac.object_id}' at index {workspace_index} not found in identities"  # noqa: E501
            )
            return False

        if not detail_rbac.purpose:
            self.logger.error(
                f"Workspace rbac items[{item_index}].detail[{detail_index}].purpose at index {workspace_index} cannot be empty"
            )
            return False

        return True

    # ---------------------------------------------------------------------------- #

    def _parse_principal_type(self, data: dict[str, Any]) -> PrincipalType:
        """Parse the principal type from JSON data."""
        principal_type_str = data.get("principalType")
        principal_type_mapping = {
            "ServicePrincipal": PrincipalType.SERVICE_PRINCIPAL,
            "Group": PrincipalType.GROUP,
            "User": PrincipalType.USER,
        }
        if isinstance(principal_type_str, str):
            return principal_type_mapping.get(principal_type_str, PrincipalType.USER)
        return PrincipalType.USER

    def _is_folder_empty(self, folder_path: str) -> bool:
        """Check if a folder is empty or doesn't exist."""
        if not folder_path:
            return True
        path_obj = Path(folder_path)
        return not path_obj.exists() or not path_obj.is_dir() or not any(path_obj.iterdir())

    def _is_file_empty(self, file_path: str) -> bool:
        """Check if a file is empty or doesn't exist."""
        if not file_path:
            return True
        path_obj = Path(file_path)
        return not path_obj.exists() or not path_obj.is_file() or path_obj.stat().st_size == 0

    def _is_json_file_invalid(self, file_path: str) -> bool:
        """Check if a JSON file is invalid."""
        try:
            with open(file_path, encoding="utf-8") as f:
                json.load(f)
            return False
        except (OSError, json.JSONDecodeError) as e:
            self.logger.error(
                f"Failed to JSON parse {file_path} with error: {e}")
            return True

    def _is_yaml_file_invalid(self, file_path: str) -> bool:
        """Check if a YAML file is invalid."""
        try:
            with open(file_path, encoding="utf-8") as f:
                yaml.safe_load(f)
            return False
        except (OSError, yaml.YAMLError) as e:
            self.logger.error(
                f"Failed to YAML parse {file_path} with error: {e}")
            return True

    # ---------------------------------------------------------------------------- #

    def _parse_common_params(self, data: dict[str, Any]) -> CommonParams:
        """Parse common parameters."""
        return CommonParams(
            local=self._parse_local_params(data["local"]),
            endpoint=self._parse_endpoint_params(data["endpoint"]),
            scope=self._parse_scope_params(data["scope"]),
            arm=self._parse_arm_params(data["arm"]),
            fabric=self._parse_fabric_params(data["fabric"]),
        )

    def _parse_local_params(self, data: dict[str, Any]) -> LocalParams:
        """Parse local parameters."""
        return LocalParams(root_folder=data["rootFolder"])

    def _parse_endpoint_params(self, data: dict[str, Any]) -> EndpointParams:
        """Parse endpoint parameters."""
        return EndpointParams(analysis_service=data["analysisService"], cicd=data["cicd"], power_bi=data["powerBi"])

    def _parse_scope_params(self, data: dict[str, Any]) -> ScopeParams:
        """Parse scope parameters."""
        return ScopeParams(analysis_service=data["analysisService"])

    def _parse_arm_params(self, data: dict[str, Any]) -> ArmParams:
        """Parse ARM parameters."""
        return ArmParams(
            location=data["location"],
            resource_group=data["resourceGroup"],
            subscription_id=data["subscriptionId"],
            tenant_id=data["tenantId"],
        )

    def _parse_fabric_params(self, data: dict[str, Any]) -> FabricParams:
        """Parse Fabric parameters."""
        return FabricParams(
            workspaces=self._parse_fabric_workspaces(data["workspaces"]),
            storage=self._parse_fabric_storage_params(data["storage"]),
        )

    def _parse_fabric_workspaces(self, data: list[dict[str, Any]]) -> list[FabricWorkspaceParams]:
        """Parse Fabric workspaces."""
        workspaces = []
        for workspace_data in data:
            workspaces.append(
                self._parse_fabric_workspace_params(workspace_data))
        return workspaces

    def _parse_fabric_workspace_params(self, data: dict[str, Any]) -> FabricWorkspaceParams:
        """Parse a single Fabric workspace."""
        shortcut = None
        if "shortcut" in data:
            shortcut = self._parse_shortcut_params(data["shortcut"])

        return FabricWorkspaceParams(
            name=data["name"],
            description=data["description"],
            dataset_storage_mode=data["datasetStorageMode"],
            template=self._parse_fabric_workspace_template_params(
                data["template"]),
            capacity=self._parse_fabric_capacity_params(data["capacity"]),
            shortcut=shortcut,
            rbac=self._parse_rbac_params(data["rbac"]),
        )

    def _parse_fabric_workspace_template_params(self, data: dict[str, Any]) -> FabricWorkspaceTemplateParams:
        """Parse Fabric workspace template parameters."""
        return FabricWorkspaceTemplateParams(
            artifacts_folder=data["artifactsFolder"],
            parameter_file_path=data["parameterFilePath"],
            item_types_in_scope=data["itemTypesInScope"],
            environment_key=data["environmentKey"],
            feature_flags=data["featureFlags"],
            unpublish_orphans=data["unpublishOrphans"],
        )

    def _parse_fabric_capacity_params(self, data: dict[str, Any]) -> FabricCapacityParams:
        """Parse Fabric capacity parameters."""
        return FabricCapacityParams(
            administrators=data["administrators"],
            name=data["name"],
            sku=data["sku"],
        )

    def _parse_fabric_storage_params(self, data: dict[str, Any]) -> FabricStorageParams:
        """Parse Fabric Storage parameters."""
        return FabricStorageParams(
            account=data["account"],
            container=data["container"],
            location=data["location"],
            resource_group=data["resourceGroup"],
            subscription_id=data["subscriptionId"],
            tenant_id=data["tenantId"],
            shortcut_data_connection_id=data["shortcutDataConnectionId"],
        )

    def _parse_shortcut_params(self, data: dict[str, Any]) -> ShortcutParams:
        """Parse shortcut parameters."""
        kql_database = []
        if "kqlDatabase" in data:
            for kql_db_data in data["kqlDatabase"]:
                kql_database.append(
                    self._parse_kql_database_shortcut(kql_db_data))

        return ShortcutParams(kql_database=kql_database)

    def _parse_kql_database_shortcut(self, data: dict[str, Any]) -> KqlDatabaseShortcut:
        """Parse KQL database shortcut parameters."""
        return KqlDatabaseShortcut(
            database=data["database"],
            table=data["table"],
            path=data["path"],
            query_acceleration_toggle=data["queryAccelerationToggle"],
        )

    def _parse_rbac_params(self, data: dict[str, Any]) -> RbacParams:
        """Parse RBAC parameters."""
        identities = []
        if "identities" in data:
            for identity_data in data["identities"]:
                identities.append(self._parse_identity_params(identity_data))

        workspace_rbac = []
        if "workspace" in data:
            for workspace_rbac_data in data["workspace"]:
                workspace_rbac.append(
                    self._parse_workspace_rbac_params(workspace_rbac_data))

        items_rbac = []
        for item_rbac_data in data["items"]:
            items_rbac.append(self._parse_item_rbac_params(item_rbac_data))

        return RbacParams(
            purge_unmatched_role_assignments=data["purgeUnmatchedRoleAssignments"],
            identities=self._deduplicate_list(identities),
            workspace=self._deduplicate_list(workspace_rbac),
            items=self._deduplicate_list(items_rbac),
        )

    def _deduplicate_list(self, items: list[Any]) -> list[Any]:
        """
        Remove duplicate items from a list based on deep equality.

        Args:
            items: List of items to deduplicate

        Returns:
            list[Any]: Deduplicated list maintaining original order
        """
        if not items:
            return items

        deduplicated: list[Any] = []
        for item in items:
            if not any(self._deep_equals(item, existing) for existing in deduplicated):
                deduplicated.append(item)
            else:
                self.logger.debug(f"Removing duplicate item: {item}")

        if len(deduplicated) != len(items):
            self.logger.info(
                f"Removed {len(items) - len(deduplicated)} duplicate entries from list")

        return deduplicated

    def _deep_equals(self, obj1: Any, obj2: Any) -> bool:
        """
        Compare two objects for deep equality.

        Args:
            obj1: First object to compare
            obj2: Second object to compare

        Returns:
            bool: True if objects are deeply equal, False otherwise
        """
        if hasattr(obj1, "__dataclass_fields__") and hasattr(obj2, "__dataclass_fields__"):
            if type(obj1) != type(obj2):  # noqa: E721
                return False

            for field_name in obj1.__dataclass_fields__:
                field1 = getattr(obj1, field_name)
                field2 = getattr(obj2, field_name)
                if not self._deep_equals(field1, field2):
                    return False
            return True

        elif isinstance(obj1, list) and isinstance(obj2, list):
            if len(obj1) != len(obj2):
                return False
            return all(self._deep_equals(item1, item2) for item1, item2 in zip(obj1, obj2, strict=False))

        elif isinstance(obj1, dict) and isinstance(obj2, dict):
            if set(obj1.keys()) != set(obj2.keys()):
                return False
            return all(self._deep_equals(obj1[key], obj2[key]) for key in obj1.keys())  # noqa: SIM118

        else:
            return obj1 == obj2

    def _parse_identity_params(self, data: dict[str, Any]) -> Identity:
        """Parse identity parameters."""
        return Identity(
            given_name=data["givenName"],
            object_id=data["objectId"],
            principal_type=self._parse_principal_type(data),
            aad_app_id=data.get("aadAppId"),
        )

    def _parse_workspace_rbac_params(self, data: dict[str, Any]) -> WorkspaceRbacParams:
        """Parse workspace RBAC parameters."""
        return WorkspaceRbacParams(
            permissions=data["permissions"],
            object_id=data["objectId"],
            purpose=data["purpose"],
        )

    def _parse_item_rbac_params(self, data: dict[str, Any]) -> ItemRbacParams:
        """Parse item RBAC parameters."""
        detail_rbac = []
        for detail_rbac_data in data["detail"]:
            detail_rbac.append(
                self._parse_item_rbac_detail_params(detail_rbac_data))

        return ItemRbacParams(
            type=data["type"],
            display_name=data["displayName"],
            detail=detail_rbac,
        )

    def _parse_item_rbac_detail_params(self, data: dict[str, Any]) -> ItemRbacDetailParams:
        """Parse item RBAC detail parameters."""
        return ItemRbacDetailParams(
            permissions=data["permissions"],
            artifact_permissions=data.get("artifactPermissions"),
            object_id=data["objectId"],
            purpose=data["purpose"],
        )
