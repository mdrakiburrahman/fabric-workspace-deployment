# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import base64
import functools
import json
import logging
import os
import random
import re
import subprocess
import tempfile
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any
from PIL import Image

import requests
import yaml

from fabric_workspace_deployment.manager.azure.cli import AzCli

# ---------------------------------------------------------------------------- #
# --------------------------- HTTP RETRY CONSTANTS --------------------------- #
# ---------------------------------------------------------------------------- #

HTTP_RETRYABLE_STATUS_CODES = frozenset(
    [
        408,  # Request Timeout
        429,  # Too Many Requests
        500,  # Internal Server Error
        502,  # Bad Gateway
        503,  # Service Unavailable
        504,  # Gateway Timeout
    ]
)

MAX_RETRY_ATTEMPTS = 10
MAX_RETRY_DELAY_SECONDS = 60
INITIAL_RETRY_DELAY_SECONDS = 1

# ---------------------------------------------------------------------------- #
# --------------------------- RBAC CONSTANTS --------------------------------- #
# ---------------------------------------------------------------------------- #

ALLOWED_STORAGE_ROLES = frozenset(["Storage Blob Data Reader", "Storage Blob Data Contributor", "Storage Blob Data Owner"])
SPARK_JOB_DEFINITION_PLATFORM_FILE = ".platform"
SPARK_JOB_DEFINITION_V1_FILE = "SparkJobDefinitionV1.json"


class HttpRetryHandler:
    """
    HTTP retry handler with exponential backoff for retryable errors.

    This class provides a reusable, dependency-injectable way to handle HTTP retries
    with exponential backoff and jitter for transient failures.
    """

    def __init__(
        self,
        max_attempts: int = MAX_RETRY_ATTEMPTS,
        max_delay_seconds: float = MAX_RETRY_DELAY_SECONDS,
        initial_delay_seconds: float = INITIAL_RETRY_DELAY_SECONDS,
        retryable_status_codes: frozenset[int] = HTTP_RETRYABLE_STATUS_CODES,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the HTTP retry handler.

        Args:
            max_attempts: Maximum number of retry attempts
            max_delay_seconds: Maximum retry delay in seconds
            initial_delay_seconds: Initial retry delay in seconds
            retryable_status_codes: Set of HTTP status codes that should trigger retries
            logger: Optional logger for logging retry attempts
        """
        self.max_attempts = max_attempts
        self.max_delay_seconds = max_delay_seconds
        self.initial_delay_seconds = initial_delay_seconds
        self.retryable_status_codes = retryable_status_codes
        self.logger = logger or logging.getLogger(__name__)

    def execute(self, func: Callable, *args, **kwargs) -> requests.Response:
        """
        Execute an HTTP request with retry logic.

        Args:
            func: The requests function to call (e.g., requests.get, requests.post)
            *args: Positional arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function

        Returns:
            requests.Response: The successful response

        Raises:
            The last exception encountered if all retries are exhausted
        """
        last_exception = None

        for attempt in range(1, self.max_attempts + 1):

            try:
                response = func(*args, **kwargs)
                response.raise_for_status()
                return response

            except requests.exceptions.HTTPError as e:
                last_exception = e
                if e.response is None or e.response.status_code not in self.retryable_status_codes:
                    raise

                if attempt >= self.max_attempts:
                    self.logger.error(f"Max retry attempts ({self.max_attempts}) exhausted for {func.__name__} " f"to {args[0] if args else 'unknown URL'}. Last error: {e}")
                    raise

                delay = self._calculate_delay(attempt)

                self.logger.warning(f"HTTP {e.response.status_code} error on attempt {attempt}/{self.max_attempts} " f"for {func.__name__} to {args[0] if args else 'unknown URL'}. " f"Retrying in {delay:.2f}s...")

                time.sleep(delay)

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                last_exception = e

                if attempt >= self.max_attempts:
                    self.logger.error(f"Max retry attempts ({self.max_attempts}) exhausted for {func.__name__} " f"to {args[0] if args else 'unknown URL'}. Last error: {e}")
                    raise

                delay = self._calculate_delay(attempt)

                self.logger.warning(f"Network error ({type(e).__name__}) on attempt {attempt}/{self.max_attempts} " f"for {func.__name__} to {args[0] if args else 'unknown URL'}. " f"Retrying in {delay:.2f}s...")

                time.sleep(delay)

        if last_exception:
            raise last_exception

        raise RuntimeError("Unexpected retry loop exit")

    def _calculate_delay(self, attempt: int) -> float:
        """
        Calculate the delay for the next retry attempt with exponential backoff and jitter.

        Args:
            attempt: The current attempt number (1-indexed)

        Returns:
            float: The delay in seconds
        """
        delay = min(self.initial_delay_seconds * (2 ** (attempt - 1)), self.max_delay_seconds)
        jitter = random.uniform(0, delay * 0.25)
        return delay + jitter


# ---------------------------------------------------------------------------- #
# ------------------------------ DATA CLASSES -------------------------------- #
# ---------------------------------------------------------------------------- #


class Operation(Enum):
    """Enumeration of available operations."""

    DEPLOY_FABRIC_CAPACITY = "deployFabricCapacity"
    DEPLOY_FABRIC_WORKSPACE = "deployFabricWorkspace"
    DEPLOY_GIT_LINK = "deployGitLink"
    DEPLOY_MODEL = "deployModel"
    DEPLOY_RBAC = "deployRbac"
    DEPLOY_SHORTCUT = "deployShortcut"
    DEPLOY_SPARK = "deploySpark"
    DEPLOY_TEMPLATE = "deployTemplate"
    DRY_RUN = "dryRun"


class PrincipalType(Enum):
    """Enumeration of principal types for RBAC."""

    GROUP = "Group"
    USER = "User"
    SERVICE_PRINCIPAL = "ServicePrincipal"


class NodeSizeFamily(Enum):
    """Enumeration of node size families for Spark pools."""

    MEMORY_OPTIMIZED = "MemoryOptimized"


class NodeSize(Enum):
    """Enumeration of node sizes for Spark pools."""

    SMALL = "Small"
    MEDIUM = "Medium"
    LARGE = "Large"
    X_LARGE = "X-Large"
    XX_LARGE = "XX-Large"


class SparkRuntimeVersion(Enum):
    """Enumeration of Spark runtime versions."""

    VERSION_1_2 = "1.2"
    VERSION_1_3 = "1.3"
    VERSION_2_0 = "2.0"


class ArtifactType(Enum):
    """Enumeration of Fabric artifact types."""

    MODEL = "Model"
    SPARK_JOB_DEFINITION = "SparkJobDefinition"


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
    capacity_rollout_region: str
    capacity_rollout_name: str
    capacity_rollout_url: str
    retention_period_days: int
    default_dataset_storage_mode: int = 1
    icon: str | None = None


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
    capacity_rollout_region: str
    capacity_rollout_name: str
    capacity_rollout_url: str
    retention_period_days: int
    default_dataset_storage_mode: int = 1


@dataclass
@dataclass
class CustomLibrarySource:
    """Source configuration for custom library files."""

    folder_path: str
    prefix: str
    suffix: str
    error_on_multiple: bool


@dataclass
class CustomLibraryDest:
    """Destination configuration for custom library files."""

    folder_path: list[str]


@dataclass
class CustomLibrary:
    """Custom library configuration for Spark libraries."""

    source: CustomLibrarySource
    dest: CustomLibraryDest


@dataclass
class AutoScale:
    """Auto scale configuration for Spark pools."""

    enabled: bool
    min_node_count: int
    max_node_count: int


@dataclass
class DynamicExecutorAllocation:
    """Dynamic executor allocation configuration for Spark pools."""

    enabled: bool
    min_executors: int
    max_executors: int


@dataclass
class PoolDetails:
    """Spark pool details configuration."""

    id: str
    name: str
    node_size_family: NodeSizeFamily
    node_size: NodeSize
    auto_scale: AutoScale
    dynamic_executor_allocation: DynamicExecutorAllocation


@dataclass
class MachineLearningAutoLog:
    """Machine learning auto log configuration."""

    enabled: bool


@dataclass
class JobManagement:
    """Job management configuration for Spark pools."""

    conservative_job_admission_enabled: bool
    session_timeout_in_minutes: int


@dataclass
class HighConcurrencyConfig:
    """High concurrency configuration for Spark pools."""

    enabled: bool
    notebook_pipeline_run_enabled: bool


@dataclass
class SparkPool:
    """Spark pool configuration."""

    enable_customized_compute_conf: bool
    machine_learning_auto_log: MachineLearningAutoLog
    job_management: JobManagement
    high_concurrency: HighConcurrencyConfig
    details: PoolDetails
    runtime_version: SparkRuntimeVersion


@dataclass
class FabricSparkParams:
    """Fabric Spark configuration parameters."""

    pools: list[SparkPool]


@dataclass
class SparkJobDefinitionV1Config:
    """Spark Job Definition V1 configuration file content."""

    executable_file: str
    default_lakehouse_artifact_id: str
    main_class: str
    additional_lakehouse_ids: list[str]
    retry_policy: str | None
    command_line_arguments: str
    additional_library_uris: list[str]
    language: str
    environment_artifact_id: str | None


@dataclass
class SparkJobDefinition:
    """Spark Job Definition configuration."""

    display_name: str
    description: str
    nested_folder_path: str
    default_lakehouse_artifact_name: str
    spark_job_definition_v1_config: SparkJobDefinitionV1Config


@dataclass
class FabricWorkspaceTemplateParams:
    """Fabric workspace template parameters."""

    artifacts_folder: str
    parameter_file_path: str
    item_types_in_scope: list[str]
    environment_key: str
    feature_flags: list[str]
    unpublish_orphans: bool
    custom_libraries: list[CustomLibrary]
    spark_job_definitions: list[SparkJobDefinition]


@dataclass
class FabricFolderArtifact:
    """Fabric folder artifact information."""

    id: int
    object_id: str
    type: int
    type_name: str
    display_name: str
    permissions: int
    is_hidden: bool
    artifact_permissions: int


@dataclass
class FabricSubfolderArtifacts:
    """Fabric subfolder artifact information."""

    id: int
    display_name: str
    object_id: str
    folder_id: int
    last_updated_date: str


@dataclass
class FabricFolderCollection:
    """Fabric folder containing artifacts."""

    artifacts: list[FabricFolderArtifact]


@dataclass
class ArtifactRequest:
    """Request payload for creating a Fabric artifact."""

    artifact_type: str
    description: str
    display_name: str


@dataclass
class FabricArtifact:
    """Fabric artifact information returned from creation."""

    object_id: str
    artifact_type: str
    display_name: str
    description: str


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
    group_id: int | None
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
    group_id: int | None
    user_id: int | None
    group_type: int | None
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
    group_id: int | None
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
    fabric_principal_id: int | None = None


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
class SqlEndpointSecurity:
    """SQL endpoint universal security configuration."""

    enabled: bool
    skip_reconcile: list[str]


@dataclass
class ModelParams:
    """Model configuration parameters."""

    display_name: str
    direct_lake_auto_sync: bool


@dataclass
class UniversalSecurity:
    """
    Universal security configuration for the workspace - we currently enforce
    the same setting across all items in a given workspace to keep things
    simple, but note that OneSecurity can be configured at a per item level.

    In general, it doesn't make a whole lot of sense to have different items
    managed differently in a workspace deployment (but it's good Fabric has the
    flexibility for those who need it).
    """

    sql_endpoint: SqlEndpointSecurity


@dataclass
class DatamartParameter:
    """Datamart parameters."""

    name: str
    value: str


@dataclass
class DatamartParametersResponse:
    """Response for datamart parameters."""

    parameters: list[DatamartParameter]


@dataclass
class DatamartBatchResponse:
    """Response for datamart batch polling."""

    batch_id: str
    progress_state: str
    batch_type: str


@dataclass
class RbacParams:
    """RBAC configuration parameters."""

    purge_unmatched_role_assignments: bool
    workspace: list[WorkspaceRbacParams]
    items: list[ItemRbacParams]
    universal_security: UniversalSecurity

    def get_identity_by_object_id(self, object_id: str, identities: list[Identity]) -> Identity:
        """
        Get identity by object ID.

        Args:
            object_id: The object ID to lookup
            identities: List of identities to search

        Returns:
            Identity: The identity information

        Raises:
            ValueError: If the object ID is not found in identities
        """
        for identity in identities:
            if identity.object_id == object_id:
                return identity
        raise ValueError(f"Identity with object ID '{object_id}' not found in identities")  # noqa: EM102

    def hydrate_workspace_rbac_with_identity(self, workspace_rbac: WorkspaceRbacParams, identities: list[Identity]) -> tuple[WorkspaceRbacParams, Identity]:
        """
        Hydrate workspace RBAC parameters with identity information.

        Args:
            workspace_rbac: The workspace RBAC parameters
            identities: List of identities to search

        Returns:
            tuple: (original workspace_rbac, identity information)

        Raises:
            ValueError: If the object ID is not found in identities
        """
        identity = self.get_identity_by_object_id(workspace_rbac.object_id, identities)
        return workspace_rbac, identity

    def hydrate_item_rbac_detail_with_identity(self, item_rbac_detail: ItemRbacDetailParams, identities: list[Identity]) -> tuple[ItemRbacDetailParams, Identity]:
        """
        Hydrate item RBAC detail parameters with identity information.

        Args:
            item_rbac_detail: The item RBAC detail parameters
            identities: List of identities to search

        Returns:
            tuple: (original item_rbac_detail, identity information)

        Raises:
            ValueError: If the object ID is not found in identities
        """
        identity = self.get_identity_by_object_id(item_rbac_detail.object_id, identities)
        return item_rbac_detail, identity


@dataclass
class FabricWorkspaceParams:
    """Fabric workspace parameters."""

    name: str
    description: str
    icon_path: str
    dataset_storage_mode: int
    template: FabricWorkspaceTemplateParams
    capacity: FabricCapacityParams
    rbac: RbacParams
    model: list[ModelParams]
    shortcut_auth_z_role_name: str
    skip_deploy: bool
    spark: FabricSparkParams
    shortcut: ShortcutParams | None = None

    def get_icon_payload(self, root_folder: str) -> str:
        """
        Get the base64 encoded icon payload for the workspace.

        Args:
            root_folder: The root folder path to resolve the icon path

        Returns:
            str: The base64 encoded icon payload in the format 'data:image/png;base64,{encoded}'

        Raises:
            FileNotFoundError: If the icon file doesn't exist
            Exception: If the file cannot be read or encoded
        """
        icon_full_path = os.path.join(root_folder, self.icon_path)

        if not os.path.exists(icon_full_path):
            raise FileNotFoundError(f"Icon file not found: {icon_full_path}")

        try:
            MAX_SIZE = 45 * 1024

            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as temp_file:
                temp_path = temp_file.name

            try:
                img = Image.open(icon_full_path)
                q, scale = 90, 1.0

                while True:
                    if scale < 1.0:
                        tmp = img.resize((int(img.width * scale), int(img.height * scale)), Image.LANCZOS)
                    else:
                        tmp = img
                    tmp.save(temp_path, optimize=True, quality=q)
                    if os.path.getsize(temp_path) <= MAX_SIZE or (scale <= 0.5 and q <= 30):
                        break

                    if scale > 0.5:
                        scale -= 0.1
                    else:
                        scale = max(0.1, scale)
                        q = max(10, q - 5)

                with open(temp_path, "rb") as f:
                    data = f.read()

                encoded = base64.b64encode(data).decode("utf-8")
                return f"data:image/png;base64,{encoded}"

            finally:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)

        except Exception as e:
            raise Exception(f"Failed to read, compress and encode icon file '{icon_full_path}': {e}") from e


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
    identities: list[Identity]


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
    async def batch_create_kusto_database_shortcut(self, workspace_id: str, database_id: str, shortcut_params: "ShortcutParams") -> None:
        """
        Batch create KQL database shortcuts using MWC token.

        Args:
            workspace_id: The workspace ID containing the database
            database_id: The KQL database ID
            shortcut_params: Parameters for the shortcuts to create
        """
        pass


class SparkManager(ABC):
    """
    Interface for managing Fabric Spark operations.
    """

    def __init__(self, common_params: "CommonParams"):
        """
        Initialize the Spark manager with common parameters.
        """
        self.common_params = common_params

    @abstractmethod
    async def execute(self) -> None:
        """
        Execute reconciliation for all workspaces in parallel.
        """
        pass

    @abstractmethod
    async def reconcile(self, workspace_id: str, capacity_id: str, spark_params: "FabricSparkParams") -> None:
        """
        Reconcile a single workspace to desired state Spark configuration.

        Args:
            workspace_id: The Fabric workspace id
            capacity_id: The Fabric capacity id
            spark_params: Parameters for the fabric workspace Spark configuration
        """
        pass

    @abstractmethod
    async def get_mwc_token(self, workspace_id: str, capacity_id: str) -> MwcScopedToken:
        """
        Get MWC scoped token for Spark operations.

        Args:
            workspace_id: The workspace ID
            capacity_id: The capacity ID

        Returns:
            MwcScopedToken: MWC scoped token information
        """
        pass

    @abstractmethod
    async def create_spark_pools(self, workspace_id: str, capacity_id: str, spark_params: "FabricSparkParams", mwc_token: str) -> None:
        """
        Create Spark pools using MWC token.

        Args:
            workspace_id: The workspace ID
            capacity_id: The capacity ID
            spark_params: Parameters for the Spark pools to create
            mwc_token: MWC authentication token
        """
        pass


class ModelManager(ABC):
    """
    Interface for managing Fabric Model operations.
    """

    def __init__(self, common_params: "CommonParams"):
        """
        Initialize the Model manager with common parameters.
        """
        self.common_params = common_params

    @abstractmethod
    async def execute(self) -> None:
        """
        Execute reconciliation for all workspaces in parallel.
        """
        pass

    @abstractmethod
    async def reconcile(self, workspace_id: str, model_params: "ModelParams") -> None:
        """
        Reconcile a single model to desired state.

        Args:
            workspace_id: The Fabric workspace id
        """
        pass

    @abstractmethod
    async def set_model(self, id: str, data: str) -> None:
        """
        Set Model properties for a given ID.

        Args:
            id: The id of the Model
            data: JSON string containing the data to update (e.g., '{"directLakeAutoSync":false}')

        Raises:
            RuntimeError: If the API call fails
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
    async def assign_workspace_storage_role(self, workspace_params: "FabricWorkspaceParams", workspace_info: "FabricWorkspaceInfo", storage_params: "FabricStorageParams") -> None:
        """
        Assign the configured role to the workspace identity for the storage account.

        Args:
            workspace_params: Parameters for the fabric workspace
            workspace_info: Information about the fabric workspace
            storage_params: Parameters for the fabric storage

        Raises:
            RuntimeError: If the role assignment fails
        """
        pass


# ---------------------------------------------------------------------------- #
# ---------------------------------------------------------------------------- #
# ---------------------------------------------------------------------------- #


class FolderClient(ABC):
    """
    Interface for managing Fabric folder operations.
    """

    def __init__(self, common_params: "CommonParams"):
        """
        Initialize the folder client with common parameters.
        """
        self.common_params = common_params

    @abstractmethod
    async def get_fabric_folder_collection(self, workspace_id: str) -> FabricFolderCollection:
        """
        Get Fabric folder information for a workspace.

        Args:
            workspace_id: The Fabric workspace id

        Returns:
            FabricFolderCollection: Fabric workspace folder information
        """
        pass


# ---------------------------------------------------------------------------- #
# ---------------------------------------------------------------------------- #
# ---------------------------------------------------------------------------- #


class ArtifactClient(ABC):
    """
    Interface for managing Fabric artifact operations.
    """

    def __init__(self, common_params: "CommonParams"):
        """
        Initialize the artifact client with common parameters.
        """
        self.common_params = common_params

    @abstractmethod
    async def post_definition(
        self,
        workspace_id: str,
        artifact_request: "ArtifactRequest",
    ) -> "FabricArtifact":
        """
        Create a new Fabric artifact definition.

        Args:
            workspace_id: The Fabric workspace ID
            artifact_request: The artifact creation request payload

        Returns:
            FabricArtifact: The created artifact information
        """
        pass


# ---------------------------------------------------------------------------- #
# ---------------------------------------------------------------------------- #
# ---------------------------------------------------------------------------- #


class SparkJobDefinitionClient(ABC):
    """
    Interface for managing Spark Job Definition operations.
    """

    def __init__(self, common_params: "CommonParams"):
        """
        Initialize the Spark Job Definition client with common parameters.

        Args:
            common_params: Common configuration parameters
        """
        self.common_params = common_params

    @abstractmethod
    async def create_spark_job_definition_artifact(
        self,
        workspace_id: str,
        spark_job_definition: "SparkJobDefinition",
    ) -> "FabricArtifact":
        """
        Create or retrieve a Spark Job Definition artifact.

        This method checks if a SparkJobDefinition with the given display name already exists.
        If it exists, returns the existing artifact. Otherwise, creates a new one.

        Args:
            workspace_id: The Fabric workspace id
            spark_job_definition: The Spark Job Definition configuration

        Returns:
            FabricArtifact: The existing or newly created artifact information

        Raises:
            RuntimeError: If the API calls fail
            ValueError: If nested_folder_path is empty or invalid
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
            self.config_data = self._load_and_process_config(config_file_absolute_path)
            self.operation = Operation(operation)
            self.common = self._parse_common_params(self.config_data["common"])

        except FileNotFoundError:
            self.logger.error(f"Configuration file not found: {config_file_absolute_path}")
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
        Gets the user's alias from environment variable or git config.

        First attempts to get from USER_DISPLAY_NAME environment variable,
        then falls back to git config user.email if not found.

        Returns:
            str: The user's alias (cleaned from display name or email)
        """
        user_alias = os.getenv("USER_DISPLAY_NAME", "")
        if user_alias:
            cleaned_alias = re.sub(r"[^a-zA-Z0-9]", "", user_alias.lower())
            if cleaned_alias:
                return cleaned_alias

        return self._get_user_alias_from_email()

    # ---------------------------------------------------------------------------- #

    @functools.cache  # noqa: B019
    def _get_user_alias_from_email(self) -> str:
        """
        Gets the user's alias from email address.

        Returns:
            str: The user's alias (part before @ in their email)

        Raises:
            RuntimeError: If no user email is found or email format is invalid
        """
        err_str = "No user email found in upn"
        user_email = self.az_cli.get_user_principal_name()
        email_parts = user_email.split("@")
        if len(email_parts) != 2:  # noqa: PLR2004
            self.logger.error(f"Invalid email format: {user_email}")
            raise RuntimeError(err_str)

        alias = email_parts[0].strip()
        if not alias:
            self.logger.error(f"Empty alias in email: {user_email}")
            raise RuntimeError(err_str)

        return alias

    @functools.cache  # noqa: B019
    def _get_git_root(self) -> str:
        """
        Gets the git root directory.

        Returns:
            str: The git root.

        Raises:
            RuntimeError: If no git root is found
        """
        err_str = "No git root found. Please ensure you are in a git repository."

        git_root = os.getenv("GIT_ROOT")
        if git_root:
            return git_root

        try:
            result = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True, timeout=10)  # fmt: skip # noqa: E501, S603, S607
            return result.stdout.strip()
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            self.logger.error(f"Failed to get git root: {e}")
            raise RuntimeError(err_str) from e

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
            "unique-env-id": self._get_unique_env_id,
            "user-appid": self._get_user_appid,
            "user-display-name": self._get_user_display_name,
            "user-oid": self._get_user_oid,
            "user-principal-type": self._get_user_principal_type,
            "user-fabric-admin-func()": self._user_fabric_admin_func,
            "git-root": self._get_git_root,
        }

    def _get_unique_env_id(self) -> str:
        return os.getenv("UNIQUE_ENV_ID", self.get_user_alias())

    def _get_user_appid(self) -> str:
        return os.getenv("USER_APP_ID", self.az_cli.get_user_appid())

    def _get_user_display_name(self) -> str:
        return os.getenv("USER_DISPLAY_NAME", self.az_cli.get_user_principal_name())

    def _get_user_oid(self) -> str:
        return os.getenv("USER_OBJECT_ID", self.az_cli.get_user_oid())

    def _get_user_principal_type(self) -> str:
        return os.getenv("USER_PRINCIPAL_TYPE", "User")

    def _user_fabric_admin_func(self) -> str:
        user_principal_type = self._get_user_principal_type()
        if user_principal_type == "ServicePrincipal":
            return self._get_user_oid()
        else:
            return self._get_user_display_name()

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
                    result = result.replace(placeholder_pattern, replacement_value)
                    self.logger.debug(f"Replaced {placeholder_pattern} with {replacement_value}")

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
            self.logger.error(f"operation cannot be empty or left as default: {self.operation}")
            return False
        return True

    def _validate_common_params(self) -> bool:
        """Validate common parameters."""
        return self._validate_local_params() and self._validate_endpoint_params() and self._validate_scope_params() and self._validate_arm_params() and self._validate_identities_params() and self._validate_fabric_params()

    def _validate_local_params(self) -> bool:
        """Validate local parameters."""
        if not self.common.local.root_folder or self._is_folder_empty(self.common.local.root_folder):
            self.logger.error(f"rootFolder cannot be empty: {self.common.local.root_folder}")
            return False
        return True

    def _validate_endpoint_params(self) -> bool:
        """Validate endpoint parameters."""
        if not self.common.endpoint.analysis_service:
            self.logger.error(f"analysisService endpoint cannot be empty: {self.common.endpoint.analysis_service}")
            return False
        if not self.common.endpoint.cicd:
            self.logger.error(f"cicd endpoint cannot be empty: {self.common.endpoint.cicd}")
            return False
        if not self.common.endpoint.power_bi:
            self.logger.error(f"powerBi endpoint cannot be empty: {self.common.endpoint.power_bi}")
            return False
        if not self.common.endpoint.analysis_service.startswith("https://"):
            self.logger.error(f"analysisService endpoint must be a valid HTTPS URL: {self.common.endpoint.analysis_service}")
            return False
        if not self.common.endpoint.cicd.startswith("https://"):
            self.logger.error(f"cicd endpoint must be a valid HTTPS URL: {self.common.endpoint.cicd}")
            return False
        if not self.common.endpoint.power_bi.startswith("https://"):
            self.logger.error(f"powerBi endpoint must be a valid HTTPS URL: {self.common.endpoint.power_bi}")
            return False

        return True

    def _validate_scope_params(self) -> bool:
        """Validate scope parameters."""
        if not self.common.scope.analysis_service:
            self.logger.error(f"analysisService scope cannot be empty: {self.common.scope.analysis_service}")
            return False
        if not self.common.scope.analysis_service.startswith("https://"):
            self.logger.error(f"analysisService scope must be a valid HTTPS URL: {self.common.scope.analysis_service}")
            return False

        return True

    def _validate_arm_params(self) -> bool:
        """Validate ARM parameters."""
        if not self.common.arm.resource_group or not self.common.arm.location or not self.common.arm.subscription_id or not self.common.arm.tenant_id:
            self.logger.error(f"resourceGroup: {self.common.arm.resource_group}, " f"location: {self.common.arm.location}, subscriptionId: {self.common.arm.subscription_id}, " f"or tenantId: {self.common.arm.tenant_id} cannot be empty")
            return False
        return True

    def _validate_identities_params(self) -> bool:
        """Validate identities parameters."""
        if self.common.identities is None:
            self.logger.error("Common identities cannot be None")
            return False

        if len(self.common.identities) == 0:
            self.logger.error("At least one identity must be configured")
            return False

        for j, identity in enumerate(self.common.identities):
            if not self._validate_identity_params(identity, -1, j):
                return False

        return True

    def _validate_fabric_params(self) -> bool:
        """Validate Fabric parameters."""
        if not self.common.fabric.workspaces or len(self.common.fabric.workspaces) == 0:
            self.logger.error("At least one Fabric workspace must be configured")
            return False

        for i, workspace in enumerate(self.common.fabric.workspaces):
            if not workspace.name:
                self.logger.error(f"Workspace name at index {i} cannot be empty")
                return False

            if not workspace.description:
                self.logger.error(f"Workspace description at index {i} cannot be empty")
                return False

            if not workspace.dataset_storage_mode:
                self.logger.error(f"Workspace dataset_storage_mode at index {i} cannot be empty")
                return False

            if not workspace.template.artifacts_folder:
                self.logger.error(f"Workspace template artifacts_folder at index {i} cannot be empty")
                return False

            if not workspace.template.parameter_file_path:
                self.logger.error(f"Workspace template parameter_file_path at index {i} cannot be empty")
                return False

            if not workspace.template.item_types_in_scope or len(workspace.template.item_types_in_scope) < 1:
                self.logger.error(f"Workspace template item_types_in_scope at index {i} cannot be empty")
                return False

            if not workspace.template.environment_key:
                self.logger.error(f"Workspace template environment_key at index {i} cannot be empty")
                return False

            if not workspace.template.feature_flags:
                self.logger.error(f"Workspace template feature_flags at index {i} cannot be empty")
                return False

            if not workspace.template.unpublish_orphans:
                self.logger.error(f"Workspace template unpublish_orphans at index {i} cannot be empty")
                return False

            if self._is_yaml_file_invalid(str(Path(self.common.local.root_folder) / workspace.template.parameter_file_path)):
                self.logger.error(f"Workspace template parameter_file_path at index {i} is invalid: {workspace.template.parameter_file_path}")  # fmt: skip  # noqa: E501
                return False

            if not workspace.icon_path:
                self.logger.error(f"Workspace icon_path at index {i} cannot be empty")
                return False

            icon_full_path = os.path.join(self.common.local.root_folder, workspace.icon_path)
            if not os.path.exists(icon_full_path):
                self.logger.error(f"Icon file not found for workspace at index {i}: {icon_full_path}")
                return False

            if not workspace.capacity.name:
                self.logger.error(f"Workspace capacity name at index {i} cannot be empty")
                return False

            if not workspace.capacity.name[0].islower() or not workspace.capacity.name.isalnum() or not workspace.capacity.name.islower():
                self.logger.error(f"Workspace capacity name at index {i} must start with a lowercase letter and contain only lowercase letters and digits (no spaces, underscores, or dashes): {workspace.capacity.name}")  # noqa: E501
                return False

            if not workspace.capacity.sku:
                self.logger.error(f"Workspace capacity sku at index {i} cannot be empty")
                return False

            if not workspace.capacity.administrators or not len(workspace.capacity.administrators) >= 1:
                self.logger.error(f"Workspace capacity administrators at index {i} must contain at least one administrator")
                return False

            if not workspace.shortcut_auth_z_role_name:
                self.logger.error(f"Workspace shortcutAuthZRoleName at index {i} cannot be empty")
                return False

            if workspace.shortcut_auth_z_role_name not in ALLOWED_STORAGE_ROLES:
                self.logger.error(f"Workspace shortcutAuthZRoleName at index {i} must be one of {ALLOWED_STORAGE_ROLES}, " f"got '{workspace.shortcut_auth_z_role_name}'")
                return False

            if workspace.skip_deploy is None:
                self.logger.error(f"Workspace skipDeploy at index {i} cannot be None")
                return False

            if not self._validate_model_params(workspace.model, i):
                return False

            if not self._validate_spark_job_definition_params(workspace.template.spark_job_definitions, i):
                return False

            if workspace.spark is None:
                self.logger.error(f"Workspace spark at index {i} cannot be None")
                return False

            if not self._validate_spark_params(workspace.spark, i):
                return False

            if workspace.shortcut is not None and not self._validate_shortcut_params(workspace.shortcut, i):
                return False

            if workspace.rbac is not None and not self._validate_rbac_params(workspace.rbac, i, self.common.identities):
                return False

        return self._validate_fabric_storage_params()

    def _validate_fabric_storage_params(self) -> bool:
        """Validate Fabric Storage parameters."""
        storage = self.common.fabric.storage
        if not storage.account or not storage.container or not storage.location or not storage.resource_group or not storage.subscription_id or not storage.tenant_id or not storage.shortcut_data_connection_id:
            self.logger.error(f"FabricStorageParams members cannot be empty: account={storage.account}, " f"container={storage.container}, location={storage.location}, " f"resourceGroup={storage.resource_group}, subscriptionId={storage.subscription_id}, " f"tenantId={storage.tenant_id}")
            return False
        return True

    def _validate_model_params(self, models: list[ModelParams], workspace_index: int) -> bool:
        """Validate model parameters."""
        if models is None:
            self.logger.error(f"Workspace model at index {workspace_index} cannot be None")
            return False

        if len(models) == 0:
            return True

        for j, model in enumerate(models):
            if not model.display_name:
                self.logger.error(f"Workspace model[{j}].displayName at index {workspace_index} cannot be empty")
                return False

            if model.direct_lake_auto_sync is None:
                self.logger.error(f"Workspace model[{j}].directLakeAutoSync at index {workspace_index} cannot be None")
                return False

        return True

    def _validate_spark_job_definition_params(self, spark_job_definitions: list[SparkJobDefinition], workspace_index: int) -> bool:
        """Validate Spark Job Definition parameters."""
        if spark_job_definitions is None:
            self.logger.error(f"Workspace spark_job_definitions at index {workspace_index} cannot be None")
            return False

        if len(spark_job_definitions) == 0:
            return True

        seen_display_names = set()
        seen_nested_paths = set()
        for j, sjd in enumerate(spark_job_definitions):
            if not sjd.display_name:
                self.logger.error(f"Spark Job Definition display_name at workspace index {workspace_index}, definition index {j} cannot be empty")
                return False

            if sjd.display_name in seen_display_names:
                self.logger.error(f"Duplicate Spark Job Definition display_name '{sjd.display_name}' at workspace index {workspace_index}, definition index {j}")
                return False
            seen_display_names.add(sjd.display_name)

            if not sjd.description:
                self.logger.error(f"Spark Job Definition description at workspace index {workspace_index}, definition index {j} cannot be empty")
                return False

            if not sjd.default_lakehouse_artifact_name:
                self.logger.error(f"Spark Job Definition default_lakehouse_artifact_name at workspace index {workspace_index}, definition index {j} cannot be empty")
                return False

            if not sjd.nested_folder_path:
                self.logger.error(f"Spark Job Definition nested_folder_path at workspace index {workspace_index}, definition index {j} cannot be empty")
                return False

            spark_job_def_v1_file = os.path.join(self.common.local.root_folder, sjd.nested_folder_path, SPARK_JOB_DEFINITION_V1_FILE)
            if not os.path.exists(spark_job_def_v1_file):
                self.logger.error(f"Spark Job Definition file '{SPARK_JOB_DEFINITION_V1_FILE}' not found at workspace index {workspace_index}, definition index {j}: {spark_job_def_v1_file}")
                return False

            if sjd.nested_folder_path in seen_nested_paths:
                self.logger.error(f"Duplicate Spark Job Definition nested_folder_path '{sjd.nested_folder_path}' at workspace index {workspace_index}, definition index {j}")
                return False
            seen_nested_paths.add(sjd.nested_folder_path)

        return True

    def _validate_spark_params(self, spark: FabricSparkParams, workspace_index: int) -> bool:
        """Validate Spark parameters."""
        if spark.pools is None:
            self.logger.error(f"Workspace spark pools at index {workspace_index} cannot be None")
            return False

        if len(spark.pools) == 0:
            return True

        pool_ids = set()
        for j, pool in enumerate(spark.pools):
            if pool.enable_customized_compute_conf is None:
                self.logger.error(f"Workspace spark pools[{j}].enableCustomizedComputeConf at index {workspace_index} cannot be None")
                return False

            if pool.machine_learning_auto_log is None or pool.machine_learning_auto_log.enabled is None:
                self.logger.error(f"Workspace spark pools[{j}].machineLearningAutoLog.enabled at index {workspace_index} cannot be None")
                return False

            if pool.job_management is None:
                self.logger.error(f"Workspace spark pools[{j}].jobManagement at index {workspace_index} cannot be None")
                return False

            if pool.job_management.conservative_job_admission_enabled is None:
                self.logger.error(f"Workspace spark pools[{j}].jobManagement.conservativeJobAdmissionEnabled at index {workspace_index} cannot be None")
                return False

            if pool.job_management.session_timeout_in_minutes is None or pool.job_management.session_timeout_in_minutes <= 0:
                self.logger.error(f"Workspace spark pools[{j}].jobManagement.sessionTimeoutInMinutes at index {workspace_index} must be greater than 0")
                return False

            if pool.high_concurrency is None:
                self.logger.error(f"Workspace spark pools[{j}].highConcurrency at index {workspace_index} cannot be None")
                return False

            if pool.high_concurrency.enabled is None:
                self.logger.error(f"Workspace spark pools[{j}].highConcurrency.enabled at index {workspace_index} cannot be None")
                return False

            if pool.high_concurrency.notebook_pipeline_run_enabled is None:
                self.logger.error(f"Workspace spark pools[{j}].highConcurrency.notebookPipelineRunEnabled at index {workspace_index} cannot be None")
                return False

            if pool.details is None:
                self.logger.error(f"Workspace spark pools[{j}].details at index {workspace_index} cannot be None")
                return False

            if not pool.details.id:
                self.logger.error(f"Workspace spark pools[{j}].details.id at index {workspace_index} cannot be empty")
                return False

            try:
                import uuid

                uuid.UUID(pool.details.id)
            except ValueError:
                self.logger.error(f"Workspace spark pools[{j}].details.id at index {workspace_index} must be a valid GUID: {pool.details.id}")
                return False

            if pool.details.id in pool_ids:
                self.logger.error(f"Workspace spark pools[{j}].details.id at index {workspace_index} is not unique: {pool.details.id}")
                return False
            pool_ids.add(pool.details.id)

            if not pool.details.name:
                self.logger.error(f"Workspace spark pools[{j}].details.name at index {workspace_index} cannot be empty")
                return False

            if pool.details.node_size_family is None:
                self.logger.error(f"Workspace spark pools[{j}].details.nodeSizeFamily at index {workspace_index} cannot be None")
                return False

            if pool.details.node_size is None:
                self.logger.error(f"Workspace spark pools[{j}].details.nodeSize at index {workspace_index} cannot be None")
                return False

            if pool.details.auto_scale is None:
                self.logger.error(f"Workspace spark pools[{j}].details.autoScale at index {workspace_index} cannot be None")
                return False

            if pool.details.auto_scale.enabled is None:
                self.logger.error(f"Workspace spark pools[{j}].details.autoScale.enabled at index {workspace_index} cannot be None")
                return False

            if pool.details.auto_scale.min_node_count is None or pool.details.auto_scale.min_node_count <= 0:
                self.logger.error(f"Workspace spark pools[{j}].details.autoScale.minNodeCount at index {workspace_index} must be greater than 0")
                return False

            if pool.details.auto_scale.max_node_count is None or pool.details.auto_scale.max_node_count <= 0:
                self.logger.error(f"Workspace spark pools[{j}].details.autoScale.maxNodeCount at index {workspace_index} must be greater than 0")
                return False

            if pool.details.auto_scale.min_node_count > pool.details.auto_scale.max_node_count:
                self.logger.error(f"Workspace spark pools[{j}].details.autoScale.minNodeCount at index {workspace_index} cannot be greater than maxNodeCount")
                return False

            if pool.details.dynamic_executor_allocation is None:
                self.logger.error(f"Workspace spark pools[{j}].details.dynamicExecutorAllocation at index {workspace_index} cannot be None")
                return False

            if pool.details.dynamic_executor_allocation.enabled is None:
                self.logger.error(f"Workspace spark pools[{j}].details.dynamicExecutorAllocation.enabled at index {workspace_index} cannot be None")
                return False

            if pool.details.dynamic_executor_allocation.min_executors is None or pool.details.dynamic_executor_allocation.min_executors <= 0:
                self.logger.error(f"Workspace spark pools[{j}].details.dynamicExecutorAllocation.minExecutors at index {workspace_index} must be greater than 0")
                return False

            if pool.details.dynamic_executor_allocation.max_executors is None or pool.details.dynamic_executor_allocation.max_executors <= 0:
                self.logger.error(f"Workspace spark pools[{j}].details.dynamicExecutorAllocation.maxExecutors at index {workspace_index} must be greater than 0")
                return False

            if pool.details.dynamic_executor_allocation.min_executors > pool.details.dynamic_executor_allocation.max_executors:
                self.logger.error(f"Workspace spark pools[{j}].details.dynamicExecutorAllocation.minExecutors at index {workspace_index} cannot be greater than maxExecutors")
                return False

            if pool.runtime_version is None:
                self.logger.error(f"Workspace spark pools[{j}].runtimeVersion at index {workspace_index} cannot be None")
                return False

        return True

    def _validate_shortcut_params(self, shortcut: ShortcutParams, workspace_index: int) -> bool:
        """Validate shortcut parameters."""
        if shortcut.kql_database is None:
            self.logger.error(f"Workspace shortcut kqlDatabase at index {workspace_index} cannot be None")
            return False

        for j, kql_db in enumerate(shortcut.kql_database):
            if not kql_db.database:
                self.logger.error(f"Workspace shortcut kqlDatabase[{j}].database at index {workspace_index} cannot be empty")
                return False

            if not kql_db.table:
                self.logger.error(f"Workspace shortcut kqlDatabase[{j}].table at index {workspace_index} cannot be empty")
                return False

            if not kql_db.path:
                self.logger.error(f"Workspace shortcut kqlDatabase[{j}].path at index {workspace_index} cannot be empty")
                return False

            if kql_db.query_acceleration_toggle is None:
                self.logger.error(f"Workspace shortcut kqlDatabase[{j}].queryAccelerationToggle at index {workspace_index} cannot be None")
                return False

        return True

    def _validate_rbac_params(self, rbac: RbacParams, workspace_index: int, identities: list[Identity]) -> bool:
        """Validate RBAC parameters."""
        if rbac.purge_unmatched_role_assignments is None:
            self.logger.error(f"Workspace rbac purgeUnmatchedRoleAssignments at index {workspace_index} cannot be None")
            return False

        if identities is None:
            self.logger.error(f"Common identities cannot be None")
            return False

        identity_object_ids = {identity.object_id for identity in identities}

        for j, identity in enumerate(identities):
            if not self._validate_identity_params(identity, workspace_index, j):
                return False

        if not self._validate_universal_security_params(rbac.universal_security, workspace_index):
            return False

        if rbac.workspace is None:
            self.logger.error(f"Workspace rbac workspace at index {workspace_index} cannot be None")
            return False

        for j, workspace_rbac in enumerate(rbac.workspace):
            if not self._validate_workspace_rbac_params(workspace_rbac, workspace_index, j, identity_object_ids):
                return False

        return all(self._validate_item_rbac_params(item_rbac, workspace_index, k, identity_object_ids) for k, item_rbac in enumerate(rbac.items))

    def _validate_universal_security_params(self, universal_security: UniversalSecurity, workspace_index: int) -> bool:
        """Validate universalSecurity settings parsed from configuration."""
        if universal_security is None:
            self.logger.error(f"Workspace rbac universalSecurity block at index {workspace_index} is required and cannot be None")
            return False

        sql_endpoint = universal_security.sql_endpoint
        if sql_endpoint is None:
            self.logger.error(f"Workspace rbac universalSecurity.sqlEndpoint at index {workspace_index} is required and cannot be None")
            return False

        if not isinstance(sql_endpoint.enabled, bool):
            self.logger.error(f"Workspace rbac universalSecurity.sqlEndpoint.enabled at index {workspace_index} must be a boolean (true/false)")
            return False

        if not isinstance(sql_endpoint.skip_reconcile, list):
            self.logger.error(f"Workspace rbac universalSecurity.sqlEndpoint.skipReconcile at index {workspace_index} must be a list of strings")
            return False

        for i, item in enumerate(sql_endpoint.skip_reconcile):
            if not isinstance(item, str):
                self.logger.error(f"Workspace rbac universalSecurity.sqlEndpoint.skipReconcile[{i}] at index {workspace_index} must be a string")
                return False

        return True

    def _validate_identity_params(self, identity: Identity, workspace_index: int, identity_index: int) -> bool:
        """Validate identity parameters."""
        if not identity.given_name:
            self.logger.error(f"Workspace rbac identities[{identity_index}].givenName at index {workspace_index} cannot be empty")
            return False

        if not identity.object_id:
            self.logger.error(f"Workspace rbac identities[{identity_index}].objectId at index {workspace_index} cannot be empty")
            return False

        if identity.principal_type == PrincipalType.SERVICE_PRINCIPAL and not identity.aad_app_id:
            self.logger.error(f"Workspace rbac identities[{identity_index}].aadAppId at index {workspace_index} cannot be empty for service principals")  # noqa: E501
            return False

        return True

    def _validate_workspace_rbac_params(self, workspace_rbac: WorkspaceRbacParams, workspace_index: int, rbac_index: int, identity_object_ids: set[str]) -> bool:
        """Validate workspace RBAC parameters."""
        if workspace_rbac.permissions is None:
            self.logger.error(f"Workspace rbac workspace[{rbac_index}].permissions at index {workspace_index} cannot be None")
            return False

        if not workspace_rbac.object_id:
            self.logger.error(f"Workspace rbac workspace[{rbac_index}].objectId at index {workspace_index} cannot be empty")
            return False

        if workspace_rbac.object_id not in identity_object_ids:
            self.logger.error(f"Workspace rbac workspace[{rbac_index}].objectId '{workspace_rbac.object_id}' at index {workspace_index} not found in identities")  # noqa: E501
            return False

        if not workspace_rbac.purpose:
            self.logger.error(f"Workspace rbac workspace[{rbac_index}].purpose at index {workspace_index} cannot be empty")
            return False

        return True

    def _validate_item_rbac_params(self, item_rbac: ItemRbacParams, workspace_index: int, item_index: int, identity_object_ids: set[str]) -> bool:
        """Validate item RBAC parameters."""
        if not item_rbac.type:
            self.logger.error(f"Workspace rbac items[{item_index}].type at index {workspace_index} cannot be empty")
            return False

        if not item_rbac.display_name:
            self.logger.error(f"Workspace rbac items[{item_index}].displayName at index {workspace_index} cannot be empty")
            return False

        if item_rbac.detail is None:
            self.logger.error(f"Workspace rbac items[{item_index}].detail at index {workspace_index} cannot be None")
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
            self.logger.error(f"Workspace rbac items[{item_index}].detail[{detail_index}].permissions at index {workspace_index} cannot be None")
            return False

        if not detail_rbac.object_id:
            self.logger.error(f"Workspace rbac items[{item_index}].detail[{detail_index}].objectId at index {workspace_index} cannot be empty")
            return False

        if detail_rbac.object_id not in identity_object_ids:
            self.logger.error(f"Workspace rbac items[{item_index}].detail[{detail_index}].objectId '{detail_rbac.object_id}' at index {workspace_index} not found in identities")  # noqa: E501
            return False

        if not detail_rbac.purpose:
            self.logger.error(f"Workspace rbac items[{item_index}].detail[{detail_index}].purpose at index {workspace_index} cannot be empty")
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
            self.logger.error(f"Failed to JSON parse {file_path} with error: {e}")
            return True

    def _is_yaml_file_invalid(self, file_path: str) -> bool:
        """Check if a YAML file is invalid."""
        try:
            with open(file_path, encoding="utf-8") as f:
                yaml.safe_load(f)
            return False
        except (OSError, yaml.YAMLError) as e:
            self.logger.error(f"Failed to YAML parse {file_path} with error: {e}")
            return True

    # ---------------------------------------------------------------------------- #

    def _parse_common_params(self, data: dict[str, Any]) -> CommonParams:
        """Parse common parameters."""
        root_folder = data["local"]["rootFolder"]
        return CommonParams(
            local=self._parse_local_params(data["local"]),
            endpoint=self._parse_endpoint_params(data["endpoint"]),
            scope=self._parse_scope_params(data["scope"]),
            arm=self._parse_arm_params(data["arm"]),
            fabric=self._parse_fabric_params(data["fabric"], root_folder),
            identities=self._parse_identities_params(data.get("identities", [])),
        )

    def _parse_identities_params(self, data: list[dict[str, Any]]) -> list[Identity]:
        """Parse identities parameters."""
        identities = []
        for identity_data in data:
            identities.append(self._parse_identity_params(identity_data))
        return self._deduplicate_list(identities)

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

    def _parse_fabric_params(self, data: dict[str, Any], root_folder: str) -> FabricParams:
        """Parse Fabric parameters."""
        return FabricParams(
            workspaces=self._parse_fabric_workspaces(data["workspaces"], root_folder),
            storage=self._parse_fabric_storage_params(data["storage"]),
        )

    def _parse_fabric_workspaces(self, data: list[dict[str, Any]], root_folder: str) -> list[FabricWorkspaceParams]:
        """Parse Fabric workspaces."""
        workspaces = []
        for workspace_data in data:
            workspaces.append(self._parse_fabric_workspace_params(workspace_data, root_folder))
        return workspaces

    def _parse_fabric_workspace_params(self, data: dict[str, Any], root_folder: str) -> FabricWorkspaceParams:
        """Parse a single Fabric workspace."""
        shortcut = None
        if "shortcut" in data:
            shortcut = self._parse_shortcut_params(data["shortcut"])

        return FabricWorkspaceParams(
            name=data["name"],
            description=data["description"],
            icon_path=data["iconPath"],
            dataset_storage_mode=data["datasetStorageMode"],
            template=self._parse_fabric_workspace_template_params(data["template"], root_folder),
            capacity=self._parse_fabric_capacity_params(data["capacity"]),
            shortcut=shortcut,
            rbac=self._parse_rbac_params(data["rbac"]),
            model=self._parse_model_params(data["model"]),
            shortcut_auth_z_role_name=data["shortcutAuthZRoleName"],
            skip_deploy=data["skipDeploy"],
            spark=self._parse_spark_params(data["spark"]),
        )

    def _parse_fabric_workspace_template_params(self, data: dict[str, Any], root_folder: str) -> FabricWorkspaceTemplateParams:
        """Parse Fabric workspace template parameters."""
        if "customLibraries" not in data:
            msg = "Missing required field 'customLibraries' in template configuration"
            raise KeyError(msg)

        if "sparkJobDefinitions" not in data:
            msg = "Missing required field 'sparkJobDefinitions' in template configuration"
            raise KeyError(msg)

        custom_libraries = []
        for lib_data in data["customLibraries"]:
            custom_libraries.append(self._parse_custom_library(lib_data))

        spark_job_definitions = []
        for sjd_data in data["sparkJobDefinitions"]:
            spark_job_definitions.append(self._parse_spark_job_definition(sjd_data, root_folder))

        return FabricWorkspaceTemplateParams(
            artifacts_folder=data["artifactsFolder"],
            parameter_file_path=data["parameterFilePath"],
            item_types_in_scope=data["itemTypesInScope"],
            environment_key=data["environmentKey"],
            feature_flags=data["featureFlags"],
            unpublish_orphans=data["unpublishOrphans"],
            custom_libraries=custom_libraries,
            spark_job_definitions=spark_job_definitions,
        )

    def _parse_custom_library(self, data: dict[str, Any]) -> CustomLibrary:
        """Parse custom library configuration."""
        source = CustomLibrarySource(
            folder_path=data["source"]["folderPath"],
            prefix=data["source"]["prefix"],
            suffix=data["source"]["suffix"],
            error_on_multiple=data["source"]["errorOnMultiple"],
        )
        dest = CustomLibraryDest(
            folder_path=data["dest"]["folderPath"],
        )
        return CustomLibrary(source=source, dest=dest)

    def _parse_spark_job_definition(self, data: dict[str, Any], root_folder: str) -> SparkJobDefinition:
        """Parse Spark Job Definition configuration by reading from .platform and SparkJobDefinitionV1.json files."""
        nested_folder_path = data["nestedFolderPath"]

        platform_file_path = os.path.join(root_folder, nested_folder_path, SPARK_JOB_DEFINITION_PLATFORM_FILE)
        if not os.path.exists(platform_file_path):
            msg = f"Required file '{SPARK_JOB_DEFINITION_PLATFORM_FILE}' not found in path: {platform_file_path}"
            self.logger.error(msg)
            raise FileNotFoundError(msg)
        try:
            with open(platform_file_path, encoding="utf-8") as f:
                platform_data = json.load(f)
        except json.JSONDecodeError as e:
            msg = f"Invalid JSON in {SPARK_JOB_DEFINITION_PLATFORM_FILE} file at {platform_file_path}: {e}"
            self.logger.error(msg)
            raise ValueError(msg) from e
        except Exception as e:
            msg = f"Failed to read {SPARK_JOB_DEFINITION_PLATFORM_FILE} file at {platform_file_path}: {e}"
            self.logger.error(msg)
            raise RuntimeError(msg) from e
        if "metadata" not in platform_data:
            msg = f"Missing required field 'metadata' in {SPARK_JOB_DEFINITION_PLATFORM_FILE} at {platform_file_path}"
            self.logger.error(msg)
            raise KeyError(msg)

        metadata = platform_data["metadata"]
        if "displayName" not in metadata:
            msg = f"Missing required field 'metadata.displayName' in {SPARK_JOB_DEFINITION_PLATFORM_FILE} at {platform_file_path}"
            self.logger.error(msg)
            raise KeyError(msg)
        if "description" not in metadata:
            msg = f"Missing required field 'metadata.description' in {SPARK_JOB_DEFINITION_PLATFORM_FILE} at {platform_file_path}"
            self.logger.error(msg)
            raise KeyError(msg)
        if "type" not in metadata:
            msg = f"Missing required field 'metadata.type' in {SPARK_JOB_DEFINITION_PLATFORM_FILE} at {platform_file_path}"
            self.logger.error(msg)
            raise KeyError(msg)

        artifact_type = metadata["type"]
        if artifact_type != ArtifactType.SPARK_JOB_DEFINITION.value:
            msg = f"Invalid artifact type '{artifact_type}' in {SPARK_JOB_DEFINITION_PLATFORM_FILE} at {platform_file_path}. " f"Expected '{ArtifactType.SPARK_JOB_DEFINITION.value}'"
            self.logger.error(msg)
            raise ValueError(msg)

        v1_file_path = os.path.join(root_folder, nested_folder_path, SPARK_JOB_DEFINITION_V1_FILE)
        if not os.path.exists(v1_file_path):
            msg = f"Required file '{SPARK_JOB_DEFINITION_V1_FILE}' not found in path: {v1_file_path}"
            self.logger.error(msg)
            raise FileNotFoundError(msg)
        try:
            with open(v1_file_path, encoding="utf-8") as f:
                v1_data = json.load(f)
        except json.JSONDecodeError as e:
            msg = f"Invalid JSON in {SPARK_JOB_DEFINITION_V1_FILE} file at {v1_file_path}: {e}"
            self.logger.error(msg)
            raise ValueError(msg) from e
        except Exception as e:
            msg = f"Failed to read {SPARK_JOB_DEFINITION_V1_FILE} file at {v1_file_path}: {e}"
            self.logger.error(msg)
            raise RuntimeError(msg) from e

        spark_job_definition_v1_config = self._parse_spark_job_definition_v1_config(v1_data, v1_file_path)

        return SparkJobDefinition(
            display_name=metadata["displayName"],
            description=metadata["description"],
            nested_folder_path=nested_folder_path,
            default_lakehouse_artifact_name=data["defaultLakehouseArtifactName"],
            spark_job_definition_v1_config=spark_job_definition_v1_config,
        )

    def _parse_spark_job_definition_v1_config(self, data: dict[str, Any], file_path: str) -> SparkJobDefinitionV1Config:
        """Parse SparkJobDefinitionV1Config from JSON data."""
        required_fields = [
            "executableFile",
            "defaultLakehouseArtifactId",
            "mainClass",
            "additionalLakehouseIds",
            "commandLineArguments",
            "additionalLibraryUris",
            "language",
        ]

        for field in required_fields:
            if field not in data:
                msg = f"Missing required field '{field}' in {SPARK_JOB_DEFINITION_V1_FILE} at {file_path}"
                self.logger.error(msg)
                raise KeyError(msg)

        return SparkJobDefinitionV1Config(
            executable_file=data["executableFile"],
            default_lakehouse_artifact_id=data["defaultLakehouseArtifactId"],
            main_class=data["mainClass"],
            additional_lakehouse_ids=data["additionalLakehouseIds"],
            retry_policy=data.get("retryPolicy"),
            command_line_arguments=data["commandLineArguments"],
            additional_library_uris=data["additionalLibraryUris"],
            language=data["language"],
            environment_artifact_id=data.get("environmentArtifactId"),
        )

    def _parse_fabric_capacity_params(self, data: dict[str, Any]) -> FabricCapacityParams:
        """Parse Fabric capacity parameters."""
        return FabricCapacityParams(
            administrators=list(dict.fromkeys(data["administrators"])),
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
                kql_database.append(self._parse_kql_database_shortcut(kql_db_data))

        return ShortcutParams(kql_database=kql_database)

    def _parse_kql_database_shortcut(self, data: dict[str, Any]) -> KqlDatabaseShortcut:
        """Parse KQL database shortcut parameters."""
        return KqlDatabaseShortcut(
            database=data["database"],
            table=data["table"],
            path=data["path"],
            query_acceleration_toggle=data["queryAccelerationToggle"],
        )

    def _parse_model_params(self, data: list[dict[str, Any]]) -> list[ModelParams]:
        """Parse model parameters."""
        models = []
        for model_data in data:
            models.append(
                ModelParams(
                    display_name=model_data["displayName"],
                    direct_lake_auto_sync=model_data["directLakeAutoSync"],
                )
            )
        return models

    def _parse_spark_params(self, data: dict[str, Any]) -> FabricSparkParams:
        """Parse Spark parameters."""
        pools = []
        for pool_data in data["pools"]:
            pools.append(self._parse_spark_pool(pool_data))
        return FabricSparkParams(pools=pools)

    def _parse_spark_pool(self, data: dict[str, Any]) -> SparkPool:
        """Parse a single Spark pool configuration."""
        return SparkPool(
            enable_customized_compute_conf=data["enableCustomizedComputeConf"],
            machine_learning_auto_log=self._parse_machine_learning_auto_log(data["machineLearningAutoLog"]),
            job_management=self._parse_job_management(data["jobManagement"]),
            high_concurrency=self._parse_high_concurrency_params(data["highConcurrency"]),
            details=self._parse_pool_details(data["details"]),
            runtime_version=SparkRuntimeVersion(data["runtimeVersion"]),
        )

    def _parse_machine_learning_auto_log(self, data: dict[str, Any]) -> MachineLearningAutoLog:
        """Parse machine learning auto log configuration."""
        return MachineLearningAutoLog(enabled=data["enabled"])

    def _parse_job_management(self, data: dict[str, Any]) -> JobManagement:
        """Parse job management configuration."""
        return JobManagement(
            conservative_job_admission_enabled=data["conservativeJobAdmissionEnabled"],
            session_timeout_in_minutes=data["sessionTimeoutInMinutes"],
        )

    def _parse_high_concurrency_params(self, data: dict[str, Any]) -> HighConcurrencyConfig:
        """Parse high concurrency configuration."""
        return HighConcurrencyConfig(
            enabled=data["enabled"],
            notebook_pipeline_run_enabled=data["notebookPipelineRunEnabled"],
        )

    def _parse_pool_details(self, data: dict[str, Any]) -> PoolDetails:
        """Parse pool details configuration."""
        return PoolDetails(
            id=data["id"],
            name=data["name"],
            node_size_family=NodeSizeFamily(data["nodeSizeFamily"]),
            node_size=NodeSize(data["nodeSize"]),
            auto_scale=self._parse_auto_scale(data["autoScale"]),
            dynamic_executor_allocation=self._parse_dynamic_executor_allocation(data["dynamicExecutorAllocation"]),
        )

    def _parse_auto_scale(self, data: dict[str, Any]) -> AutoScale:
        """Parse auto scale configuration."""
        return AutoScale(
            enabled=data["enabled"],
            min_node_count=data["minNodeCount"],
            max_node_count=data["maxNodeCount"],
        )

    def _parse_dynamic_executor_allocation(self, data: dict[str, Any]) -> DynamicExecutorAllocation:
        """Parse dynamic executor allocation configuration."""
        return DynamicExecutorAllocation(
            enabled=data["enabled"],
            min_executors=data["minExecutors"],
            max_executors=data["maxExecutors"],
        )

    def _parse_rbac_params(self, data: dict[str, Any]) -> RbacParams:
        """Parse RBAC parameters."""
        workspace_rbac = []
        if "workspace" in data:
            for workspace_rbac_data in data["workspace"]:
                workspace_rbac.append(self._parse_workspace_rbac_params(workspace_rbac_data))

        items_rbac = []
        for item_rbac_data in data["items"]:
            items_rbac.append(self._parse_item_rbac_params(item_rbac_data))

        universal_security = self._parse_universal_security_params(data["universalSecurity"])

        return RbacParams(
            purge_unmatched_role_assignments=data["purgeUnmatchedRoleAssignments"],
            workspace=self._deduplicate_list(workspace_rbac),
            items=self._deduplicate_list(items_rbac),
            universal_security=universal_security,
        )

    def _parse_universal_security_params(self, data: dict[str, Any]) -> UniversalSecurity:
        """
        Parse UniversalSecurity.

        Args:
            data: Dictionary containing universalSecurity configuration data

        Returns:
            UniversalSecurity: Dataclass instance with parsed configuration
        """
        return UniversalSecurity(sql_endpoint=self._parse_sql_endpoint_params(data["sqlEndpoint"]))

    def _parse_sql_endpoint_params(self, data: dict[str, Any]) -> SqlEndpointSecurity:
        """
        Parse the SqlEndpointSecurity block into SqlEndpointSecurity dataclass and require 'enabled'.

        Args:
            data: Dictionary containing SqlEndpointSecurity configuration parameters

        Returns:
            SqlEndpointSecurity: Dataclass instance with parsed enabled parameter
        """
        return SqlEndpointSecurity(enabled=data["enabled"], skip_reconcile=data.get("skipReconcile", []))

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
            self.logger.info(f"Removed {len(items) - len(deduplicated)} duplicate entries from list")

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
            detail_rbac.append(self._parse_item_rbac_detail_params(detail_rbac_data))

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
