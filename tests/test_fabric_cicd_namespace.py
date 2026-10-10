# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
from pathlib import Path
from types import MethodType, SimpleNamespace

from fabric_workspace_deployment.manager.fabric.cicd import FabricCicdManager
from fabric_workspace_deployment.operations.operation_interfaces import ArtifactType, FabricParams, FabricStorageParams, LocalConcreteFile, SeedFile, SparkJobDefinition, SparkJobDefinitionV1Config, StorageAccountFile, StorageRbacAuthMode, StorageRbacAuthParams, StorageRbacParams
from fabric_workspace_deployment.storage_namespace import DeploymentNamespace


class FakeWorkspace:
    async def get(self, _workspace_params):
        return SimpleNamespace(capacity_id="capacity")


class FakeFolderClient:
    def __init__(self, artifacts):
        self.artifacts = artifacts

    async def get_fabric_folder_collection(self, _workspace_id):
        return SimpleNamespace(artifacts=self.artifacts)


class FakeSparkJobDefinitionClient:
    def __init__(self):
        self.configs = []

    async def update_spark_job_definition_config(self, workspace_id, artifact_id, lakehouse_name, config):
        self.configs.append((workspace_id, artifact_id, lakehouse_name, config))


class FakeSparkEnvironmentClient:
    def __init__(self):
        self.spark_settings = []
        self.published = []

    async def put_spark_settings(self, capacity_id, workspace_id, environment_artifact_id, sparkcompute_yaml_path):
        self.spark_settings.append(
            {
                "capacity_id": capacity_id,
                "workspace_id": workspace_id,
                "environment_artifact_id": environment_artifact_id,
                "path": Path(sparkcompute_yaml_path),
                "content": Path(sparkcompute_yaml_path).read_text(encoding="utf-8"),
            }
        )

    async def publish_spark_settings(self, capacity_id, workspace_id, environment_artifact_id):
        self.published.append((capacity_id, workspace_id, environment_artifact_id))


def _common(root: Path):
    storage = FabricStorageParams(
        account="arcdatasynapsedogfood",
        container="onelake",
        location="eastus",
        resource_group="analytics-rg",
        subscription_id="subscription",
        tenant_id="tenant",
        shortcut_data_connection_id="connection",
        seed_files=[
            SeedFile(
                local_concrete_file=LocalConcreteFile("spark.jar"),
                storage_account_file=StorageAccountFile("jars/sparkMsit.jar"),
            ),
            SeedFile(
                local_concrete_file=LocalConcreteFile("config.yaml"),
                storage_account_file=StorageAccountFile("configs/sparkMsit-fabric.yaml"),
            ),
        ],
        rbac=StorageRbacParams(StorageRbacAuthParams(StorageRbacAuthMode.CLI)),
        deployment_namespace=DeploymentNamespace("user/dev"),
    )
    return SimpleNamespace(
        local=SimpleNamespace(root_folder=str(root)),
        fabric=FabricParams(workspaces=[], storages=[storage]),
    )


def _manager(root: Path, spark_job_client=None, folder_client=None, workspace=None, spark_environment_client=None):
    return FabricCicdManager(
        common_params=_common(root),
        token_credential=None,
        az_cli=None,
        fabric_cli=None,
        workspace=workspace,
        spark_job_definition_client=spark_job_client,
        folder_client=folder_client,
        monitoring_manager=None,
        spark_environment_client=spark_environment_client,
    )


def test_reconcile_uses_rewritten_staging_tree_without_mutating_source(tmp_path: Path):
    artifacts = tmp_path / "artifacts"
    notebook = artifacts / "Notebook.Notebook" / "notebook-content.py"
    notebook.parent.mkdir(parents=True)
    notebook.write_text("# MAGIC Files/onelake/configs/sparkMsit-fabric.yaml", encoding="utf-8")
    (artifacts / "parameter.yml").write_text("find_replace: []", encoding="utf-8")

    manager = _manager(tmp_path)
    observed = {}

    async def capture(self, workspace_id, workspace_params, template_params, artifacts_root):
        observed["workspace_id"] = workspace_id
        observed["root"] = artifacts_root
        observed["content"] = (artifacts_root / "Notebook.Notebook" / "notebook-content.py").read_text(encoding="utf-8")

    manager._reconcile_artifacts = MethodType(capture, manager)
    template = SimpleNamespace(
        artifacts_folder="artifacts",
        parameter_file_path="artifacts/parameter.yml",
        spark_job_definitions=[],
        custom_libraries=[],
        generator=SimpleNamespace(monitoring=[]),
    )

    asyncio.run(manager.reconcile("workspace", SimpleNamespace(), template))

    assert observed["workspace_id"] == "workspace"
    assert observed["content"] == "# MAGIC Files/onelake/user/dev/configs/sparkMsit-fabric.yaml"
    assert observed["root"] != artifacts
    assert not observed["root"].exists()
    assert notebook.read_text(encoding="utf-8") == "# MAGIC Files/onelake/configs/sparkMsit-fabric.yaml"


def test_spark_job_definition_post_update_uses_namespaced_config(tmp_path: Path):
    client = FakeSparkJobDefinitionClient()
    folder = FakeFolderClient(
        [
            SimpleNamespace(
                display_name="sparkMsit",
                object_id="spark-job-id",
                type_name=ArtifactType.SPARK_JOB_DEFINITION.value,
            )
        ]
    )
    manager = _manager(tmp_path, spark_job_client=client, folder_client=folder)
    config = SparkJobDefinitionV1Config(
        executable_file="abfss://onelake@arcdatasynapsedogfood.dfs.core.windows.net/jars/sparkMsit.jar",
        default_lakehouse_artifact_id="lakehouse",
        main_class="example.Main",
        additional_lakehouse_ids=[],
        retry_policy=None,
        command_line_arguments="Files/onelake/configs/sparkMsit-fabric.yaml",
        additional_library_uris=["abfss://onelake@arcdatasynapsedogfood.dfs.core.windows.net/jars/sparkMsit.jar"],
        language="Scala/Java",
        environment_artifact_id=None,
    )
    template = SimpleNamespace(
        spark_job_definitions=[
            SparkJobDefinition(
                display_name="sparkMsit",
                description="",
                nested_folder_path="artifacts/sparkMsit.SparkJobDefinition",
                default_lakehouse_artifact_name="lakehouse",
                spark_job_definition_v1_config=config,
            )
        ]
    )

    asyncio.run(manager._update_spark_job_definition_configs("workspace", template))

    deployed = client.configs[0][3]
    assert deployed.executable_file.endswith("/user/dev/jars/sparkMsit.jar")
    assert deployed.command_line_arguments == "Files/onelake/user/dev/configs/sparkMsit-fabric.yaml"
    assert deployed.additional_library_uris == ["abfss://onelake@arcdatasynapsedogfood.dfs.core.windows.net/user/dev/jars/sparkMsit.jar"]
    assert config.executable_file.endswith("/jars/sparkMsit.jar")


def test_environment_post_processing_reads_selected_artifacts_root(tmp_path: Path):
    source_env = tmp_path / "source" / "Example.Environment" / "Setting"
    source_env.mkdir(parents=True)
    (source_env / "Sparkcompute.yml").write_text("spark.jars: root", encoding="utf-8")

    staged_root = tmp_path / "staged"
    staged_env = staged_root / "Example.Environment" / "Setting"
    staged_env.mkdir(parents=True)
    (staged_env / "Sparkcompute.yml").write_text("spark.jars: namespaced", encoding="utf-8")

    spark_client = FakeSparkEnvironmentClient()
    folder = FakeFolderClient(
        [
            SimpleNamespace(
                display_name="Example",
                object_id="environment-id",
                type_name=ArtifactType.ENVIRONMENT.value,
            )
        ]
    )
    manager = _manager(
        tmp_path / "source",
        folder_client=folder,
        workspace=FakeWorkspace(),
        spark_environment_client=spark_client,
    )

    asyncio.run(manager._post_process_environment_spark_settings("workspace", SimpleNamespace(), staged_root))

    assert spark_client.spark_settings[0]["path"] == staged_env / "Sparkcompute.yml"
    assert spark_client.spark_settings[0]["content"] == "spark.jars: namespaced"
    assert spark_client.published == [("capacity", "workspace", "environment-id")]
