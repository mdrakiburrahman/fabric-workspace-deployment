# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
from pathlib import Path
from types import MethodType, SimpleNamespace

from fabric_workspace_deployment.manager.fabric.cicd import FabricCicdManager
from fabric_workspace_deployment.manager.fabric.seed import FabricSeedManager
from fabric_workspace_deployment.operations.operation_interfaces import ArtifactType, FabricParams, FabricStorageParams, LocalConcreteFile, OperationParams, SeedFile, SparkJobDefinition, SparkJobDefinitionV1Config, StorageAccountFile, StorageRbacAuthMode, StorageRbacAuthParams, StorageRbacParams
from fabric_workspace_deployment.storage_namespace import DeploymentNamespace, SeedPathBinding, StorageNamespaceRewriter

ACCOUNT = "arcdatasynapsedogfood"
CONTAINER = "onelake"


def seed(local_path: str, storage_path: str) -> SeedFile:
    return SeedFile(local_concrete_file=LocalConcreteFile(local_path), storage_account_file=StorageAccountFile(storage_path))


def common(root: Path, seeds: list[SeedFile]):
    storage = FabricStorageParams(
        account=ACCOUNT,
        container=CONTAINER,
        location="eastus",
        resource_group="analytics-rg",
        subscription_id="subscription",
        tenant_id="tenant",
        shortcut_data_connection_id="connection",
        seed_files=seeds,
        rbac=StorageRbacParams(StorageRbacAuthParams(StorageRbacAuthMode.CLI)),
        deployment_namespace=DeploymentNamespace("user/dev"),
    )
    return SimpleNamespace(local=SimpleNamespace(root_folder=str(root)), fabric=FabricParams([], [storage]))


def cicd_manager(root: Path, seeds: list[SeedFile], spark_job_client=None, folder_client=None):
    return FabricCicdManager(common(root, seeds), None, None, None, None, spark_job_client, folder_client, None, None)


def test_rewriter_localizes_declared_seed_references_only():
    rewriter = StorageNamespaceRewriter(
        [
            SeedPathBinding(ACCOUNT, CONTAINER, "jars/sparkMsit.jar", "user/dev"),
            SeedPathBinding(ACCOUNT, CONTAINER, "configs/sparkMsit-fabric.yaml", "user/dev"),
            SeedPathBinding(ACCOUNT, CONTAINER, "pkgs/spark-dbt-bundle.tar.gz", "user/dev"),
        ]
    )
    content = "\n".join(
        [
            f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net/jars/sparkMsit.jar",
            "Files/onelake/configs/sparkMsit-fabric.yaml",
            "/lakehouse/default/Files/onelake/pkgs/spark-dbt-bundle.tar.gz",
            "Files/onelake/synapse/workspaces/prod/warehouse",
        ]
    )

    rewritten = rewriter.rewrite_text(content)

    assert f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net/user/dev/jars/sparkMsit.jar" in rewritten
    assert "Files/onelake/user/dev/configs/sparkMsit-fabric.yaml" in rewritten
    assert "/lakehouse/default/Files/onelake/user/dev/pkgs/spark-dbt-bundle.tar.gz" in rewritten
    assert "Files/onelake/synapse/workspaces/prod/warehouse" in rewritten


def test_storage_parser_keeps_namespace_optional():
    params = OperationParams.__new__(OperationParams)
    raw = {
        "account": ACCOUNT,
        "container": CONTAINER,
        "location": "eastus",
        "resourceGroup": "analytics-rg",
        "subscriptionId": "subscription",
        "tenantId": "tenant",
        "shortcutDataConnectionId": "connection",
        "rbac": {"auth": {"mode": "cli"}},
        "seedFiles": [],
    }

    assert params._parse_fabric_storage_params(raw).deployment_namespace is None
    assert params._parse_fabric_storage_params({**raw, "deploymentNamespace": {"pathPrefix": "user/dev/"}}).deployment_namespace == DeploymentNamespace("user/dev")


def test_seed_manager_uploads_namespaced_and_rewritten_payloads(tmp_path: Path):
    (tmp_path / "workspace.json").write_text('{"config":"onelake/configs/app.json"}', encoding="utf-8")
    (tmp_path / "app.json").write_text("{}", encoding="utf-8")
    (tmp_path / "spark.jar").write_bytes(b"\x00\xffjar")
    uploads = {}

    class StorageManager:
        def upload_blob(self, _account, _container, local_path, destination):
            uploads[destination] = Path(local_path).read_bytes()

    manager = FabricSeedManager(
        common(
            tmp_path,
            [
                seed("workspace.json", "configs/workspace.json"),
                seed("app.json", "configs/app.json"),
                seed("spark.jar", "jars/spark.jar"),
            ],
        ),
        StorageManager(),
    )

    asyncio.run(manager._execute())

    assert uploads["user/dev/configs/workspace.json"] == b'{"config":"onelake/user/dev/configs/app.json"}'
    assert uploads["user/dev/jars/spark.jar"] == b"\x00\xffjar"
    assert (tmp_path / "workspace.json").read_text(encoding="utf-8") == '{"config":"onelake/configs/app.json"}'


def test_cicd_publishes_a_rewritten_staging_tree(tmp_path: Path):
    artifacts = tmp_path / "artifacts"
    notebook = artifacts / "Notebook.Notebook" / "notebook-content.py"
    notebook.parent.mkdir(parents=True)
    notebook.write_text("Files/onelake/configs/app.json", encoding="utf-8")
    (artifacts / "parameter.yml").write_text("find_replace: []", encoding="utf-8")
    manager = cicd_manager(tmp_path, [seed("app.json", "configs/app.json")])
    observed = {}

    async def capture(self, workspace_id, workspace_params, template_params, artifacts_root):
        observed["root"] = artifacts_root
        observed["content"] = (artifacts_root / "Notebook.Notebook" / "notebook-content.py").read_text(encoding="utf-8")

    manager._reconcile_artifacts = MethodType(capture, manager)
    template = SimpleNamespace(artifacts_folder="artifacts", parameter_file_path="artifacts/parameter.yml", spark_job_definitions=[], custom_libraries=[], generator=SimpleNamespace(monitoring=[]))

    asyncio.run(manager.reconcile("workspace", SimpleNamespace(), template))

    assert observed["content"] == "Files/onelake/user/dev/configs/app.json"
    assert not observed["root"].exists()
    assert notebook.read_text(encoding="utf-8") == "Files/onelake/configs/app.json"


def test_sjd_post_update_uses_the_same_namespace_map(tmp_path: Path):
    deployed = []

    class FolderClient:
        async def get_fabric_folder_collection(self, _workspace_id):
            return SimpleNamespace(artifacts=[SimpleNamespace(display_name="sparkMsit", object_id="job-id", type_name=ArtifactType.SPARK_JOB_DEFINITION.value)])

    class SparkJobClient:
        async def update_spark_job_definition_config(self, _workspace_id, _artifact_id, _lakehouse_name, config):
            deployed.append(config)

    manager = cicd_manager(tmp_path, [seed("spark.jar", "jars/sparkMsit.jar"), seed("config.yaml", "configs/sparkMsit-fabric.yaml")], SparkJobClient(), FolderClient())
    config = SparkJobDefinitionV1Config(
        executable_file=f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net/jars/sparkMsit.jar",
        default_lakehouse_artifact_id="lakehouse",
        main_class="example.Main",
        additional_lakehouse_ids=[],
        retry_policy=None,
        command_line_arguments="Files/onelake/configs/sparkMsit-fabric.yaml",
        additional_library_uris=[],
        language="Scala/Java",
        environment_artifact_id=None,
    )
    template = SimpleNamespace(spark_job_definitions=[SparkJobDefinition("sparkMsit", "", "artifacts/sparkMsit.SparkJobDefinition", "lakehouse", config)])

    asyncio.run(manager._update_spark_job_definition_configs("workspace", template))

    assert deployed[0].executable_file.endswith("/user/dev/jars/sparkMsit.jar")
    assert deployed[0].command_line_arguments == "Files/onelake/user/dev/configs/sparkMsit-fabric.yaml"
