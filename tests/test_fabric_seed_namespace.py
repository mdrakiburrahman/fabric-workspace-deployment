# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
from pathlib import Path
from types import SimpleNamespace

from fabric_workspace_deployment.manager.fabric.seed import FabricSeedManager
from fabric_workspace_deployment.operations.operation_interfaces import FabricParams, FabricStorageParams, LocalConcreteFile, SeedFile, StorageAccountFile, StorageRbacAuthMode, StorageRbacAuthParams, StorageRbacParams
from fabric_workspace_deployment.storage_namespace import DeploymentNamespace


class FakeStorageManager:
    def __init__(self):
        self.calls = []

    def upload_blob(self, account, container, absolute_local_file_path, azure_file_path):
        local_path = Path(absolute_local_file_path)
        self.calls.append(
            {
                "account": account,
                "container": container,
                "local_path": local_path,
                "azure_file_path": azure_file_path,
                "content": local_path.read_bytes(),
            }
        )


def _seed(local_path: str, storage_path: str) -> SeedFile:
    return SeedFile(
        local_concrete_file=LocalConcreteFile(local_path),
        storage_account_file=StorageAccountFile(storage_path),
    )


def _common(root: Path, seed_files: list[SeedFile]):
    storage = FabricStorageParams(
        account="arcdatasynapsedogfood",
        container="onelake",
        location="eastus",
        resource_group="analytics-rg",
        subscription_id="subscription",
        tenant_id="tenant",
        shortcut_data_connection_id="connection",
        seed_files=seed_files,
        rbac=StorageRbacParams(StorageRbacAuthParams(StorageRbacAuthMode.CLI)),
        deployment_namespace=DeploymentNamespace("user/dev"),
    )
    return SimpleNamespace(
        local=SimpleNamespace(root_folder=str(root)),
        fabric=FabricParams(workspaces=[], storages=[storage]),
    )


def test_seed_manager_uploads_namespaced_paths_and_rewrites_text_dependencies(tmp_path: Path):
    (tmp_path / "workspace-lib.json").write_text(
        '{"config":"onelake/configs/fabric-workspace-deployment.json"}',
        encoding="utf-8",
    )
    (tmp_path / "fabric-config.json").write_text('{"operation":"dryRun"}', encoding="utf-8")
    binary = b"\x00\xffjar"
    (tmp_path / "spark.jar").write_bytes(binary)

    seeds = [
        _seed("workspace-lib.json", "configs/workspace-lib-fabric.json"),
        _seed("fabric-config.json", "configs/fabric-workspace-deployment.json"),
        _seed("spark.jar", "jars/sparkMsit.jar"),
    ]
    storage_manager = FakeStorageManager()
    manager = FabricSeedManager(_common(tmp_path, seeds), storage_manager)

    asyncio.run(manager._execute())

    calls = {call["azure_file_path"]: call for call in storage_manager.calls}
    assert set(calls) == {
        "user/dev/configs/workspace-lib-fabric.json",
        "user/dev/configs/fabric-workspace-deployment.json",
        "user/dev/jars/sparkMsit.jar",
    }
    assert calls["user/dev/configs/workspace-lib-fabric.json"]["content"].decode("utf-8") == '{"config":"onelake/user/dev/configs/fabric-workspace-deployment.json"}'
    assert calls["user/dev/jars/sparkMsit.jar"]["content"] == binary
    assert calls["user/dev/jars/sparkMsit.jar"]["local_path"] == tmp_path / "spark.jar"
    assert (tmp_path / "workspace-lib.json").read_text(encoding="utf-8") == '{"config":"onelake/configs/fabric-workspace-deployment.json"}'
