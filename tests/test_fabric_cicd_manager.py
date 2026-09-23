# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio
import sys
import types

from types import SimpleNamespace

from fabric_workspace_deployment.manager.fabric.cicd import FabricCicdManager


def test_reconcile_uses_one_full_scope_workspace(tmp_path, monkeypatch):
    artifacts_folder = tmp_path / "artifacts"
    artifacts_folder.mkdir()
    (tmp_path / "parameter.yml").write_text("find_replace: []\n", encoding="utf-8")

    common_params = SimpleNamespace(
        local=SimpleNamespace(root_folder=str(tmp_path)),
        endpoint=SimpleNamespace(cicd="https://api.fabric.microsoft.com"),
    )
    manager = FabricCicdManager(
        common_params=common_params,
        token_credential=object(),
        az_cli=object(),
        fabric_cli=object(),
        workspace=object(),
        spark_job_definition_client=object(),
        folder_client=object(),
        monitoring_manager=object(),
        spark_environment_client=object(),
    )

    item_types_in_scope = ["DataPipeline", "Reflex"]
    template_params = SimpleNamespace(
        spark_job_definitions=[],
        custom_libraries=[],
        generator=SimpleNamespace(monitoring=[]),
        parameter_file_path="parameter.yml",
        item_types_in_scope=item_types_in_scope,
        environment_key="CI",
        artifacts_folder="artifacts",
        feature_flags=[],
        unpublish_orphans=True,
    )

    constructed_scopes = []
    published_scopes = []
    unpublished_scopes = []

    class FakeFabricWorkspace:
        def __init__(self, *, item_type_in_scope, **_kwargs):
            self.item_type_in_scope = list(item_type_in_scope)
            constructed_scopes.append(list(item_type_in_scope))

    fabric_cicd = types.ModuleType("fabric_cicd")
    fabric_cicd.constants = SimpleNamespace(DEFAULT_API_ROOT_URL=None)
    fabric_cicd.disable_file_logging = lambda: None
    fabric_cicd.append_feature_flag = lambda _feature_flag: None
    fabric_cicd.FabricWorkspace = FakeFabricWorkspace
    fabric_cicd.publish_all_items = lambda workspace: published_scopes.append(list(workspace.item_type_in_scope))
    fabric_cicd.unpublish_all_orphan_items = lambda workspace: unpublished_scopes.append(list(workspace.item_type_in_scope))
    monkeypatch.setitem(sys.modules, "fabric_cicd", fabric_cicd)

    asyncio.run(manager.reconcile("workspace-id", SimpleNamespace(), template_params))

    assert constructed_scopes == [item_types_in_scope]
    assert published_scopes == [item_types_in_scope]
    assert unpublished_scopes == [item_types_in_scope]


def test_fabric_cicd_publishes_data_pipeline_before_reflex():
    from fabric_cicd import constants

    publish_order = [item_type.value for item_type in constants.SERIAL_ITEM_PUBLISH_ORDER.values()]

    assert publish_order.index("DataPipeline") < publish_order.index("Reflex")
