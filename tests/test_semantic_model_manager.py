# SPDX-FileCopyrightText: 2025-present Raki Rahman <mdrakiburrahman@gmail.com>
#
# SPDX-License-Identifier: MIT

import asyncio

from pathlib import Path
from types import SimpleNamespace

import pytest

from fabric_workspace_deployment.manager.fabric import model as model_module
from fabric_workspace_deployment.manager.fabric.model import SemanticModelManager
from fabric_workspace_deployment.operations.operation_interfaces import ModelParams


def _common(root: Path):
    return SimpleNamespace(
        local=SimpleNamespace(root_folder=str(root)),
        endpoint=SimpleNamespace(
            cicd="https://api.fabric.microsoft.com",
            analysis_service="https://wabi.example.invalid",
        ),
        scope=SimpleNamespace(
            analysis_service="https://analysis.windows.net/powerbi/api",
        ),
    )


def _workspace_params():
    return SimpleNamespace(
        template=SimpleNamespace(
            artifacts_folder="artifacts",
            environment_key="dev",
            feature_flags=["configured-flag"],
        )
    )


def _model_artifact(model_id: int = 42):
    return SimpleNamespace(
        id=model_id,
        type_name="Model",
        display_name="insights",
    )


class FakeFolderClient:
    def __init__(self, artifact_sequences):
        self.artifact_sequences = list(artifact_sequences)
        self.calls = []

    async def get_fabric_folder_collection(self, workspace_id):
        self.calls.append(workspace_id)
        if len(self.artifact_sequences) > 1:
            artifacts = self.artifact_sequences.pop(0)
        else:
            artifacts = self.artifact_sequences[0]
        return SimpleNamespace(artifacts=artifacts)


class FakeHttpRetry:
    def __init__(self):
        self.calls = []

    def execute(self, func, url, **kwargs):
        self.calls.append((func, url, kwargs))
        return SimpleNamespace()


class FakeAzCli:
    def get_access_token(self, scope):
        assert scope == "https://analysis.windows.net/powerbi/api"
        return "fabric-token"


def _manager(root: Path, folder_client: FakeFolderClient, http_retry: FakeHttpRetry, token_credential=None):
    return SemanticModelManager(
        _common(root),
        FakeAzCli(),
        SimpleNamespace(),
        folder_client,
        http_retry,
        token_credential or object(),
    )


def _patch_fabric_cicd(monkeypatch):
    import fabric_cicd

    captured = {
        "feature_flags": [],
    }

    class FakeFabricWorkspace:
        def __init__(self, **kwargs):
            captured["workspace_kwargs"] = kwargs

    def fake_publish_all_items(workspace, **kwargs):
        captured["workspace"] = workspace
        captured["publish_kwargs"] = kwargs

    monkeypatch.setattr(fabric_cicd, "disable_file_logging", lambda: None)
    monkeypatch.setattr(fabric_cicd, "append_feature_flag", captured["feature_flags"].append)
    monkeypatch.setattr(fabric_cicd, "FabricWorkspace", FakeFabricWorkspace)
    monkeypatch.setattr(fabric_cicd, "publish_all_items", fake_publish_all_items)
    return captured


def test_existing_model_only_reconciles_settings(tmp_path, monkeypatch):
    import fabric_cicd

    monkeypatch.setattr(fabric_cicd, "publish_all_items", lambda *args, **kwargs: pytest.fail("existing models must not be republished"))
    folder_client = FakeFolderClient([[_model_artifact()]])
    http_retry = FakeHttpRetry()
    manager = _manager(tmp_path, folder_client, http_retry)

    asyncio.run(
        manager.reconcile(
            "workspace-id",
            ModelParams(display_name="insights", direct_lake_auto_sync=False),
            _workspace_params(),
        )
    )

    assert len(folder_client.calls) == 1
    assert http_retry.calls[0][1].endswith("/metadata/models/42/settings")
    assert http_retry.calls[0][2]["data"] == '{"directLakeAutoSync":false}'


def test_missing_model_is_published_from_workspace_artifacts(tmp_path, monkeypatch):
    source_directory = tmp_path / "artifacts" / "projects" / "insights" / "insights.SemanticModel"
    source_directory.mkdir(parents=True)
    folder_client = FakeFolderClient([[], [_model_artifact()]])
    http_retry = FakeHttpRetry()
    token_credential = object()
    manager = _manager(tmp_path, folder_client, http_retry, token_credential)
    captured = _patch_fabric_cicd(monkeypatch)

    asyncio.run(
        manager.reconcile(
            "workspace-id",
            ModelParams(display_name="insights", direct_lake_auto_sync=True),
            _workspace_params(),
        )
    )

    assert captured["workspace_kwargs"] == {
        "workspace_id": "workspace-id",
        "environment": "dev",
        "repository_directory": str(tmp_path / "artifacts"),
        "item_type_in_scope": ["SemanticModel"],
        "token_credential": token_credential,
    }
    assert captured["publish_kwargs"] == {
        "items_to_include": ["insights.SemanticModel"],
    }
    assert captured["feature_flags"] == [
        "configured-flag",
        "enable_experimental_features",
        "enable_items_to_include",
    ]
    assert folder_client.calls == ["workspace-id", "workspace-id"]
    assert http_retry.calls[0][1].endswith("/metadata/models/42/settings")
    assert http_retry.calls[0][2]["data"] == '{"directLakeAutoSync":true}'


def test_missing_model_source_fails_instead_of_warning(tmp_path):
    (tmp_path / "artifacts").mkdir()
    manager = _manager(tmp_path, FakeFolderClient([[]]), FakeHttpRetry())

    with pytest.raises(RuntimeError, match="source directory 'insights.SemanticModel' was not found"):
        asyncio.run(
            manager.reconcile(
                "workspace-id",
                ModelParams(display_name="insights", direct_lake_auto_sync=False),
                _workspace_params(),
            )
        )


def test_publish_fails_when_model_never_becomes_available(tmp_path, monkeypatch):
    (tmp_path / "artifacts" / "insights.SemanticModel").mkdir(parents=True)
    folder_client = FakeFolderClient([[]])
    manager = _manager(tmp_path, folder_client, FakeHttpRetry())
    _patch_fabric_cicd(monkeypatch)
    monkeypatch.setattr(model_module, "MODEL_PUBLISH_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(model_module, "MODEL_PUBLISH_RETRY_DELAY_SECONDS", 0)

    with pytest.raises(RuntimeError, match="did not become available"):
        asyncio.run(
            manager.reconcile(
                "workspace-id",
                ModelParams(display_name="insights", direct_lake_auto_sync=False),
                _workspace_params(),
            )
        )

    assert folder_client.calls == ["workspace-id", "workspace-id", "workspace-id"]
