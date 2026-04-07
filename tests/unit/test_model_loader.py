"""The served model and the metadata stamped on predictions must agree."""

from __future__ import annotations

import types

import pytest

import api.model_loader as loader

CFG = types.SimpleNamespace(
    model_name="fraud_detector",
    model_stage="Production",
    mlflow_tracking_uri="file:///tmp/registry",
    model_cache_ttl_secs=60,
    decision_threshold=0.5,
)


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    loader.invalidate_cache()
    loader.get_settings = lambda: CFG
    clock = [0.0]
    monkeypatch.setattr(loader, "time", types.SimpleNamespace(monotonic=lambda: clock[0]))
    registry = {
        "version": "1",
        "run_id": "run-1",
        "decision_threshold": 0.3,
    }
    monkeypatch.setattr(loader, "get_model_info", lambda stage=None: dict(registry))

    artifacts = []

    def fake_load(uri):
        artifact = f"model-{registry['version']}"
        artifacts.append((uri, artifact))
        return artifact

    # Replace the module's mlflow handle outright: `mlflow.sklearn` is a lazy
    # module proxy, so patching an attribute on it does not reach the real call.
    monkeypatch.setattr(
        loader,
        "mlflow",
        types.SimpleNamespace(
            set_tracking_uri=lambda uri: None,
            sklearn=types.SimpleNamespace(load_model=fake_load),
        ),
    )
    yield types.SimpleNamespace(clock=clock, registry=registry, artifacts=artifacts)
    loader.invalidate_cache()


def test_metadata_travels_with_the_cached_artifact(_isolate):
    model, info = loader.load_model_and_info()
    assert model == "model-1"
    assert info["version"] == "1"
    assert info["decision_threshold"] == 0.3

    # A new version is promoted while v1 is still inside its TTL.
    _isolate.registry.update({"version": "2", "run_id": "run-2", "decision_threshold": 0.7})

    cached_model, cached_info = loader.load_model_and_info()
    assert cached_model == "model-1"
    assert cached_info["version"] == "1"
    assert cached_info["decision_threshold"] == 0.3  # not the promoted 0.7

    _isolate.clock[0] = 61  # TTL lapses
    refreshed_model, refreshed_info = loader.load_model_and_info()
    assert refreshed_model == "model-2"
    assert refreshed_info["version"] == "2"
    assert refreshed_info["decision_threshold"] == 0.7
    assert len(_isolate.artifacts) == 2  # artifacts are only re-downloaded on change


def test_unchanged_version_does_not_redownload(_isolate):
    loader.load_model_and_info()
    _isolate.clock[0] = 61
    model, _ = loader.load_model_and_info()
    assert model == "model-1"
    assert len(_isolate.artifacts) == 1


def test_load_model_returns_only_the_artifact(_isolate):
    assert loader.load_model() == "model-1"


def test_missing_version_is_unavailable(_isolate):
    _isolate.registry["version"] = None
    with pytest.raises(loader.ModelUnavailable, match="no version in stage"):
        loader.load_model_and_info()


def test_registry_failure_is_unavailable(monkeypatch, _isolate):
    def boom(stage=None):
        raise RuntimeError("registry down")

    monkeypatch.setattr(loader, "get_model_info", boom)
    with pytest.raises(loader.ModelUnavailable, match="registry unreachable"):
        loader.load_model_and_info()
