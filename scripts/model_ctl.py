"""Model registry operations: check a stage, promote a training run.

`train.py` registers every model that passes its quality gate. This script is
the only thing that moves a registered version into a stage, so promotion is a
deliberate CI step rather than a side effect of training.
"""

from __future__ import annotations

import mlflow
import typer
from mlflow.exceptions import MlflowException, RestException

from config.settings import get_settings

app = typer.Typer()


def _client() -> mlflow.tracking.MlflowClient:
    cfg = get_settings()
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    return mlflow.tracking.MlflowClient()


@app.command()
def exists(
    stage: str = typer.Option("Production", help="Model stage to check"),
):
    """Exit 0 if a model version exists in *stage*, else exit 1."""
    cfg = get_settings()
    client = _client()
    try:
        versions = client.get_latest_versions(cfg.model_name, stages=[stage])
    except RestException as exc:
        print(f"No {cfg.model_name} in stage {stage} ({exc})")
        raise typer.Exit(code=1) from exc
    if versions:
        print(f"Found {cfg.model_name} v{versions[0].version} in stage {stage}")
        raise typer.Exit(code=0)
    print(f"No {cfg.model_name} in stage {stage}")
    raise typer.Exit(code=1)


@app.command()
def promote(
    run_id: str = typer.Option(..., "--run-id", help="MLflow run ID that registered the model"),
    stage: str = typer.Option("Staging", help="Target stage"),
):
    """Move the version registered by *run_id* into *stage*."""
    cfg = get_settings()
    client = _client()

    try:
        versions = client.search_model_versions(f"run_id='{run_id}'")
    except MlflowException as exc:
        print(f"Run {run_id} has no registered {cfg.model_name} version: {exc}")
        raise typer.Exit(code=1) from exc

    if not versions:
        print(f"Run {run_id} has no registered {cfg.model_name} version")
        raise typer.Exit(code=1)

    version = versions[0].version
    client.transition_model_version_stage(
        name=cfg.model_name,
        version=version,
        stage=stage,
        archive_existing_versions=(stage == "Production"),
    )
    print(f"Promoted {cfg.model_name} v{version} (run {run_id}) → {stage}")


if __name__ == "__main__":
    app()
