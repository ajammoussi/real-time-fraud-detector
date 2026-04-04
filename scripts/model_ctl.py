"""Promote a model version in MLflow registry."""
import mlflow
import typer
from mlflow.exceptions import RestException
from config.settings import get_settings

app = typer.Typer()


@app.command()
def exists(
    stage: str = typer.Option("Production", help="Model stage to check"),
):
    """Exit 0 if model exists in stage, else exit 1."""
    cfg = get_settings()
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    client = mlflow.tracking.MlflowClient()
    try:
        versions = client.get_latest_versions(cfg.model_name, stages=[stage])
    except RestException:
        print(f"No {cfg.model_name} in stage {stage}")
        raise typer.Exit(code=1)
    if versions:
        print(f"Found {cfg.model_name} in stage {stage}")
        raise typer.Exit(code=0)
    print(f"No {cfg.model_name} in stage {stage}")
    raise typer.Exit(code=1)

@app.command()
def promote(
    run_id:   str = typer.Argument(..., help="MLflow run ID"),
    stage:    str = typer.Option("Staging", help="Target stage"),
):
    cfg = get_settings()
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    client = mlflow.tracking.MlflowClient()
    mv = client.create_model_version(
        name=cfg.model_name, source=f"runs:/{run_id}/model", run_id=run_id
    )
    client.transition_model_version_stage(
        name=cfg.model_name, version=mv.version, stage=stage,
        archive_existing_versions=(stage == "Production"),
    )
    print(f"Promoted {cfg.model_name} v{mv.version} → {stage}")

if __name__ == "__main__":
    app()
