"""example/dry_run/my_flow.py — git-delivered ML payload, Prefect dry run.

Validates workflow wiring only: each @task and config variable stands in for the
real example/ file (train_prepare.py, train_featurize.py, train.py, validate.py,
test_prepare.py, test_featurize.py, test.py, parity_plot.py, prepare.json,
optuna.json). No real ML — every stage just records that it ran, while
train_prepare also counts the files under --data_folder and logs that to the
Prefect run log + MLflow, so `python my_flow.py --data_folder <dir>` walks the
whole prepare -> featurize -> train -> validate / test path end to end.

Run by pipeline.py (orchestrator, prefect.md §4.3):
    python my_flow.py --submitter <m> --data_folder <dir>

Local debugging — run ephemerally with no Prefect server (MLflow tracking also skipped):
    python my_flow.py --run-on local --data_folder <dir>
"""
__version__ = "0.0.19"

import argparse
import os
from pathlib import Path
from typing import Any, Dict, Literal

import mlflow
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact

State = Dict[str, str]                            # per-stage status map (stage -> "ok") threaded through the flow
Stages = Literal["", "train_prepare", "train_featurize", "train", "validate",
                 "test_prepare", "test_featurize", "test"]  # pipeline stage names

prepare_json: Dict[str, Any] = {
    "__version__": "0.0.0",
    "train": {
        "split": [0.8, 0.2]
    },
    "validate": {
    },
    "test": {
    }
}  # stand-in for prepare.json (free-form / nested)
optuna_json: Dict[str, Any] = {
    "__version__": "0.0.0",
    "environment": {
    },
    "search_space": {
    }
}  # stand-in for optuna.json

STAGES = ("train_prepare", "train_featurize", "train", "validate",
          "test_prepare", "test_featurize", "test")


@task(task_run_name="train_prepare", retries=2, retry_delay_seconds=5)
def train_prepare(state: State, data_folder: str, prepare_json: Dict[str, Any]) -> State:
    log = get_run_logger()
    data = Path(data_folder)
    n_files = sum(1 for p in data.rglob("*") if p.is_file()) if data.exists() else 0
    log.info(f"train_prepare: {n_files} files under {data_folder}")   # Prefect run log
    log.info(f"train_prepare: prepare_json = {prepare_json}")
    mlflow.log_metric("n_data_files", n_files)                        # MLflow (metric -> tracking store)
    mlflow.log_param("train_prepare.prepare_json", prepare_json)      # MLflow (config visible in the run)
    return {**state, "train_prepare": "ok"}


@task(task_run_name="train_featurize", retries=2, retry_delay_seconds=5)
def train_featurize(state: State) -> State:
    return {**state, "train_featurize": "ok"}


@task(task_run_name="train", retries=2, retry_delay_seconds=5)
def train(state: State, optuna_json: Dict[str, Any]) -> State:
    log = get_run_logger()
    log.info(f"train: optuna_json = {optuna_json}")                  # Prefect run log
    mlflow.log_param("train.optuna_json", optuna_json)               # MLflow (config visible in the run)
    return {**state, "train": "ok"}


@task(task_run_name="validate", retries=2, retry_delay_seconds=5)
def validate(state: State) -> State:
    return {**state, "validate": "ok"}


@task(task_run_name="test_prepare", retries=2, retry_delay_seconds=5)
def test_prepare(state: State, prepare_json: Dict[str, Any]) -> State:
    log = get_run_logger()
    log.info(f"test_prepare: prepare_json = {prepare_json}")         # Prefect run log
    mlflow.log_param("test_prepare.prepare_json", prepare_json)      # MLflow (config visible in the run)
    return {**state, "test_prepare": "ok"}


@task(task_run_name="test_featurize", retries=2, retry_delay_seconds=5)
def test_featurize(state: State) -> State:
    return {**state, "test_featurize": "ok"}


@task(task_run_name="test", retries=2, retry_delay_seconds=5)
def test(state: State) -> State:
    return {**state, "test": "ok"}


# report tasks — submitted concurrently after train/validate/test; MLflow is logged in the flow
# (main thread) since a .submit() task runs in another thread where the active MLflow run is not set.
@task(task_run_name="parity_plot-{stage}", retries=2, retry_delay_seconds=5)
def parity_plot(state: State, stage: Stages = "") -> str:
    get_run_logger().info(f"parity_plot after {stage} ({len(state)} stages so far)")
    return f"parity_plot.{stage}"


@task(task_run_name="publish_artifacts-{stage}", retries=2, retry_delay_seconds=5)
def publish_artifacts(state: State, stage: Stages = "") -> str:
    log = get_run_logger()
    log.info(f"publish_artifacts: {stage} result = {state}")         # Prefect run log
    try:
        create_markdown_artifact(key=f"result-{stage}",
                                 markdown=f"# {stage} result\n\n`{state}`")   # Prefect UI artifact
    except Exception as e:                                           # no Prefect API backend -> skip
        log.warning(f"artifact skipped: {e}")
    return f"publish_artifacts.{stage}"


@flow(name="my_flow", flow_run_name="{submitter}", log_prints=True)
def my_flow(*, submitter: str = "local", data_folder: str = "./data") -> State:
    log = get_run_logger()
    log.info(f"dry run: submitter={submitter} "
             f"data={data_folder} prepare={prepare_json} optuna={optuna_json}")

    reports = []
    # point MLflow at the tracking server, else it logs to a local ./mlruns and never
    # reaches the dashboard. container: service name http://mlflow:5000; host: set
    # MLFLOW_TRACKING_URI (e.g. http://localhost:5000). --run-on local -> no-op shim.
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    mlflow.set_experiment("dry_run")                 # named experiment (else lands in "Default")
    with mlflow.start_run(run_name=f"{submitter}"):  # -> experiment "dry_run" on the MLflow server
        s = train_prepare({}, data_folder, prepare_json)           # train branch
        s = train_featurize(s)
        s = train(s, optuna_json)
        reports += [parity_plot.submit(s, "train"), publish_artifacts.submit(s, "train")]
        s = validate(s)
        reports += [parity_plot.submit(s, "validate"), publish_artifacts.submit(s, "validate")]
        s = test_prepare(s, prepare_json)                          # test branch
        s = test_featurize(s)
        s = test(s)
        reports += [parity_plot.submit(s, "test"), publish_artifacts.submit(s, "test")]

        published = sorted(f.result() for f in reports)            # resolve report futures (raise on failure)
        for key in published:
            mlflow.log_param(key, "ok")                            # MLflow: parity_plot.* / publish_artifacts.*

    ran = sorted(s)
    assert set(ran) == set(STAGES), f"missing stages: {set(STAGES) - set(ran)}"
    log.info(f"dry run ok: stages={ran} reports={published}")
    return s


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--submitter", default="local")
    p.add_argument("--data_folder", default="./data")
    # optional (default server) so pipeline.py's pool call (no --run-on) is unaffected;
    # pass --run-on local for standalone debugging with no Prefect server.
    p.add_argument("--run-on", choices=["local", "server"], default="server",
                   help="local: run ephemerally with no server (local debugging); "
                        "server: record the run on the Prefect server (PREFECT_API_URL)")
    return p.parse_args()


class _NoOpMLflow:
    """No-op MLflow stand-in for --run-on local: skips all tracking calls.
    start_run() returns a null context; log_metric / log_param / ... become no-ops."""
    def start_run(self, *args, **kwargs):
        from contextlib import nullcontext
        return nullcontext()

    def __getattr__(self, _name):
        return lambda *args, **kwargs: None


if __name__ == "__main__":
    a = parse_args()
    if a.run_on == "local":                                 # local debug: no Prefect server, no MLflow
        import logging
        logging.getLogger("prefect._internal.concurrency").setLevel(logging.CRITICAL)  # mute ephemeral EventsWorker noise
        mlflow = _NoOpMLflow()                              # rebind module global -> every mlflow.* call is a no-op
        from prefect.settings import PREFECT_API_URL, temporary_settings
        with temporary_settings({PREFECT_API_URL: ""}):     # disable PREFECT_API_URL -> ephemeral run
            my_flow(submitter=a.submitter, data_folder=a.data_folder)
    else:                                                   # use the configured Prefect + MLflow servers
        my_flow(submitter=a.submitter, data_folder=a.data_folder)
