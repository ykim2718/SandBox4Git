"""my_flow.py - Kaggle Electric Power Consumption flow (entrypoint).

Predicts a Tetouan-city power-consumption zone (a continuous / regression target) from
weather + calendar features with LightGBM. Run as the team payload that pipeline.py drives:

    python my_flow.py --submitter <m> --data_folder ./data   (pipeline.py passes --submitter/--data_folder)

Pipeline (a small DAG): load_config -> train_prepare -> train_featurize -> train ->
(validate || test); parity_plot AND publish_artifacts both fire right after each of
train / validate / test. The raw data is one CSV (powerconsumption.csv, 10-min samples over
2017), so the only split the code makes is temporal: train_prepare slices off the most recent
test_fraction as the test lane and carves a contiguous validation tail from the rest;
train_featurize fits per-feature 0-1 scaling on the training rows (saved as scaler.json, which
test reuses), derives calendar features, and applies the split. train runs Optuna over
LGBMRegressor and reports the CV-RMSE + train-set RMSE/MAE/R2; validate scores the held-out tail;
test scores the most-recent slice and writes powerconsumption-test-pred.csv.

Prefect features exercised: @flow + @task, flow_run_name / task_run_name templating,
tags, retries, log_prints, get_run_logger, ThreadPoolTaskRunner with .submit()
futures + wait_for for the DAG, runtime context, and markdown/table artifacts.
Optuna trials are logged to a PostgreSQL study so they can be viewed in optuna-dashboard.
"""
import argparse
import copy
import json
import os
import re
from pathlib import Path
from typing import Any, Optional, Tuple, Union

import numpy as np
import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.runtime import flow_run
from prefect.task_runners import ThreadPoolTaskRunner

__version__ = "0.0.16"

HERE: Path = Path(__file__).resolve().parent
OPTUNA_JSON: Path = HERE / "optuna.json"
PREPARE_JSON: Path = HERE / "prepare.json"
CSV_NAME: str = "powerconsumption.csv"

# The 5 weather inputs - the only columns that get 0-1 scaled (calendar features stay raw).
WEATHER = ["Temperature", "Humidity", "WindSpeed", "GeneralDiffuseFlows", "DiffuseFlows"]
ZONE_COL = {"Zone1": "PowerConsumption_Zone1",
            "Zone2": "PowerConsumption_Zone2",
            "Zone3": "PowerConsumption_Zone3"}
# accept the UCI header spellings too, mapped onto the Kaggle schema this flow expects
_ALIASES = {
    "wind speed": "WindSpeed", "general diffuse flows": "GeneralDiffuseFlows",
    "diffuse flows": "DiffuseFlows", "zone 1 power consumption": "PowerConsumption_Zone1",
    "zone 2 power consumption": "PowerConsumption_Zone2",
    "zone 3 power consumption": "PowerConsumption_Zone3", "datetime": "Datetime",
}

# Fallback defaults; the authoritative values live in optuna.json `search_space` (per slot,
# type/range/step/log/init) and `environment` (run-level). optuna.json overrides these, so the
# flow still runs if a key is missing there. SEARCH_SPACE is the source of type/step/log.
SEARCH_SPACE = {
    "n_estimators":      {"type": "int",   "range": [200, 1200], "step": 100},
    "learning_rate":     {"type": "float", "range": [0.01, 0.2], "log": True},
    "num_leaves":        {"type": "int",   "range": [15, 255]},
    "max_depth":         {"type": "int",   "range": [3, 12]},
    "min_child_samples": {"type": "int",   "range": [5, 80]},
    "subsample":         {"type": "float", "range": [0.6, 1.0]},
    "colsample_bytree":  {"type": "float", "range": [0.6, 1.0]},
    "reg_alpha":         {"type": "float", "range": [1e-3, 10.0], "log": True},
    "reg_lambda":        {"type": "float", "range": [1e-3, 10.0], "log": True},
}

# Run-level fallback defaults; optuna.json `environment` overrides each matching key.
RUN_DEFAULTS = {
    "n_trials": 20, "direction": "minimize", "metric": "rmse", "cv_folds": 5,
    "target_zone": "Zone1", "random_state": 42, "storage": None, "mlflow_uri": None,
    "study_name": "epc_power",
    "lgbm_fixed": {"objective": "regression", "metric": "rmse", "verbosity": -1, "n_jobs": -1},
}

# Data-prepare fallback defaults; prepare.json overrides each key. These drive the temporal
# split in train_prepare / test_prepare (how much data, where the test / validation cuts fall).
PREPARE_DEFAULTS = {"sample_rows": None, "test_fraction": 0.2, "val_fraction": 0.2}


# config: read fresh each run so edits to optuna.json / prepare.json always take effect
@task(name="load_config", task_run_name="load_config", log_prints=True)
def load_config_json(optuna_cfg: Union[str, Path], prepare_cfg: Union[str, Path]) -> dict:
    """Merge optuna.json + prepare.json over the code fallbacks into one `cfg` dict.

    optuna.json has three keys: `__version__` (doc), `environment` (run-level config that
    overrides RUN_DEFAULTS), and `search_space` (per-slot {type, range, step, log, init} that
    overrides SEARCH_SPACE; `init` becomes the warm-start anchor enqueued as trial 0). A slot for
    a name not in SEARCH_SPACE is reported and ignored. prepare.json holds the data-split settings
    (`sample_rows`, `test_fraction`, `val_fraction`) that override PREPARE_DEFAULTS. Returns the
    merged run config flattened to the top level, plus `search_space` and `warm_start`.
    """
    raw = json.loads(Path(optuna_cfg).read_text(encoding="utf-8"))
    run = copy.deepcopy(RUN_DEFAULTS)
    run.update(raw.get("environment", {}))                   # environment overrides run defaults
    space = copy.deepcopy(SEARCH_SPACE)
    warm_start = {}
    for name, slot in (raw.get("search_space") or {}).items():
        if name not in space:                                # slot for an unknown hyperparameter
            print(f"config: search_space slot '{name}' not in SEARCH_SPACE - reported and ignored")
            continue
        space[name].update({k: v for k, v in slot.items() if k != "init"})   # type/range/step/log
        if "init" in slot:
            warm_start[name] = slot["init"]                  # warm-start anchor (enqueued as trial 0)

    prep_raw = json.loads(Path(prepare_cfg).read_text(encoding="utf-8"))
    prep = copy.deepcopy(PREPARE_DEFAULTS)
    prep.update({k: v for k, v in prep_raw.items() if not k.startswith("_")})   # skip __version__

    out = dict(run)
    out.update(prep)                                         # sample_rows / test_fraction / val_fraction
    out["search_space"] = space
    out["warm_start"] = warm_start
    print(f"optuna config v{raw.get('__version__', '?')} + prepare config "
          f"v{prep_raw.get('__version__', '?')}: {len(space)} search slots, "
          f"{len(warm_start)} warm-start init(s); n_trials={out['n_trials']}, "
          f"target={out['target_zone']}, sample_rows={out['sample_rows']}, "
          f"test_fraction={out['test_fraction']}, val_fraction={out['val_fraction']}")
    return out


def _mask(dsn: str) -> str:
    """Hide the password in a DSN before logging it."""
    return re.sub(r"(://[^:/@]+:)[^@]*(@)", r"\1***\2", dsn)


def _optuna_storage(cfg: dict) -> Tuple[str, str]:
    """Optuna study storage. Priority: cfg['storage'] > POSTGRESQL_OPTUNA_DSN env > local sqlite.

    pipeline.py bridges the MLflow tracking URI to the payload but NOT the optuna DSN (and it no
    longer passes a block name), so for a shared postgres study set POSTGRESQL_OPTUNA_DSN in the
    base job template env; otherwise this falls back to a per-run local sqlite file so the flow
    still runs.
    """
    override = cfg.get("storage")
    if override:
        return override, "config"
    dsn = os.environ.get("POSTGRESQL_OPTUNA_DSN")
    if dsn:
        return dsn, "env (POSTGRESQL_OPTUNA_DSN)"
    return "sqlite:///optuna.db", "local sqlite (fallback)"


def _mlflow_start(uri: str, experiment: str, run_name: str) -> Optional[Any]:
    """Best-effort MLflow run: set tracking URI + experiment, start a run, and return the
    mlflow module - or None if the server is unreachable (so a local dry run never fails on it).
    The caller logs per-trial metrics through the returned module, then calls end_run()."""
    if not uri:
        return None
    try:
        import mlflow
        mlflow.set_tracking_uri(uri)
        mlflow.set_experiment(experiment)
        mlflow.start_run(run_name=run_name or None)
        print(f"MLflow logging to {uri} (experiment={experiment})")
        return mlflow
    except Exception as e:                                    # server down / mlflow missing -> skip
        print(f"MLflow disabled (cannot use {uri}): {e}")
        return None


def _read_series(data_dir: Union[str, Path]) -> pd.DataFrame:
    """Read powerconsumption.csv into a clean, time-sorted frame (Kaggle or UCI header spelling)."""
    path = Path(data_dir) / CSV_NAME
    if not path.exists():
        raise FileNotFoundError(
            f"missing {CSV_NAME} in {data_dir}. Download the Kaggle Electric Power Consumption "
            "CSV (or `catalog.py download electric_power_consumption`) and place it there.")
    df = pd.read_csv(path)
    df = df.rename(columns={c: _ALIASES.get(c.strip().lower(), c.strip()) for c in df.columns})
    df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")
    for c in WEATHER + list(ZONE_COL.values()):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna(subset=["Datetime"]).sort_values("Datetime").reset_index(drop=True)


def _bounds(df: pd.DataFrame, cfg: dict) -> Tuple[pd.DataFrame, int]:
    """Apply the optional recent-tail sample and compute the train+val / test boundary index.

    Returns (df_used, test_start) where test_start is the row index where the test lane begins.
    Both lanes call this on the same CSV, so they agree on the split with no shared state.
    """
    n_sample = cfg.get("sample_rows")
    if n_sample and n_sample < len(df):
        df = df.tail(int(n_sample)).reset_index(drop=True)       # keep the most recent rows
    test_start = int(len(df) * (1 - cfg.get("test_fraction", 0.2)))
    return df, test_start


@task(name="train_prepare", task_run_name="train_prepare", tags=["epc", "dp"],
      retries=2, retry_delay_seconds=2, log_prints=True)
def train_prepare(data_dir: Union[str, Path], work: Union[str, Path], cfg: dict) -> dict:
    """Read the CSV, slice off the recent test span, and carve a contiguous validation tail.

    The split is temporal (no shuffle - it is a time series): the most recent test_fraction is
    the test lane, the val_fraction just before it is held-out validation, the rest is train.
    """
    df, test_start = _bounds(_read_series(data_dir), cfg)
    trainval = df.iloc[:test_start].copy()                       # everything before the test span
    if trainval.empty:
        raise ValueError("no training rows after the split - lower test_fraction / sample_rows.")

    # validation = the contiguous tail of train+val (so it sits right before the test span)
    val_cut = int(len(trainval) * (1 - cfg.get("val_fraction", 0.2)))
    val_start = trainval["Datetime"].iloc[val_cut]

    work_dir = Path(work)
    work_dir.mkdir(parents=True, exist_ok=True)
    trainval.to_parquet(work_dir / "trainval_raw.parquet")
    n_tr, n_va = val_cut, len(trainval) - val_cut
    print(f"prepared {len(trainval)} rows; split {n_tr}/{n_va} (train/val), val starts {val_start}")
    return {"work": str(work), "val_start": val_start.isoformat()}   # cut passed to train_featurize


def _scale(df: pd.DataFrame, scaler: dict = None) -> Tuple[pd.DataFrame, dict]:
    """Per-feature 0-1 (min-max) scaling for the 5 weather columns. Fit when `scaler` is None
    (training) and return the {feature: [min, max]} map; otherwise apply the supplied map (test)
    so train and test share one scale. Calendar features are added later and stay raw."""
    df = df.copy()
    if scaler is None:
        scaler = {v: [float(df[v].min()), float(df[v].max())] for v in WEATHER}
    for v in WEATHER:
        lo, hi = scaler[v]
        rng = hi - lo
        df[v] = (df[v] - lo) / rng if rng else 0.0
    return df, scaler


def _features(df: pd.DataFrame) -> pd.DataFrame:
    """Derive calendar features from Datetime and return weather (already scaled) + calendar.

    14 features: 5 weather + 3 calendar (hour, dayofweek, is_weekend) + 6 cyclical (sin/cos of
    hour, dayofweek, month) - daily / weekly / yearly seasonality. Every feature is bounded and
    repeats each cycle, so none drifts out of the training range when the test span is in the
    future. Raw monotonic indices (day, month, dayofyear) are deliberately left out: a tree
    cannot extrapolate past its training range, so a future-only test value just pins to the
    edge split - the cyclical sin/cos carry the same seasonality without that failure mode.
    """
    dt = df["Datetime"].dt
    out = df[WEATHER].copy()
    out["hour"] = dt.hour
    out["dayofweek"] = dt.dayofweek
    out["is_weekend"] = (dt.dayofweek >= 5).astype(int)
    two_pi = 2 * np.pi
    out["hour_sin"] = np.sin(two_pi * dt.hour / 24)
    out["hour_cos"] = np.cos(two_pi * dt.hour / 24)
    out["dow_sin"] = np.sin(two_pi * dt.dayofweek / 7)
    out["dow_cos"] = np.cos(two_pi * dt.dayofweek / 7)
    out["month_sin"] = np.sin(two_pi * (dt.month - 1) / 12)
    out["month_cos"] = np.cos(two_pi * (dt.month - 1) / 12)
    return out


@task(name="train_featurize", task_run_name="train_featurize", tags=["epc", "dp", "fe"],
      retries=2, retry_delay_seconds=2, log_prints=True)
def train_featurize(prep: dict, cfg: dict) -> dict:
    """Preprocess + feature engineering: fit per-feature 0-1 scaling on the training rows only
    (saved as scaler.json, which test reuses), derive calendar features, attach the target, and
    split train/val on the temporal cut from train_prepare."""
    work_dir = Path(prep["work"])
    df = pd.read_parquet(work_dir / "trainval_raw.parquet")
    val_start = pd.Timestamp(prep["val_start"])                  # the train/val cut from train_prepare
    target = ZONE_COL[cfg.get("target_zone", "Zone1")]

    is_train = df["Datetime"] < val_start
    _, scaler = _scale(df[is_train])                             # fit 0-1 scaling on train rows only
    (work_dir / "scaler.json").write_text(json.dumps(scaler), encoding="utf-8")  # reused at test time
    scaled, _ = _scale(df, scaler)                               # apply to the whole train+val span
    feat = _features(scaled)
    feat["y"] = df[target].to_numpy()
    feat["Datetime"] = df["Datetime"].to_numpy()
    feat = feat.dropna(subset=["y"])

    tr = feat[feat["Datetime"] < val_start]
    va = feat[feat["Datetime"] >= val_start]
    feat_cols = [c for c in feat.columns if c not in ("y", "Datetime")]
    tr.to_parquet(work_dir / "train.parquet")
    va.to_parquet(work_dir / "val.parquet")
    (work_dir / "features.json").write_text(json.dumps(feat_cols), encoding="utf-8")
    print(f"built {len(feat)} rows x {len(feat_cols)} features (target {target}); "
          f"train={len(tr)} val={len(va)}")
    return {"work": str(work_dir), "n_features": len(feat_cols),
            "n_train": int(len(tr)), "n_val": int(len(va)), "target": target,
            "target_mean": float(feat["y"].mean()), "target_std": float(feat["y"].std())}


@task(name="train", task_run_name="train", tags=["epc", "model"],
      retries=1, retry_delay_seconds=2, log_prints=True)
def train(prep: dict, cfg: dict, storage: str,
          mlflow_uri: str = "", run_name: str = "") -> dict:
    """Optuna search over LGBMRegressor, CV-RMSE on the training rows; refit best.

    `storage` is the postgresql_optuna DSN (trials are logged there for optuna-dashboard).
    If `mlflow_uri` is reachable, each trial's CV-RMSE is also logged to MLflow as a step,
    so the MLflow UI shows a per-trial metric curve (best-effort; skipped if MLflow is down).
    The inner tuning CV is plain k-fold (the temporal evaluation lives in validate / test).
    """
    import optuna
    from lightgbm import LGBMRegressor
    from sklearn.model_selection import cross_val_score
    from tqdm import tqdm

    work_dir = Path(prep["work"])
    tr = pd.read_parquet(work_dir / "train.parquet")
    feat_cols = json.loads((work_dir / "features.json").read_text(encoding="utf-8"))
    x, y = tr[feat_cols], tr["y"]
    fixed = cfg.get("lgbm_fixed", {})
    seed = cfg.get("random_state", 42)
    folds = cfg.get("cv_folds", 5)
    study_name = cfg.get("study_name", "epc_power")
    space = cfg["search_space"]                              # effective ranges (SEARCH_SPACE + overrides)
    n_trials = int(cfg.get("n_trials", 20))

    def suggest(trial, name: str, spec: dict) -> Union[int, float, str]:   # one slot -> one suggestion
        lo, hi = spec["range"][0], spec["range"][1] if len(spec["range"]) > 1 else spec["range"][0]
        kind = spec.get("type", "float")
        if kind == "int":
            return trial.suggest_int(name, int(lo), int(hi), step=spec.get("step", 1))
        if kind == "categorical":
            return trial.suggest_categorical(name, spec["range"])
        return trial.suggest_float(name, lo, hi, log=spec.get("log", False))

    def objective(trial) -> float:
        params = {name: suggest(trial, name, spec) for name, spec in space.items()}
        params.update(random_state=seed, **fixed)
        model = LGBMRegressor(**params)
        score = cross_val_score(model, x, y, cv=folds,
                                scoring="neg_root_mean_squared_error")
        return -score.mean()

    mlf = _mlflow_start(mlflow_uri, study_name, run_name)     # None if MLflow is unreachable

    def log_trial(study, trial) -> None:                     # Optuna runs this after each trial
        print(f"trial {trial.number}: cv_rmse={trial.value:.4f} best={study.best_value:.4f}")
        if mlf:                                              # step=trial number -> a metric curve
            mlf.log_metric("cv_rmse", float(trial.value), step=trial.number)
            mlf.log_metric("best_cv_rmse", float(study.best_value), step=trial.number)

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(direction=cfg.get("direction", "minimize"),
                                sampler=optuna.samplers.TPESampler(seed=seed),
                                storage=storage, study_name=study_name,
                                load_if_exists=True)
    warm = {k: v for k, v in (cfg.get("warm_start") or {}).items() if k in space}
    if warm:                                                 # trial 0 reproduces the warm-start anchor
        study.enqueue_trial(warm, skip_if_exists=True)
        print(f"warm-start enqueued ({len(warm)} params): {warm}")
    for _ in tqdm(range(n_trials), desc="optuna trials", unit="trial"):   # one trial per step -> tqdm bar
        study.optimize(objective, n_trials=1, callbacks=[log_trial])
    print(f"best CV RMSE={study.best_value:.4f} params={study.best_params}")
    print(f"optuna study '{study_name}' -> {_mask(storage)}  (view: optuna-dashboard <dsn>)")

    best = LGBMRegressor(random_state=seed, **fixed, **study.best_params)
    best.fit(x, y)
    model_path = work_dir / "model.txt"
    best.booster_.save_model(str(model_path))
    imp = sorted(zip(feat_cols, best.booster_.feature_importance(importance_type="gain")),
                 key=lambda t: t[1], reverse=True)
    # train-set predictions -> train metrics (so train returns the model AND its metrics)
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    train_pred = best.predict(x)
    yt = y.to_numpy()
    train_rmse = float(np.sqrt(mean_squared_error(yt, train_pred)))
    train_mae = float(mean_absolute_error(yt, train_pred))
    train_r2 = float(r2_score(yt, train_pred))
    print(f"train RMSE={train_rmse:.4f} MAE={train_mae:.4f} R2={train_r2:.4f} "
          f"(best CV RMSE={study.best_value:.4f})")
    if mlf:                                                  # final best params/score, then close the run
        mlf.log_params(study.best_params)
        mlf.log_metric("final_cv_rmse", float(study.best_value))
        mlf.log_metric("train_rmse", train_rmse)
        mlf.end_run()
    return {"work": str(work_dir), "model_path": str(model_path),
            "best_params": study.best_params, "best_cv_rmse": float(study.best_value),
            "train_rmse": train_rmse, "train_mae": train_mae, "train_r2": train_r2,
            "top_features": [{"feature": f, "gain": float(g)} for f, g in imp[:15]],
            "train_true": y.tolist(), "train_pred": [float(p) for p in train_pred]}


@task(name="validate", task_run_name="validate", tags=["epc", "eval"],
      retries=2, retry_delay_seconds=2, log_prints=True)
def validate(trained: dict, prep: dict) -> dict:
    """Score the held-out validation tail (val.parquet): RMSE, MAE, R2."""
    from lightgbm import Booster
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    work_dir = Path(trained["work"])
    va = pd.read_parquet(work_dir / "val.parquet")
    feat_cols = json.loads((work_dir / "features.json").read_text(encoding="utf-8"))
    booster = Booster(model_file=trained["model_path"])
    pred = booster.predict(va[feat_cols])
    y = va["y"].to_numpy()
    rmse = float(np.sqrt(mean_squared_error(y, pred)))
    mae = float(mean_absolute_error(y, pred))
    r2 = float(r2_score(y, pred))
    print(f"val RMSE={rmse:.4f} MAE={mae:.4f} R2={r2:.4f}")
    return {"val_rmse": rmse, "val_mae": mae, "val_r2": r2,
            "val_true": y.tolist(), "val_pred": [float(p) for p in pred]}


@task(name="test_prepare", task_run_name="test_prepare", tags=["epc", "dp"],
      retries=2, retry_delay_seconds=2, log_prints=True)
def test_prepare(data_dir: Union[str, Path], work: Union[str, Path], cfg: dict) -> dict:
    """Test-side prepare: slice the most-recent test span off the same CSV and save it raw.
    The test counterpart of train_prepare (no fit, no split - the test span is fixed by config)."""
    df, test_start = _bounds(_read_series(data_dir), cfg)
    test = df.iloc[test_start:].copy()
    if test.empty:
        print("no test rows after the split - skipping the test lane")
        return {"work": str(work), "has_test": False}
    work_dir = Path(work)
    work_dir.mkdir(parents=True, exist_ok=True)
    test.to_parquet(work_dir / "test_raw.parquet")
    print(f"prepared {len(test)} test rows (from {df['Datetime'].iloc[test_start]})")
    return {"work": str(work), "has_test": True}


@task(name="test_featurize", task_run_name="test_featurize", tags=["epc", "dp", "fe"],
      retries=2, retry_delay_seconds=2, log_prints=True)
def test_featurize(tprep: dict, cfg: dict) -> dict:
    """Test-side featurize: apply the training scaler.json + features.json schema to the test
    span (no fit, no split) and save test.parquet. Mirrors train_featurize for test."""
    work_dir = Path(tprep["work"])
    if not tprep.get("has_test"):
        return {"work": str(work_dir), "has_test": False}
    scaler = json.loads((work_dir / "scaler.json").read_text(encoding="utf-8"))   # training 0-1 min/max
    feat_cols = json.loads((work_dir / "features.json").read_text(encoding="utf-8"))
    df = pd.read_parquet(work_dir / "test_raw.parquet")
    target = ZONE_COL[cfg.get("target_zone", "Zone1")]
    scaled, _ = _scale(df, scaler)                           # preprocessing: apply the training scale
    feat = _features(scaled)                                 # feature engineering
    feat["y"] = df[target].to_numpy()
    feat["Datetime"] = df["Datetime"].to_numpy()
    for c in feat_cols:                                      # align to the training feature schema
        if c not in feat.columns:
            feat[c] = np.nan
    feat[feat_cols + ["y", "Datetime"]].to_parquet(work_dir / "test.parquet")
    print(f"built {len(feat)} test rows x {len(feat_cols)} features (target {target})")
    return {"work": str(work_dir), "has_test": True}


@task(name="test", task_run_name="test", tags=["epc", "infer"],
      retries=1, retry_delay_seconds=2, log_prints=True)
def test(trained: dict, tfz: dict, data_dir: Union[str, Path]) -> dict:
    """Predict the prepared test span, write powerconsumption-test-pred.csv, and score it
    (the single dataset always carries the true target, so the test slice is always scored)."""
    from lightgbm import Booster
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    work_dir = Path(trained["work"])
    if not tfz.get("has_test"):
        print("no test set - skipping test prediction")
        return {"test_predicted": 0, "submission": None,
                "test_rmse": None, "test_mae": None, "test_r2": None}
    feat_cols = json.loads((work_dir / "features.json").read_text(encoding="utf-8"))
    feat = pd.read_parquet(work_dir / "test.parquet")
    booster = Booster(model_file=trained["model_path"])
    feat["y_pred"] = booster.predict(feat[feat_cols])
    out = feat[["Datetime", "y", "y_pred"]].rename(columns={"y": "y_true"})
    sub = work_dir / "powerconsumption-test-pred.csv"
    out.to_csv(sub, index=False)
    print(f"wrote {len(out)} predictions -> {sub}")

    result = {"test_predicted": int(len(out)), "submission": str(sub),
              "test_rmse": None, "test_mae": None, "test_r2": None}
    m = out.dropna(subset=["y_true", "y_pred"])
    if len(m):
        yt, yp = m["y_true"], m["y_pred"]
        result["test_rmse"] = float(np.sqrt(mean_squared_error(yt, yp)))
        result["test_mae"] = float(mean_absolute_error(yt, yp))
        result["test_r2"] = float(r2_score(yt, yp))
        print(f"test (n={len(m)}) RMSE={result['test_rmse']:.4f} "
              f"MAE={result['test_mae']:.4f} R2={result['test_r2']:.4f}")
        result["test_true"] = [float(v) for v in yt]
        result["test_pred"] = [float(v) for v in yp]
    return result


@task(name="parity_plot", task_run_name="parity_plot ({stage})", tags=["epc", "viz"], log_prints=True)
def parity_plot(y_true: list, y_pred: list, stage: str, work: Union[str, Path],
                target: str = "power") -> dict:
    """Save a y_true vs y_pred 1:1 parity chart for `stage` (train / validation / test); the
    stage is the chart title. Uses the thread-safe Figure API (no pyplot global state) so the
    three per-stage plots run concurrently. Best-effort: also attaches it to the Prefect UI."""
    if not y_true or not y_pred:
        print(f"parity_plot[{stage}]: no data - skipped")
        return {"stage": stage, "path": None}
    from matplotlib.figure import Figure
    from sklearn.metrics import r2_score

    yt = np.asarray(y_true, dtype=float)
    yp = np.asarray(y_pred, dtype=float)
    r2 = float(r2_score(yt, yp))
    lo, hi = float(min(yt.min(), yp.min())), float(max(yt.max(), yp.max()))
    fig = Figure(figsize=(5, 5))
    ax = fig.subplots()
    ax.scatter(yt, yp, s=10, alpha=0.3, edgecolor="none")
    ax.plot([lo, hi], [lo, hi], "r--", lw=1, label="y = x")
    ax.set(xlabel=f"y_true ({target})", ylabel="y_pred",
           title=f"{stage} - parity  (n={len(yt)}, R2={r2:.3f})")
    ax.set_aspect("equal", "box")
    ax.legend(loc="upper left", fontsize=8)
    fig.tight_layout()
    out = Path(work) / f"parity_{stage}.png"
    fig.savefig(out, dpi=110)
    print(f"parity_plot[{stage}] -> {out}  (R2={r2:.3f})")
    try:                                                     # best-effort: embed in the Prefect UI
        import base64
        b64 = base64.b64encode(out.read_bytes()).decode()
        create_markdown_artifact(
            key=f"epc-power-parity-{stage}",
            markdown=f"### {stage} - parity (R2={r2:.3f})\n\n![parity](data:image/png;base64,{b64})",
            description=f"y_true vs y_pred parity plot ({stage})")
    except Exception as e:
        print(f"parity artifact skipped: {e}")
    return {"stage": stage, "path": str(out), "r2": r2}


@task(name="publish_artifacts", task_run_name="publish_artifacts ({stage})", tags=["epc"], log_prints=True)
def publish_artifacts(stage: str, metrics: dict, run_label: str,
                      top_features: Optional[list] = None) -> None:
    """Attach this stage's metrics to the Prefect UI right after the stage finishes - one set per
    stage, mirroring parity_plot. For train, also publish the top-feature table. Best-effort: a
    pure-local run with no API backend just skips."""
    try:
        rows = [{"metric": k, "value": round(v, 4) if isinstance(v, float) else v}
                for k, v in metrics.items() if v is not None]
        create_table_artifact(key=f"epc-power-metrics-{stage}", table=rows,
                              description=f"Electric Power Consumption {stage} metrics - {run_label}")
        if top_features:
            create_table_artifact(key="epc-power-top-features", table=top_features,
                                  description=f"LightGBM top features by gain - {run_label}")
        md = (f"### EPC power - {stage}  (`{run_label}`, run `{flow_run.id}`)\n\n"
              + "\n".join(f"- {r['metric']}: **{r['value']}**" for r in rows))
        create_markdown_artifact(key=f"epc-power-summary-{stage}", markdown=md,
                                 description=f"EPC power {stage} summary")
        print(f"published {stage} artifacts: {len(rows)} metrics")
    except Exception as e:                                   # no API backend (pure local) -> skip artifacts
        get_run_logger().warning(f"artifact publish skipped ({stage}): {e}")


@flow(name="epc_power", flow_run_name="{submitter}", log_prints=True,
      task_runner=ThreadPoolTaskRunner(max_workers=4))
def my_flow(data_dir: Union[str, Path], submitter: str = "local",
            sample_rows: Optional[int] = None) -> dict:
    """Electric Power Consumption regression: train_prepare -> train_featurize -> train ->
    (validate || test), parity after each. `sample_rows` (when given) overrides the config -
    a fast smoke test on the most-recent N rows; leave it None to use optuna.json."""
    log = get_run_logger()
    work = str(HERE / "work")
    Path(work).mkdir(parents=True, exist_ok=True)
    log.info(f"start: submitter={submitter} data={data_dir}")

    cfg = load_config_json(OPTUNA_JSON, PREPARE_JSON)        # read fresh each run
    if sample_rows is not None:                             # CLI override for a fast smoke test
        cfg["sample_rows"] = sample_rows
        log.info(f"sample_rows overridden from CLI: {sample_rows} (most-recent rows)")
    log.info(f"tuning {cfg['n_trials']} trials, metric={cfg['metric']}, target={cfg.get('target_zone', 'Zone1')}")

    storage, src = _optuna_storage(cfg)                     # cfg['storage'] > POSTGRESQL_OPTUNA_DSN env > sqlite
    log.info(f"optuna storage [{src}]: {_mask(storage)}")

    # MLflow server: pipeline.py bridges the block's mlflow endpoint as MLFLOW_TRACKING_URI (then fallbacks).
    mlflow_uri = os.environ.get("MLFLOW_TRACKING_URI") or cfg.get("mlflow_uri") or "http://mlflow:5000"
    run_name = f"{submitter}"
    log.info(f"mlflow uri: {mlflow_uri}")

    prep = train_prepare.submit(data_dir, work, cfg)
    fz = train_featurize.submit(prep, cfg, wait_for=[prep])   # 0-1 scaling + calendar features + split
    tr = train.submit(fz, cfg, storage, mlflow_uri, run_name, wait_for=[fz])

    # test lane: its own test_prepare + test_featurize, mirroring the training lane - concurrent
    tprep = test_prepare.submit(data_dir, work, cfg)
    tfz = test_featurize.submit(tprep, cfg, wait_for=[tprep, fz])   # reuses scaler.json + features.json

    va = validate.submit(tr, fz, wait_for=[tr])             # held-out validation scoring
    te = test.submit(tr, tfz, data_dir, wait_for=[tr, tfz])

    # right after each stage (mirrors each other): parity_plot draws its chart and
    # publish_artifacts attaches that stage's metrics to the Prefect UI
    prep_meta = fz.result()
    target = prep_meta["target"]
    train_meta = tr.result()
    p_train = parity_plot.submit(train_meta["train_true"], train_meta["train_pred"],
                                 "train", work, target, wait_for=[tr])
    a_train = publish_artifacts.submit(
        "train", {"best_cv_rmse": train_meta["best_cv_rmse"], "train_rmse": train_meta["train_rmse"],
                  "train_mae": train_meta["train_mae"], "train_r2": train_meta["train_r2"]},
        run_name, train_meta["top_features"], wait_for=[tr])
    metrics = va.result()
    p_val = parity_plot.submit(metrics["val_true"], metrics["val_pred"],
                               "validation", work, target, wait_for=[va])
    a_val = publish_artifacts.submit(
        "validation", {"val_rmse": metrics["val_rmse"], "val_mae": metrics["val_mae"],
                       "val_r2": metrics["val_r2"]}, run_name, wait_for=[va])
    pred_meta = te.result()
    p_test = parity_plot.submit(pred_meta.get("test_true", []), pred_meta.get("test_pred", []),
                                "test", work, target, wait_for=[te])
    a_test = publish_artifacts.submit(
        "test", {"test_predicted": pred_meta["test_predicted"], "test_rmse": pred_meta["test_rmse"],
                 "test_mae": pred_meta["test_mae"], "test_r2": pred_meta["test_r2"]},
        run_name, wait_for=[te])
    for f in (p_train, p_val, p_test, a_train, a_val, a_test):
        f.result()

    summary = {"submitter": submitter, "target": target,
               "n_train": prep_meta["n_train"], "n_val": prep_meta["n_val"],
               "n_features": prep_meta["n_features"],
               "best_cv_rmse": train_meta["best_cv_rmse"],
               "train_rmse": train_meta["train_rmse"], "train_mae": train_meta["train_mae"],
               "train_r2": train_meta["train_r2"],
               "val_rmse": metrics["val_rmse"], "val_mae": metrics["val_mae"],
               "val_r2": metrics["val_r2"],                  # scalars only - not the parity arrays
               "test_predicted": pred_meta["test_predicted"],
               "test_rmse": pred_meta["test_rmse"], "test_mae": pred_meta["test_mae"],
               "test_r2": pred_meta["test_r2"]}
    log.info(f"done: {summary}")
    return summary


def parse_args(argv: list = None) -> argparse.Namespace:
    """Parse the CLI args pipeline.py passes to this payload."""
    p = argparse.ArgumentParser()
    p.add_argument("--data_folder", type=Path, default=HERE / "data")
    p.add_argument("--submitter", type=str, default="local")
    p.add_argument("--sample_rows", type=int, default=None,
                   help="fast smoke test: use only the most-recent N rows (overrides optuna.json)")
    # optional (default server) so pipeline.py's pool call (no --run-on) is unaffected;
    # pass --run-on local for standalone debugging with no Prefect server.
    p.add_argument("--run-on", choices=["local", "server"], default="server",
                   help="local: run ephemerally with no server (local debugging); "
                        "server: record the run on the Prefect server (PREFECT_API_URL)")
    return p.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    kw = dict(submitter=a.submitter, sample_rows=a.sample_rows)
    if a.run_on == "local":                                 # disable PREFECT_API_URL -> ephemeral local run
        from prefect.settings import PREFECT_API_URL, temporary_settings
        with temporary_settings({PREFECT_API_URL: ""}):
            my_flow(str(a.data_folder), **kw)
    else:                                                    # use the configured Prefect server
        my_flow(str(a.data_folder), **kw)