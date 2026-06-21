"""Phase 4: train a ">5-min-late" classifier, track + register it in Databricks
MLflow (Unity Catalog model registry), and batch-score predictions to Gold.

Training runs locally (scikit-learn) but logs to the workspace's *managed*
MLflow and the *UC model registry*, so the experiment and the registered model
live in Databricks. Honest note: with a small, mostly-on-time snapshot the model
is provisional — it sharpens as the 15-min cron accumulates peak/multi-day data.

    uv run trackrecord-train
"""

from __future__ import annotations

import os

import mlflow
import pandas as pd
from mlflow.models.signature import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (confusion_matrix, f1_score, precision_score,
                             recall_score, roc_auc_score)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from .. import config
from ..lakehouse.common import client, df as sql_df, run_sql, warehouse_id

CAT = os.environ.get("TR_CATALOG", "workspace")
MODEL_NAME = f"{CAT}.gold.late5_classifier"
CAT_FEATURES = ["line_code", "day_of_week"]
NUM_FEATURES = ["sched_hour", "route_type", "precip_mm", "is_suburban_i", "is_raining_i"]
FEATURES = CAT_FEATURES + NUM_FEATURES
TARGET = "is_late_5min"

PULL_SQL = f"""
SELECT trip_id, stop_id, service_date, line_code, day_of_week,
       coalesce(sched_hour, -1)  AS sched_hour,
       coalesce(route_type, -1)  AS route_type,
       coalesce(precip_mm, 0)    AS precip_mm,
       CASE WHEN is_suburban THEN 1 ELSE 0 END AS is_suburban_i,
       CASE WHEN is_raining  THEN 1 ELSE 0 END AS is_raining_i,
       delay_minutes,
       CASE WHEN is_late_5min THEN 1 ELSE 0 END AS is_late_5min
FROM {CAT}.silver.stop_delays
WHERE line_code IS NOT NULL
"""


def load_df(w, wid) -> pd.DataFrame:
    d = sql_df(run_sql(w, wid, PULL_SQL))
    for c in ["sched_hour", "route_type", "is_suburban_i", "is_raining_i", "is_late_5min"]:
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0).astype(int)
    for c in ["precip_mm", "delay_minutes"]:
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0.0)
    d["line_code"] = d["line_code"].fillna("NA")
    d["day_of_week"] = d["day_of_week"].fillna("NA")
    return d


def build_pipeline() -> Pipeline:
    pre = ColumnTransformer(
        [("cat", OneHotEncoder(handle_unknown="ignore"), CAT_FEATURES)],
        remainder="passthrough",
    )
    clf = LogisticRegression(max_iter=2000, class_weight="balanced")
    return Pipeline([("pre", pre), ("clf", clf)])


def score_to_gold(w, wid, vroot, d: pd.DataFrame, pipe) -> str:
    d = d.copy()
    d["prob_late"] = pipe.predict_proba(d[FEATURES])[:, 1]
    d["pred_late"] = (d["prob_late"] >= 0.5).astype(int)
    preds = d[["trip_id", "stop_id", "service_date", "line_code", "sched_hour",
               "delay_minutes", "is_late_5min", "prob_late", "pred_late"]]
    out = config.DATA_RAW / "ml"
    out.mkdir(parents=True, exist_ok=True)
    local = out / "delay_predictions.parquet"
    preds.to_parquet(local, index=False)
    dest = f"{vroot}/ml/delay_predictions.parquet"
    with open(local, "rb") as fh:
        w.files.upload(dest, fh, overwrite=True)
    run_sql(w, wid, f"CREATE OR REPLACE TABLE {CAT}.gold.delay_predictions AS "
                    f"SELECT * FROM read_files('{dest}', format => 'parquet')")
    return f"{CAT}.gold.delay_predictions"


def main(argv: list[str] | None = None) -> int:
    w = client()
    wid = warehouse_id(w)
    user = w.current_user.me().user_name
    vroot = f"/Volumes/{CAT}/bronze/raw"

    d = load_df(w, wid)
    X, y = d[FEATURES], d[TARGET]
    n_pos = int(y.sum())
    print(f"rows={len(d)}  positives(>5min)={n_pos}  rate={n_pos/len(d):.2%}")

    stratify = y if n_pos >= 8 else None
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, random_state=42,
                                              stratify=stratify)
    pipe = build_pipeline()
    pipe.fit(X_tr, y_tr)
    proba = pipe.predict_proba(X_te)[:, 1]
    pred = (proba >= 0.5).astype(int)
    auc = roc_auc_score(y_te, proba) if y_te.nunique() > 1 else float("nan")
    metrics = {
        "auc": float(auc),
        "precision": float(precision_score(y_te, pred, zero_division=0)),
        "recall": float(recall_score(y_te, pred, zero_division=0)),
        "f1": float(f1_score(y_te, pred, zero_division=0)),
        "n_train": len(X_tr), "n_test": len(X_te), "n_pos": n_pos,
    }
    cm = confusion_matrix(y_te, pred).tolist()
    print("metrics:", {k: round(v, 3) if isinstance(v, float) else v for k, v in metrics.items()})
    print("confusion_matrix [[tn,fp],[fn,tp]]:", cm)

    os.environ.setdefault("MLFLOW_RECORD_ENV_VARS_IN_MODEL_LOGGING", "false")
    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(f"/Users/{user}/trackrecord-delay")
    with mlflow.start_run(run_name="late5-logreg") as run:
        mlflow.log_params({"model": "LogisticRegression(balanced)", "features": ",".join(FEATURES),
                           "target": "delay_seconds>300"})
        mlflow.log_metrics(metrics)
        sig = infer_signature(X_tr, pipe.predict(X_tr))
        mlflow.sklearn.log_model(pipe, name="model", signature=sig,
                                 input_example=X_tr.head(3), registered_model_name=MODEL_NAME)
        run_id = run.info.run_id
    print(f"logged MLflow run {run_id}; registered {MODEL_NAME}")

    table = score_to_gold(w, wid, vroot, d, pipe)
    print(f"batch-scored -> {table}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
