"""Interactively query the registered delay model: P(a trip is >5-min late).

Loads the latest version of the Unity Catalog model and scores a hypothetical
trip from CLI args.

    uv run trackrecord-predict --line T4 --hour 8 --rain
    uv run trackrecord-predict --line T1 --hour 14
"""

from __future__ import annotations

import argparse
import re

import mlflow
import pandas as pd

from ..lakehouse.common import client  # noqa: F401  imported for .env side-effect

MODEL_NAME = "workspace.gold.late5_classifier"


def _latest_version() -> int:
    from mlflow.tracking import MlflowClient

    mc = MlflowClient(registry_uri="databricks-uc")
    versions = [int(v.version) for v in mc.search_model_versions(f"name='{MODEL_NAME}'")]
    return max(versions) if versions else 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Predict P(>5-min late) for a trip")
    p.add_argument("--line", default="T4", help="line code, e.g. T4, T1, CCN, SCO")
    p.add_argument("--hour", type=int, default=8, help="scheduled hour 0-23")
    p.add_argument("--dow", default="Thu", help="day of week (Mon..Sun)")
    p.add_argument("--route-type", type=int, default=2, help="GTFS route_type (2=rail)")
    p.add_argument("--rain", action="store_true", help="raining (precip >= 0.2 mm)")
    a = p.parse_args(argv)

    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")
    ver = _latest_version()
    model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{ver}")

    precip = 2.0 if a.rain else 0.0
    row = {
        "line_code": a.line, "day_of_week": a.dow, "sched_hour": a.hour,
        "route_type": a.route_type, "precip_mm": precip,
        "is_suburban_i": 1 if re.match(r"^T[0-9]", a.line) else 0,
        "is_raining_i": 1 if precip >= 0.2 else 0,
    }
    prob = float(model.predict_proba(pd.DataFrame([row]))[0, 1])
    verdict = "LIKELY LATE" if prob >= 0.5 else "probably on time"
    print(f"model: {MODEL_NAME} v{ver}")
    print(f"trip:  {a.line} @ {a.hour:02d}:00  ({a.dow}, {'raining' if a.rain else 'dry'})")
    print(f"P(>5 min late) = {prob:.1%}  ->  {verdict}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
