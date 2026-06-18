"""Create/publish the Databricks AI/BI (Lakeview) dashboard on the gold tables.

Idempotent: updates the existing dashboard (matched by display name) or creates
it, then publishes. The serialized spec is also written to
``docs/dashboard.lvdash.json`` as a version-controlled artifact.

    uv run trackrecord-dashboard
"""

from __future__ import annotations

import json
import os

from databricks.sdk.service.dashboards import Dashboard

from .. import config
from .common import client, warehouse_id

CAT = os.environ.get("TR_CATALOG", "workspace")
NAME = "TrackRecord — Sydney Rail Reliability"


def _q(dataset, fields, disaggregated):
    return [{"name": "main", "query": {"datasetName": dataset, "fields": fields,
                                       "disaggregated": disaggregated}}]


def _counter(name, dataset, field, label):
    return {"name": name,
            "queries": _q(dataset, [{"name": field, "expression": f"`{field}`"}], True),
            "spec": {"version": 2, "widgetType": "counter",
                     "encodings": {"value": {"fieldName": field, "displayName": label}}}}


def _bar(name, dataset, dim, measure_expr, measure_name, xlab, ylab, line=False):
    return {"name": name,
            "queries": _q(dataset, [{"name": dim, "expression": f"`{dim}`"},
                                    {"name": measure_name, "expression": measure_expr}], False),
            "spec": {"version": 3, "widgetType": "line" if line else "bar",
                     "encodings": {
                         "x": {"fieldName": dim,
                               "scale": {"type": "quantitative" if line else "categorical"},
                               "displayName": xlab},
                         "y": {"fieldName": measure_name, "scale": {"type": "quantitative"},
                               "displayName": ylab}}}}


def _table(name, dataset, cols):
    fields = [{"name": c, "expression": f"`{c}`"} for c, _ in cols]
    columns = [{"fieldName": c, "displayName": lbl} for c, lbl in cols]
    return {"name": name, "queries": _q(dataset, fields, True),
            "spec": {"version": 1, "widgetType": "table", "encodings": {"columns": columns}}}


def _pos(widget, x, y, width, height):
    return {"widget": widget, "position": {"x": x, "y": y, "width": width, "height": height}}


def build_spec() -> dict:
    datasets = [
        {"name": "kpi", "displayName": "KPI",
         "queryLines": [f"SELECT * FROM {CAT}.gold.network_kpi"]},
        {"name": "lines", "displayName": "Lines",
         "queryLines": [f"SELECT line_code, avg_delay_min, pct_late_5min "
                        f"FROM {CAT}.gold.line_punctuality WHERE is_suburban"]},
        {"name": "hourly", "displayName": "Hourly",
         "queryLines": [f"SELECT sched_hour, avg_delay_min FROM {CAT}.gold.hourly_punctuality"]},
        {"name": "stops", "displayName": "Stations",
         "queryLines": [f"SELECT stop_name, avg_delay_min, pct_late_5min, stop_events "
                        f"FROM {CAT}.gold.stop_punctuality ORDER BY avg_delay_min DESC LIMIT 15"]},
        {"name": "weather", "displayName": "Weather",
         "queryLines": [f"SELECT CASE WHEN is_raining THEN 'Wet' ELSE 'Dry' END AS condition, "
                        f"avg_delay_min FROM {CAT}.gold.weather_delay"]},
    ]
    layout = [
        _pos(_counter("on_time", "kpi", "pct_on_time", "On-time % (<=5 min)"), 0, 0, 2, 3),
        _pos(_counter("avg_delay", "kpi", "avg_delay_min", "Avg delay (min)"), 2, 0, 2, 3),
        _pos(_counter("p90", "kpi", "p90_delay_min", "P90 delay (min)"), 4, 0, 2, 3),
        _pos(_bar("byline", "lines", "line_code", "AVG(`avg_delay_min`)", "avg_delay",
                  "Line", "Avg delay (min)"), 0, 3, 3, 6),
        _pos(_bar("byhour", "hourly", "sched_hour", "AVG(`avg_delay_min`)", "avg_delay",
                  "Scheduled hour", "Avg delay (min)", line=True), 3, 3, 3, 6),
        _pos(_table("stations", "stops", [("stop_name", "Station"), ("avg_delay_min", "Avg delay (min)"),
                                          ("pct_late_5min", "% >5min late"), ("stop_events", "Events")]),
             0, 9, 3, 6),
        _pos(_bar("byweather", "weather", "condition", "AVG(`avg_delay_min`)", "avg_delay",
                  "Condition", "Avg delay (min)"), 3, 9, 3, 6),
    ]
    return {"datasets": datasets,
            "pages": [{"name": "reliability", "displayName": "Reliability", "layout": layout}]}


def _find_existing(w):
    for d in w.lakeview.list():
        if d.display_name == NAME and "TRASH" not in str(getattr(d, "lifecycle_state", "")).upper():
            return d.dashboard_id
    return None


def main(argv: list[str] | None = None) -> int:
    w = client()
    wid = warehouse_id(w)
    spec = build_spec()
    ser = json.dumps(spec)
    art = config.REPO_ROOT / "docs" / "dashboard.lvdash.json"
    art.write_text(json.dumps(spec, indent=2))
    print(f"wrote spec artifact -> {art.relative_to(config.REPO_ROOT)}")

    did = _find_existing(w)
    if did:
        w.lakeview.update(did, dashboard=Dashboard(display_name=NAME, serialized_dashboard=ser,
                                                   warehouse_id=wid))
        print("updated dashboard", did)
    else:
        me = w.current_user.me().user_name
        created = w.lakeview.create(dashboard=Dashboard(display_name=NAME, parent_path=f"/Users/{me}",
                                                        serialized_dashboard=ser, warehouse_id=wid))
        did = created.dashboard_id
        print("created dashboard", did)
    w.lakeview.publish(did, embed_credentials=True, warehouse_id=wid)
    print("published")
    print("URL:", f"{w.config.host}/dashboardsv3/{did}/published")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
