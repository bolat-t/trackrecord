"""Build the Gold layer: dashboard-ready aggregates over silver.stop_delays.

Focus is the suburban T-line network (the headline reliability story); intercity
and light-rail are kept in ``line_punctuality`` (flagged via ``is_suburban``) but
excluded from the KPI / time-of-day / station / weather tables to keep the
narrative clean.

Tables (all in ``{CAT}.gold``):
  network_kpi         one row — overall KPIs + data window
  line_punctuality    per line (all lines; is_suburban flag)
  hourly_punctuality  per scheduled hour (suburban)
  daily_punctuality   per service_date (suburban) + daily rain
  stop_punctuality    per station (suburban, >= 20 events)
  weather_delay       dry vs wet split (suburban)

    uv run trackrecord-gold
"""

from __future__ import annotations

import os

from .common import client, rows, run_sql, warehouse_id

CAT = os.environ.get("TR_CATALOG", "workspace")
S = f"{CAT}.silver.stop_delays"
G = f"{CAT}.gold"

STATEMENTS = {
    "network_kpi": f"""
      CREATE OR REPLACE TABLE {G}.network_kpi AS
      SELECT count(*) AS stop_events,
             count(DISTINCT trip_id) AS trips,
             count(DISTINCT line_code) AS lines,
             round(avg(delay_minutes), 2) AS avg_delay_min,
             round(percentile_approx(delay_minutes, 0.5), 2) AS median_delay_min,
             round(percentile_approx(delay_minutes, 0.9), 2) AS p90_delay_min,
             round(100.0*avg(CASE WHEN is_late_5min THEN 1 ELSE 0 END), 1) AS pct_late_5min,
             round(100.0*avg(CASE WHEN delay_seconds <= 300 THEN 1 ELSE 0 END), 1) AS pct_on_time,
             min(service_date) AS first_day, max(service_date) AS last_day,
             count(DISTINCT captured_at) AS captures
      FROM {S} WHERE is_suburban
    """,
    "line_punctuality": f"""
      CREATE OR REPLACE TABLE {G}.line_punctuality AS
      SELECT line_code, any_value(line_name) AS line_name,
             any_value(is_suburban) AS is_suburban, any_value(route_type) AS route_type,
             count(*) AS stop_events, count(DISTINCT trip_id) AS trips,
             round(avg(delay_minutes), 2) AS avg_delay_min,
             round(percentile_approx(delay_minutes, 0.9), 2) AS p90_delay_min,
             round(100.0*avg(CASE WHEN is_late_5min THEN 1 ELSE 0 END), 1) AS pct_late_5min
      FROM {S} WHERE line_code IS NOT NULL GROUP BY line_code
    """,
    "hourly_punctuality": f"""
      CREATE OR REPLACE TABLE {G}.hourly_punctuality AS
      SELECT sched_hour, count(*) AS stop_events,
             round(avg(delay_minutes), 2) AS avg_delay_min,
             round(100.0*avg(CASE WHEN is_late_5min THEN 1 ELSE 0 END), 1) AS pct_late_5min
      FROM {S} WHERE is_suburban AND sched_hour IS NOT NULL GROUP BY sched_hour
    """,
    "daily_punctuality": f"""
      CREATE OR REPLACE TABLE {G}.daily_punctuality AS
      SELECT service_date, any_value(day_of_week) AS day_of_week, count(*) AS stop_events,
             round(avg(delay_minutes), 2) AS avg_delay_min,
             round(100.0*avg(CASE WHEN is_late_5min THEN 1 ELSE 0 END), 1) AS pct_late_5min,
             round(avg(precip_mm), 3) AS avg_precip_mm,
             max(CASE WHEN is_raining THEN 1 ELSE 0 END) AS any_rain
      FROM {S} WHERE is_suburban GROUP BY service_date
    """,
    "stop_punctuality": f"""
      CREATE OR REPLACE TABLE {G}.stop_punctuality AS
      SELECT stop_id, any_value(stop_name) AS stop_name, count(*) AS stop_events,
             round(avg(delay_minutes), 2) AS avg_delay_min,
             round(100.0*avg(CASE WHEN is_late_5min THEN 1 ELSE 0 END), 1) AS pct_late_5min
      FROM {S} WHERE is_suburban AND stop_name IS NOT NULL
      GROUP BY stop_id HAVING count(*) >= 20
    """,
    "weather_delay": f"""
      CREATE OR REPLACE TABLE {G}.weather_delay AS
      SELECT is_raining, count(*) AS stop_events,
             round(avg(delay_minutes), 2) AS avg_delay_min,
             round(100.0*avg(CASE WHEN is_late_5min THEN 1 ELSE 0 END), 1) AS pct_late_5min
      FROM {S} WHERE is_suburban AND precip_mm IS NOT NULL GROUP BY is_raining
    """,
}


def build(w, wid) -> None:
    for sql in STATEMENTS.values():
        run_sql(w, wid, sql)


def proof(w, wid) -> None:
    def show(title, q):
        print(f"\n{title}")
        for r in rows(run_sql(w, wid, q)):
            print("  " + " | ".join("" if v is None else str(v) for v in r))

    show("network_kpi", f"""SELECT stop_events, trips, lines, avg_delay_min, median_delay_min,
            p90_delay_min, pct_late_5min, pct_on_time, first_day, last_day, captures
            FROM {G}.network_kpi""")
    show("worst suburban lines", f"""SELECT line_code, line_name, stop_events, avg_delay_min,
            p90_delay_min, pct_late_5min FROM {G}.line_punctuality WHERE is_suburban
            ORDER BY avg_delay_min DESC LIMIT 6""")
    show("top late hours", f"""SELECT sched_hour, stop_events, avg_delay_min, pct_late_5min
            FROM {G}.hourly_punctuality ORDER BY avg_delay_min DESC LIMIT 6""")
    show("worst stations", f"""SELECT stop_name, stop_events, avg_delay_min, pct_late_5min
            FROM {G}.stop_punctuality ORDER BY avg_delay_min DESC LIMIT 6""")
    show("weather split (dry/wet)", f"""SELECT is_raining, stop_events, avg_delay_min, pct_late_5min
            FROM {G}.weather_delay ORDER BY is_raining""")


def main(argv: list[str] | None = None) -> int:
    w = client()
    wid = warehouse_id(w)
    build(w, wid)
    print(f"built {G}.{{network_kpi, line_punctuality, hourly_punctuality, "
          f"daily_punctuality, stop_punctuality, weather_delay}}")
    proof(w, wid)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
