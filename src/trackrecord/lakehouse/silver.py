"""Build the Silver layer: one clean row per realised stop event, with delay + weather.

Source: ``bronze.rt_trip_updates`` enriched with ``gtfs_routes`` / ``gtfs_stops``
/ ``gtfs_stop_times`` and joined to ``bronze.weather_sydney``.

Key decisions (all verified against the data, 2026-06-18):
- ``delay_seconds = coalesce(arrival_delay, departure_delay)`` — TfNSW already
  computes this (actual − scheduled, seconds). Source has true NULLs, no NaN.
- Dedupe to the **latest capture** per ``(trip_id, service_date, stop_id)``.
- Join scheduled times on **(trip_id, stop_id)** — the RT ``stop_sequence`` is
  sparse/NULL, but ``stop_id`` matches the static feed 100% (unique per trip in
  ``stop_times``). ~98% of stop-events get a scheduled time (``schedule_matched``).
- ``service_date`` from capture time in Australia/Sydney (feed omits start_date).
- Weather joined by (service_date, scheduled hour) from Open-Meteo hourly data.
- Filter ``RTTA_*`` non-revenue runs and SKIPPED stop updates.

    uv run trackrecord-silver
"""

from __future__ import annotations

import os

from .common import client, rows, run_sql, warehouse_id

CAT = os.environ.get("TR_CATALOG", "workspace")

SILVER_SQL = f"""
CREATE OR REPLACE TABLE {CAT}.silver.stop_delays AS
WITH latest AS (
  SELECT *, ROW_NUMBER() OVER (
              PARTITION BY trip_id, service_date, stop_id
              ORDER BY captured_at DESC) AS rn
  FROM (
    SELECT trip_id, route_id, stop_id,
           coalesce(arrival_delay, departure_delay) AS delay_seconds,
           stu_schedule_relationship, captured_at,
           date(from_utc_timestamp(captured_at, 'Australia/Sydney')) AS service_date
    FROM {CAT}.bronze.rt_trip_updates
    WHERE coalesce(arrival_delay, departure_delay) IS NOT NULL
      AND (stu_schedule_relationship IS NULL OR stu_schedule_relationship = 0)
      AND route_id NOT LIKE 'RTTA%'
  )
),
enriched AS (
  SELECT
    l.service_date,
    date_format(l.service_date, 'E')                     AS day_of_week,
    l.trip_id,
    l.route_id,
    r.route_short_name                                   AS line_code,
    r.route_long_name                                    AS line_name,
    try_cast(r.route_type AS INT)                        AS route_type,
    (r.route_short_name RLIKE '^T[0-9]')                 AS is_suburban,
    l.stop_id,
    s.stop_name,
    st.arrival_time                                      AS sched_arrival,
    try_cast(split(st.arrival_time, ':')[0] AS INT) % 24 AS sched_hour,
    cast(l.delay_seconds AS INT)                         AS delay_seconds,
    round(l.delay_seconds / 60.0, 2)                     AS delay_minutes,
    (l.delay_seconds > 300)                              AS is_late_5min,
    (st.trip_id IS NOT NULL)                             AS schedule_matched,
    l.captured_at
  FROM latest l
  LEFT JOIN {CAT}.bronze.gtfs_routes     r  ON r.route_id = l.route_id
  LEFT JOIN {CAT}.bronze.gtfs_stops      s  ON s.stop_id  = l.stop_id
  LEFT JOIN {CAT}.bronze.gtfs_stop_times st ON st.trip_id = l.trip_id AND st.stop_id = l.stop_id
  WHERE l.rn = 1
)
SELECT e.*,
       wx.precip_mm,
       wx.temp_c,
       (coalesce(wx.precip_mm, 0) >= 0.2) AS is_raining
FROM enriched e
LEFT JOIN {CAT}.bronze.weather_sydney wx
       ON to_date(wx.obs_date) = e.service_date AND wx.obs_hour = e.sched_hour
"""


def build(w, wid) -> None:
    run_sql(w, wid, SILVER_SQL)


def proof(w, wid) -> None:
    def show(title, q):
        print(f"\n{title}")
        for r in rows(run_sql(w, wid, q)):
            print("  " + " | ".join("" if v is None else str(v) for v in r))

    show("overview (events | trips | lines | % with scheduled time)", f"""
        SELECT count(*), count(DISTINCT trip_id), count(DISTINCT line_code),
               round(100.0*avg(CASE WHEN schedule_matched THEN 1 ELSE 0 END),1)
        FROM {CAT}.silver.stop_delays""")
    show("suburban (T-lines) punctuality: avg delay min | % >5min late | worst min", f"""
        SELECT round(avg(delay_minutes),2), round(100.0*avg(CASE WHEN is_late_5min THEN 1 ELSE 0 END),1),
               max(delay_minutes)
        FROM {CAT}.silver.stop_delays WHERE is_suburban""")
    show("worst T-lines by avg delay (line_code | line | n | avg_min | %late)", f"""
        SELECT line_code, any_value(line_name), count(*),
               round(avg(delay_minutes),2), round(100.0*avg(CASE WHEN is_late_5min THEN 1 ELSE 0 END),1)
        FROM {CAT}.silver.stop_delays WHERE is_suburban
        GROUP BY line_code HAVING count(*) >= 25 ORDER BY 4 DESC LIMIT 8""")
    show("weather join: % matched | avg delay dry | avg delay wet | wet events", f"""
        SELECT round(100.0*avg(CASE WHEN precip_mm IS NOT NULL THEN 1 ELSE 0 END),1),
               round(avg(CASE WHEN NOT is_raining THEN delay_minutes END),2),
               round(avg(CASE WHEN is_raining THEN delay_minutes END),2),
               sum(CASE WHEN is_raining THEN 1 ELSE 0 END)
        FROM {CAT}.silver.stop_delays""")


def main(argv: list[str] | None = None) -> int:
    w = client()
    wid = warehouse_id(w)
    build(w, wid)
    print(f"built {CAT}.silver.stop_delays")
    proof(w, wid)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
