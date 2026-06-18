"""Build the Silver layer: one clean row per realised stop event, with delay.

Source: ``bronze.rt_trip_updates`` enriched with ``gtfs_routes`` / ``gtfs_stops``
/ ``gtfs_stop_times``.

Key decisions (all verified against the data, 2026-06-18):
- ``delay_seconds = coalesce(arrival_delay, departure_delay)`` — TfNSW already
  computes this (actual − scheduled, seconds). Source has true NULLs, no NaN.
- Dedupe to the **latest capture** per ``(trip_id, service_date, stop_id)`` so we
  keep the most-settled prediction for each stop event.
- Join scheduled times on **(trip_id, stop_id)** — the RT ``stop_sequence`` is
  sparse/NULL, but ``stop_id`` matches the static feed 100% (and is unique per
  trip in ``stop_times``). ~93% of RT stop-events get a scheduled time; the rest
  are mostly intercity/added trips absent from the suburban GTFS bundle
  (flagged via ``schedule_matched``).
- ``service_date`` is derived from capture time in Australia/Sydney, because the
  feed leaves ``start_date`` empty.
- Filter out ``RTTA_*`` non-revenue runs and SKIPPED stop updates.

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
)
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
    show("delay by scheduled hour, T-lines (hour | n | avg_min)", f"""
        SELECT sched_hour, count(*), round(avg(delay_minutes),2)
        FROM {CAT}.silver.stop_delays WHERE is_suburban AND sched_hour IS NOT NULL
        GROUP BY sched_hour ORDER BY 3 DESC LIMIT 6""")


def main(argv: list[str] | None = None) -> int:
    w = client()
    wid = warehouse_id(w)
    build(w, wid)
    print(f"built {CAT}.silver.stop_delays")
    proof(w, wid)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
