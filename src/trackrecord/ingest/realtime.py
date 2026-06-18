"""Capture a GTFS-Realtime feed and write it as partitioned Parquet.

One "capture" = one poll of a feed at a moment in time. TripUpdate captures are
flattened to **one row per stop_time_update** (the grain we need to compute
delay = actual - scheduled in Silver). Files land under a Hive-style layout:

    data/raw/realtime/<mode>/<kind>/dt=YYYY-MM-DD/hour=HH/<kind>_<epoch>.parquet

so they read straight into a partitioned Bronze table in Spark. This script is
what the GitHub Actions cron calls once per run to accumulate live data.
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

import httpx
import pandas as pd
from google.transit import gtfs_realtime_pb2 as rt

from .. import config


def fetch_feed(mode: str, kind: str = "tripupdate") -> bytes:
    table = config.REALTIME_TRIPUPDATE if kind == "tripupdate" else config.REALTIME_VEHICLEPOS
    if mode not in table:
        raise SystemExit(f"unknown mode '{mode}' for {kind}; known: {sorted(table)}")
    resp = httpx.get(table[mode], headers=config.auth_headers(), timeout=60.0)
    resp.raise_for_status()
    return resp.content


def parse_tripupdates(blob: bytes, captured_at: datetime) -> tuple[pd.DataFrame, int]:
    feed = rt.FeedMessage()
    feed.ParseFromString(blob)
    feed_ts = feed.header.timestamp or None
    rows: list[dict] = []
    for ent in feed.entity:
        if not ent.HasField("trip_update"):
            continue
        tu = ent.trip_update
        trip = tu.trip
        base = {
            "entity_id": ent.id,
            "trip_id": trip.trip_id or None,
            "route_id": trip.route_id or None,
            "start_date": trip.start_date or None,
            "start_time": trip.start_time or None,
            "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
            "trip_schedule_relationship": trip.schedule_relationship,
            "vehicle_id": tu.vehicle.id or None if tu.HasField("vehicle") else None,
            "trip_update_ts": tu.timestamp or None,
            "feed_ts": feed_ts,
            "captured_at": captured_at,
        }
        if not tu.stop_time_update:
            rows.append({**base, "stop_sequence": None, "stop_id": None,
                         "arrival_delay": None, "arrival_time": None,
                         "departure_delay": None, "departure_time": None,
                         "stu_schedule_relationship": None})
            continue
        for stu in tu.stop_time_update:
            has_arr, has_dep = stu.HasField("arrival"), stu.HasField("departure")
            rows.append({
                **base,
                "stop_sequence": stu.stop_sequence if stu.HasField("stop_sequence") else None,
                "stop_id": stu.stop_id or None,
                "arrival_delay": stu.arrival.delay if has_arr and stu.arrival.HasField("delay") else None,
                "arrival_time": stu.arrival.time if has_arr and stu.arrival.HasField("time") else None,
                "departure_delay": stu.departure.delay if has_dep and stu.departure.HasField("delay") else None,
                "departure_time": stu.departure.time if has_dep and stu.departure.HasField("time") else None,
                "stu_schedule_relationship": stu.schedule_relationship,
            })
    return pd.DataFrame(rows), len(feed.entity)


def parse_vehiclepos(blob: bytes, captured_at: datetime) -> tuple[pd.DataFrame, int]:
    feed = rt.FeedMessage()
    feed.ParseFromString(blob)
    feed_ts = feed.header.timestamp or None
    rows: list[dict] = []
    for ent in feed.entity:
        if not ent.HasField("vehicle"):
            continue
        v = ent.vehicle
        pos = v.position
        has_pos = v.HasField("position")
        rows.append({
            "entity_id": ent.id,
            "trip_id": v.trip.trip_id or None,
            "route_id": v.trip.route_id or None,
            "vehicle_id": v.vehicle.id or None,
            "lat": pos.latitude if has_pos else None,
            "lon": pos.longitude if has_pos else None,
            "bearing": pos.bearing if has_pos and pos.HasField("bearing") else None,
            "speed": pos.speed if has_pos and pos.HasField("speed") else None,
            "vehicle_ts": v.timestamp or None,
            "feed_ts": feed_ts,
            "captured_at": captured_at,
        })
    return pd.DataFrame(rows), len(feed.entity)


def capture(mode: str, kind: str = "tripupdate") -> dict:
    captured_at = datetime.now(timezone.utc)
    blob = fetch_feed(mode, kind)
    parse = parse_tripupdates if kind == "tripupdate" else parse_vehiclepos
    df, n_entities = parse(blob, captured_at)
    out_dir = (config.DATA_RAW / "realtime" / mode / kind
               / f"dt={captured_at:%Y-%m-%d}" / f"hour={captured_at:%H}")
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{kind}_{int(captured_at.timestamp())}.parquet"
    df.to_parquet(out, index=False)
    return {"path": out, "rows": len(df), "entities": n_entities,
            "bytes": len(blob), "captured_at": captured_at}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Capture a TfNSW GTFS-RT feed to Parquet")
    p.add_argument("--mode", default="sydneytrains")
    p.add_argument("--kind", default="tripupdate", choices=["tripupdate", "vehiclepos"])
    p.add_argument("--repeat", type=int, default=1, help="number of captures (cron uses 1)")
    p.add_argument("--interval", type=float, default=0.0, help="seconds between captures")
    args = p.parse_args(argv)
    for i in range(args.repeat):
        info = capture(args.mode, args.kind)
        rel = info["path"].relative_to(config.REPO_ROOT)
        print(f"[{i + 1}/{args.repeat}] {info['captured_at']:%H:%M:%S}Z "
              f"entities={info['entities']} rows={info['rows']:,} "
              f"bytes={info['bytes']:,} -> {rel}")
        if i + 1 < args.repeat and args.interval:
            time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
