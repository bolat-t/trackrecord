"""Phase 0 smoke test for the TfNSW GTFS-Realtime feed.

Two modes:

  --selftest   Offline. Builds a synthetic GTFS-RT FeedMessage, serializes it,
               parses it back, and prints a summary. Proves the protobuf
               toolchain (gtfs-realtime-bindings + protobuf) works on this
               machine WITHOUT needing the API key or network.

  (default)    Live. GETs the real trip-update feed (Sydney Trains by default),
               parses the protobuf, and prints the feed timestamp, entity
               count, and a sample TripUpdate with stop-level delays.
               Needs TFNSW_API_KEY (see .env.example).

Usage:
    uv run trackrecord-smoke --selftest
    uv run trackrecord-smoke                 # live, sydneytrains
    uv run trackrecord-smoke --mode metro
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone

from google.transit import gtfs_realtime_pb2 as rt

from . import config


def _fmt_ts(epoch: int) -> str:
    if not epoch:
        return "(none)"
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone()
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def summarize(feed: "rt.FeedMessage", source: str) -> None:
    print(f"\n  source            : {source}")
    print(f"  gtfs_rt version   : {feed.header.gtfs_realtime_version}")
    print(f"  feed timestamp    : {_fmt_ts(feed.header.timestamp)}")
    print(f"  entities          : {len(feed.entity)}")
    trip_updates = [e for e in feed.entity if e.HasField("trip_update")]
    print(f"  trip_updates      : {len(trip_updates)}")
    if trip_updates:
        tu = trip_updates[0].trip_update
        stus = tu.stop_time_update
        print(
            f"  sample trip       : route={tu.trip.route_id or '?'} "
            f"trip={tu.trip.trip_id or '?'} stops={len(stus)}"
        )
        for stu in list(stus)[:3]:
            arr = stu.arrival.delay if stu.HasField("arrival") else None
            dep = stu.departure.delay if stu.HasField("departure") else None
            ref = stu.stop_id or f"seq={stu.stop_sequence}"
            print(f"      stop {ref}: arrival_delay={arr}s departure_delay={dep}s")


def build_synthetic() -> "rt.FeedMessage":
    """A tiny but realistic FeedMessage: one late T1 trip with three stops."""
    feed = rt.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = rt.FeedHeader.FULL_DATASET
    feed.header.timestamp = int(time.time())
    ent = feed.entity.add()
    ent.id = "synthetic-1"
    tu = ent.trip_update
    tu.trip.trip_id = "T1.SYNTH.0700"
    tu.trip.route_id = "T1"
    for i, (stop, delay) in enumerate([("2000", 0), ("2010", 120), ("2020", 372)]):
        stu = tu.stop_time_update.add()
        stu.stop_sequence = i + 1
        stu.stop_id = stop
        stu.arrival.delay = delay
        stu.departure.delay = delay
    return feed


def run_selftest() -> int:
    print("== TfNSW GTFS-RT toolchain self-test (offline, synthetic message) ==")
    feed = build_synthetic()
    blob = feed.SerializeToString()
    print(f"  serialized bytes  : {len(blob)}")
    parsed = rt.FeedMessage()
    parsed.ParseFromString(blob)
    summarize(parsed, source="synthetic FeedMessage (encode -> decode roundtrip)")
    ok = parsed.entity[0].trip_update.stop_time_update[2].arrival.delay == 372
    print(f"\n  roundtrip OK      : {ok}")
    print("  -> protobuf + gtfs-realtime-bindings verified. Live call needs TFNSW_API_KEY.")
    return 0 if ok else 1


def run_live(mode: str) -> int:
    import httpx

    url = config.REALTIME_TRIPUPDATE[mode]
    print(f"== TfNSW GTFS-RT LIVE smoke test :: {mode} ==\n  GET {url}")
    try:
        resp = httpx.get(url, headers=config.auth_headers(), timeout=30.0)
    except Exception as exc:  # noqa: BLE001 - surface any transport error plainly
        print(f"  request failed: {exc}", file=sys.stderr)
        return 2
    ctype = resp.headers.get("content-type")
    print(f"  HTTP {resp.status_code}  ({len(resp.content)} bytes, content-type={ctype})")
    if resp.status_code != 200:
        print(f"  body (first 300): {resp.text[:300]}", file=sys.stderr)
        return 3
    feed = rt.FeedMessage()
    feed.ParseFromString(resp.content)
    summarize(feed, source=url)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TfNSW GTFS-RT smoke test")
    parser.add_argument(
        "--selftest", action="store_true",
        help="offline protobuf roundtrip (no key/network)",
    )
    parser.add_argument(
        "--mode", default="sydneytrains",
        choices=sorted(config.REALTIME_TRIPUPDATE),
        help="live feed to hit (default: sydneytrains)",
    )
    args = parser.parse_args(argv)
    if args.selftest:
        return run_selftest()
    return run_live(args.mode)


if __name__ == "__main__":
    raise SystemExit(main())
