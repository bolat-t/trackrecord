"""Download a static GTFS schedule bundle (zip) and extract key tables to Parquet.

GTFS static = the scheduled timetable we measure realtime against. We keep the
raw zip for provenance and write the tables we need as Parquet under:

    data/raw/schedule/<mode>/<table>.parquet

Everything is read as strings on purpose: GTFS times can exceed 24h (e.g.
"25:30:00" for trips after midnight), so we keep them verbatim and type them in
Silver rather than let pandas misparse them.
"""

from __future__ import annotations

import argparse
import io
import zipfile
from datetime import datetime, timezone

import httpx
import pandas as pd

from .. import config

# Tables used for reliability analysis (others in the zip are ignored).
WANT = ["agency", "routes", "trips", "stops", "stop_times", "calendar", "calendar_dates"]


def download(mode: str) -> bytes:
    if mode not in config.SCHEDULE:
        raise SystemExit(f"unknown mode '{mode}'; known: {sorted(config.SCHEDULE)}")
    resp = httpx.get(config.SCHEDULE[mode], headers=config.auth_headers(), timeout=120.0)
    resp.raise_for_status()
    return resp.content


def extract(mode: str, blob: bytes) -> dict:
    out_dir = config.DATA_RAW / "schedule" / mode
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    (out_dir / f"gtfs_{stamp}.zip").write_bytes(blob)  # keep raw for provenance
    tables: dict[str, int] = {}
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        members = z.namelist()
        for table in WANT:
            fn = f"{table}.txt"
            if fn not in members:
                continue
            with z.open(fn) as fh:
                df = pd.read_csv(fh, dtype=str)
            df.to_parquet(out_dir / f"{table}.parquet", index=False)
            tables[table] = len(df)
    return {"out_dir": out_dir, "tables": tables, "members": members}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Download static GTFS and extract to Parquet")
    p.add_argument("--mode", default="sydneytrains")
    args = p.parse_args(argv)
    blob = download(args.mode)
    print(f"downloaded {len(blob):,} bytes")
    info = extract(args.mode, blob)
    print(f"zip members: {len(info['members'])}  ({', '.join(info['members'][:12])}...)")
    for table, n in info["tables"].items():
        print(f"  {table:16} {n:>12,} rows")
    print(f"-> {info['out_dir'].relative_to(config.REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
