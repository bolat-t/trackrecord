"""Build the Bronze layer.

Creates the Unity Catalog layout (catalog + bronze/silver/gold schemas + a raw
Volume), uploads the local raw Parquet into the Volume, and CREATEs Bronze Delta
tables straight from those files. Idempotent — safe to re-run.

    uv run trackrecord-bronze
"""

from __future__ import annotations

import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType

from .. import config
from .common import client, rows, run_sql, warehouse_id

CATALOG = os.environ.get("TR_CATALOG", "transport_nsw")
SCHEMAS = ["bronze", "silver", "gold"]
RAW_SCHEMA, RAW_VOLUME = "bronze", "raw"
MODE = "sydneytrains"


def ensure_catalog(w: WorkspaceClient) -> str:
    existing = {c.name for c in w.catalogs.list()}
    if CATALOG in existing:
        print(f"catalog {CATALOG} exists")
        return CATALOG
    try:
        w.catalogs.create(name=CATALOG, comment="Sydney transport reliability lakehouse (TrackRecord)")
        print(f"created catalog {CATALOG}")
        return CATALOG
    except Exception as e:  # noqa: BLE001 - Free Edition may restrict catalog creation
        print(f"cannot create catalog {CATALOG} ({type(e).__name__}); falling back to 'workspace'")
        return "workspace"


def ensure_schemas_volume(w: WorkspaceClient, cat: str) -> str:
    have = {s.name for s in w.schemas.list(catalog_name=cat)}
    for s in SCHEMAS:
        if s not in have:
            w.schemas.create(name=s, catalog_name=cat)
            print(f"created schema {cat}.{s}")
    vols = {v.name for v in w.volumes.list(catalog_name=cat, schema_name=RAW_SCHEMA)}
    if RAW_VOLUME not in vols:
        w.volumes.create(catalog_name=cat, schema_name=RAW_SCHEMA, name=RAW_VOLUME,
                         volume_type=VolumeType.MANAGED)
        print(f"created volume {cat}.{RAW_SCHEMA}.{RAW_VOLUME}")
    return f"/Volumes/{cat}/{RAW_SCHEMA}/{RAW_VOLUME}"


def upload_raw(w: WorkspaceClient, vroot: str) -> int:
    files = sorted(config.DATA_RAW.rglob("*.parquet"))
    for p in files:
        rel = p.relative_to(config.DATA_RAW).as_posix()
        with open(p, "rb") as fh:
            w.files.upload(f"{vroot}/{rel}", fh, overwrite=True)
    print(f"uploaded {len(files)} parquet file(s) -> {vroot}")
    return len(files)


def build_bronze(w: WorkspaceClient, wid: str, cat: str, vroot: str) -> list[tuple[str, int]]:
    created: list[str] = []
    # Static GTFS: one Bronze table per parquet present (mirrored into the Volume).
    for p in sorted((config.DATA_RAW / "schedule" / MODE).glob("*.parquet")):
        tbl = f"{cat}.bronze.gtfs_{p.stem}"
        src = f"{vroot}/schedule/{MODE}/{p.name}"
        run_sql(w, wid, f"CREATE OR REPLACE TABLE {tbl} AS "
                        f"SELECT * FROM read_files('{src}', format => 'parquet')")
        created.append(tbl)
    # Realtime: one Bronze table per kind (a directory of partitioned parquet).
    for kind, tname in {"tripupdate": "rt_trip_updates", "vehiclepos": "rt_vehicle_positions"}.items():
        kdir = config.DATA_RAW / "realtime" / MODE / kind
        if not any(kdir.rglob("*.parquet")):
            continue
        tbl = f"{cat}.bronze.{tname}"
        src = f"{vroot}/realtime/{MODE}/{kind}/"
        run_sql(w, wid, f"CREATE OR REPLACE TABLE {tbl} AS "
                        f"SELECT * FROM read_files('{src}', format => 'parquet')")
        created.append(tbl)
    union = " UNION ALL ".join(f"SELECT '{t}' AS tbl, count(*) AS n FROM {t}" for t in created)
    resp = run_sql(w, wid, f"SELECT tbl, n FROM ({union}) ORDER BY tbl")
    return [(r[0], int(r[1])) for r in rows(resp)]


def main(argv: list[str] | None = None) -> int:
    w = client()
    cat = ensure_catalog(w)
    vroot = ensure_schemas_volume(w, cat)
    upload_raw(w, vroot)
    wid = warehouse_id(w)
    print(f"using catalog '{cat}', warehouse {wid} (auto-starts on first query)")
    counts = build_bronze(w, wid, cat, vroot)
    print("\nBronze tables:")
    for tbl, n in counts:
        print(f"  {tbl:44} {n:>12,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
