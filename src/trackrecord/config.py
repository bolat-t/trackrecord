"""Central config: TfNSW endpoints, auth, and project paths.

Endpoints reflect the TfNSW Open Data Hub docs (verified June 2026). The
Sydney Trains GTFS-Realtime *trip-update* path is the one the live collector
depends on; a v2 trip-update feed also exists and we confirm the exact path
in the live smoke test once the API key is in. The metro/ferry sub-paths are
best-known values and are likewise confirmed against the live API.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()  # read .env if present (no-op in Databricks)

TFNSW_API_KEY = os.environ.get("TFNSW_API_KEY", "")

BASE = "https://api.transport.nsw.gov.au"

# GTFS-Realtime Trip Updates: predicted/actual stop times -> delay vs schedule.
# Endpoint versions differ per feed — verified live against the API 2026-06-18:
# Sydney Trains is on v2 (v1 -> 404); NSW Trains (regional) is on v1. The
# metro/ferry *live* paths below are unverified (the hybrid backbone uses the
# historical dataset for those) — confirm before relying on them.
REALTIME_TRIPUPDATE = {
    "sydneytrains": f"{BASE}/v2/gtfs/realtime/sydneytrains",
    "nswtrains": f"{BASE}/v1/gtfs/realtime/nswtrains",
    "metro": f"{BASE}/v2/gtfs/realtime/metro",  # unverified
    "sydneyferries": f"{BASE}/v2/gtfs/realtime/ferries/sydneyferries",  # unverified
}

# GTFS-Realtime Vehicle Positions (lat/lon/bearing) — optional, Phase 1+.
# Verified live 2026-06-18: Sydney Trains vehicle positions are on v2.
REALTIME_VEHICLEPOS = {
    "sydneytrains": f"{BASE}/v2/gtfs/vehiclepos/sydneytrains",
}

# Static GTFS schedule bundles (zip: stops/routes/trips/stop_times/calendar...).
SCHEDULE = {
    "sydneytrains": f"{BASE}/v1/gtfs/schedule/sydneytrains",
    "metro": f"{BASE}/v1/gtfs/schedule/metro",
    "sydneyferries": f"{BASE}/v1/gtfs/schedule/ferries/sydneyferries",
}


def auth_headers() -> dict[str, str]:
    """TfNSW uses an 'apikey' scheme (NOT Bearer): 'Authorization: apikey <key>'."""
    if not TFNSW_API_KEY:
        raise RuntimeError(
            "TFNSW_API_KEY is not set. Copy .env.example to .env and paste your key, "
            "or run:  export TFNSW_API_KEY=..."
        )
    return {"Authorization": f"apikey {TFNSW_API_KEY}"}


# Repo paths
REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = REPO_ROOT / "data" / "raw"
