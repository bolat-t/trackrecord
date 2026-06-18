"""Upload local raw Parquet into the Unity Catalog Volume (no Bronze build).

This is what the GitHub Actions cron runs after each live capture: it just lands
new files in the Volume (Files API — no compute), so it never touches the Free
Edition compute quota. Bronze is refreshed separately (``trackrecord-bronze``).

    uv run trackrecord-upload
"""

from __future__ import annotations

from .bronze import ensure_catalog, ensure_schemas_volume, upload_raw
from .common import client


def main(argv: list[str] | None = None) -> int:
    w = client()
    cat = ensure_catalog(w)
    vroot = ensure_schemas_volume(w, cat)
    upload_raw(w, vroot)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
