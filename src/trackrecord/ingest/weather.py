"""Fetch Sydney hourly weather (Open-Meteo) for the rain-vs-delay analysis.

Open-Meteo is free, key-less, reliable, and returns hourly **precipitation (mm)**
directly with `past_days` backfill. We tested BOM's Observatory Hill JSON first,
but it 301-redirects / blocks automated pulls and only exposes cumulative
'rain since 9am' — so Open-Meteo is the robust choice. Labelled honestly in the
README.

Writes a single rolling-window Parquet (overwritten each run; Open-Meteo returns
the whole recent window each call):

    data/raw/weather/sydney/weather.parquet

    uv run trackrecord-weather
"""

from __future__ import annotations

import httpx
import pandas as pd

from .. import config

# Sydney (Observatory Hill ~ CBD)
LAT, LON = -33.8607, 151.2050
URL = "https://api.open-meteo.com/v1/forecast"


def fetch() -> dict:
    params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": "precipitation,temperature_2m,rain",
        "timezone": "Australia/Sydney",
        "past_days": 7,
        "forecast_days": 1,
    }
    resp = httpx.get(URL, params=params, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


def to_frame(payload: dict) -> pd.DataFrame:
    h = payload["hourly"]
    n = len(h["time"])
    df = pd.DataFrame({
        "obs_time_local": h["time"],                       # 'YYYY-MM-DDTHH:MM' (Sydney)
        "precip_mm": h["precipitation"],
        "rain_mm": h.get("rain", [None] * n),
        "temp_c": h["temperature_2m"],
    })
    df["obs_date"] = df["obs_time_local"].str.slice(0, 10)        # YYYY-MM-DD
    df["obs_hour"] = df["obs_time_local"].str.slice(11, 13).astype(int)
    return df


def capture() -> dict:
    df = to_frame(fetch())
    out_dir = config.DATA_RAW / "weather" / "sydney"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "weather.parquet"
    df.to_parquet(out, index=False)
    return {"path": out, "rows": len(df),
            "from": df["obs_time_local"].min(), "to": df["obs_time_local"].max()}


def main(argv: list[str] | None = None) -> int:
    info = capture()
    rel = info["path"].relative_to(config.REPO_ROOT)
    print(f"weather rows={info['rows']} {info['from']} -> {info['to']} -> {rel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
