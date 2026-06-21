# Databricks (Free Edition) — run notes

How TrackRecord uses Databricks, the Free Edition quirks worked around, and how to
reproduce the lakehouse.

## Platform

- **Free Edition**, serverless only, **one metastore**. New UC catalogs can't be created
  (`InvalidState`) → everything lives in the pre-provisioned **`workspace`** catalog.
- **Outbound egress** from serverless is restricted to an allowlist, so the TfNSW/Open-Meteo
  pulls run **outside** Databricks (locally + a GitHub Actions cron) and land raw Parquet in
  a **Unity Catalog Volume** (`workspace.bronze.raw`); Spark/SQL reads from the Volume.
- Auth is a **Personal Access Token** in `.env` (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`);
  every script drives the workspace through the **`databricks-sdk`** (no notebooks-by-hand).
- Compute = the **Serverless Starter** SQL warehouse (2X-Small) for SQL; serverless job
  compute for the Workflow.

## Reproduce

```bash
uv sync
uv run trackrecord-gtfs        # static GTFS bundle -> data/raw/schedule
uv run trackrecord-collect     # one live GTFS-RT capture -> data/raw/realtime
uv run trackrecord-weather     # Open-Meteo hourly precip -> data/raw/weather
uv run trackrecord-bronze      # create UC schemas + Volume, upload raw, build Bronze Delta
uv run trackrecord-silver      # Silver: per-stop delay (+ weather join)
uv run trackrecord-gold        # Gold aggregates
uv run trackrecord-dashboard   # publish the AI/BI (Lakeview) dashboard
uv run trackrecord-train       # MLflow run + UC-registered model + gold.delay_predictions
uv run trackrecord-workflow    # create + run the Databricks Job (bronze->silver->gold)
```

## Objects created

| Kind | Name |
|---|---|
| Schemas | `workspace.bronze` / `silver` / `gold` |
| Volume | `workspace.bronze.raw` (raw landing) |
| Bronze | `gtfs_*` (routes/trips/stops/stop_times/calendar/agency), `rt_trip_updates`, `rt_vehicle_positions`, `weather_sydney` |
| Silver | `silver.stop_delays` |
| Gold | `network_kpi`, `line_punctuality`, `hourly_punctuality`, `daily_punctuality`, `stop_punctuality`, `weather_delay`, `delay_predictions` |
| MLflow | experiment `/Users/<you>/trackrecord-delay`; UC model `workspace.gold.late5_classifier` |
| Workflow | Job **"TrackRecord — medallion refresh"** (3 serverless notebook tasks) |
| Dashboard | **TrackRecord — Sydney Rail Reliability** (AI/BI) |

## Limits to watch

- Usage caps **pause compute** for the day/month if exceeded; idle accounts can be reclaimed
  → open the workspace periodically. The 15-min cron only does capture + Volume upload (no
  compute) to stay clear of the quota; the medallion refresh is a separate scheduled Job.
