# Findings — TrackRecord

Measuring how on-time Sydney's trains really are, from TfNSW GTFS-Realtime, modelled
as a Databricks medallion lakehouse.

## Headline (snapshot — read the caveat)

From the current capture window (~4,600 stop-events, 96 of them >5-min-late):

- **On-time (≤5 min): ~99.6%** across the suburban T-line network; mean delay **0.26 min**.
- **Least-reliable suburban line: T4** (Cronulla ↔ Bondi Junction via City) — highest average and P90 delay.
- **Worst station: Cronulla** — fittingly, T4's terminus.
- **Least reliable overall = NSW TrainLink intercity:** South Coast (SCO), Southern Highlands (SHL) and Central Coast & Newcastle (CCN) carry the biggest delays — and the delay-risk model independently flags exactly these (plus T4) as highest-risk.
- **Rain vs delay:** no rain has fallen in the capture window yet, so the "rain adds Y minutes" figure is **pending real wet weather** — the join and feature are built and will populate automatically.

## ⚠️ Honest caveat

These are a **single-day, mostly off-peak snapshot**. A GitHub Actions cron captures the
live feed every 15 minutes, so every table, the dashboard and the model refresh from the
accumulating data with no rework — figures sharpen toward AM/PM peaks and multi-day
patterns over time. The model metrics below (AUC 0.97 / recall 1.0 / **precision 0.19**)
are **provisional and optimistic**, driven by only 96 positive examples in one day.

## The delay-risk model

A `>5-min-late` classifier (LogisticRegression, balanced) on line, scheduled hour,
day-of-week, route type and rain — tracked in MLflow and registered in Unity Catalog
(`workspace.gold.late5_classifier`), then batch-scored to `gold.delay_predictions`.
Highest predicted-risk lines: **SCO, SHL, CCN, T4** — consistent with the observed data.

## Data-engineering decisions & quirks solved

- **Endpoint versioning:** Sydney Trains GTFS-RT trip updates are on **`/v2/`** (`/v1/` → 404); static GTFS stays on `/v1/`.
- **RT ↔ static join:** the realtime `stop_sequence` is sparse/NULL, so scheduled times join on **`(trip_id, stop_id)`** (98% coverage) rather than sequence.
- **Delay signal:** `coalesce(arrival_delay, departure_delay)` — TfNSW pre-computes actual − scheduled (true NULLs, no NaN).
- **Service date** is derived from capture time in `Australia/Sydney` (the feed leaves `start_date` empty).
- **Non-revenue** repositioning runs (`RTTA_*`) are filtered out.
- **Free Edition:** new Unity Catalog catalogs are blocked (`InvalidState`) → the `workspace` catalog is used; serverless egress is allowlisted → ingestion runs externally and lands raw in a UC Volume.
- **Weather:** BOM's JSON blocks automated pulls, so **Open-Meteo** (free, hourly precipitation) is the source.
- **AI/BI (Lakeview):** a widget's query must be named `main_query` and linked via `spec.data.queryName`, or the server silently drops the fields and encodings.

## Architecture

External ingestion (Mac / GitHub Actions cron) → **UC Volume** → **Bronze** → **Silver**
(delay + weather) → **Gold** (aggregates + predictions), orchestrated by a **Databricks
Workflow**; an **MLflow** model registered in **Unity Catalog**; a **Databricks AI/BI**
dashboard. See [README](README.md) and [docs/architecture.svg](docs/architecture.svg).
