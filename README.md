# TrackRecord — Sydney's public-transport reliability, measured end-to-end

A **data-engineering / lakehouse** project: Transport for NSW **GTFS-Realtime** feeds →
a **Databricks medallion** (Bronze → Silver → Gold on **Delta Lake + Unity Catalog**) →
an **MLflow** delay-prediction model and a **Databricks AI/BI** dashboard.

The story: **how on-time Sydney's network really is** — actual vs scheduled stop times —
**which lines and times of day are worst**, and **how much rain adds to delays**.

> Portfolio project 3 of 3 — the data-engineering lane.
> (1: *Global Pulse*, an AI/RAG news agent. 2: *GridLens*, an analytics-engineering pipeline on the NEM.)

## Architecture

![TrackRecord architecture](docs/architecture.svg)

Ingestion runs **outside** Databricks (your Mac + a GitHub Actions cron) and lands raw files
into a **Unity Catalog Volume** — Databricks Free Edition restricts a notebook's outbound
internet to an allowlist of trusted domains, so a serverless notebook can't be relied on to
call the TfNSW API directly. This is also just a clean raw-landing-zone pattern.

## Stack

| Layer | Choice | Why |
|---|---|---|
| **Ingestion** | Python + `httpx` + `gtfs-realtime-bindings` (managed by `uv`) | Pull GTFS-RT protobuf, parse to rows/Parquet |
| **Scheduler** | GitHub Actions cron | Free always-on poller for the Sydney Trains live feed |
| **Lakehouse** | **Databricks Free Edition** (serverless) | Unity Catalog + Delta + Workflows + DBSQL, $0 |
| **Storage** | **Delta Lake**, medallion (bronze/silver/gold) | versioned tables — the core DE pattern |
| **Governance** | **Unity Catalog** | catalog / schema / volume, lineage, permissions |
| **Orchestration** | **Databricks Workflows** | one job: ingest → bronze → silver → gold → score |
| **ML** | **MLflow** | track + register a ">5 min late" classifier; batch score |
| **Dashboard** | **Databricks AI/BI** | web-based, Mac-friendly (no Power BI / Windows dependency) |
| **Weather** | Bureau of Meteorology (BOM) | rain/temp join for the "rain adds Y minutes" angle |

## Data sources (verified, honestly labelled)

**Transport for NSW Open Data Hub** — free, requires an API key.
- Auth header `Authorization: apikey <key>` (not Bearer). **Bronze plan: 60,000 calls/day, 5 req/sec** (verified June 2026).
- **GTFS-Realtime Trip Updates** (binary protobuf) — predicted/actual stop times → the delay signal.
- **Static GTFS** schedule bundles — the scheduled times we measure against.

Two honest constraints shaped the design (both verified June 2026):
- ⚠️ The **"Historical GTFS and GTFS Realtime"** backfill dataset currently covers **Metro + Ferry only** ("other modes over time"). So TrackRecord uses a **hybrid** backbone: build immediately on **Metro + Ferry historical** data, while a **Sydney Trains live collector** accumulates real trip updates for the headline reliability story.
- ⚠️ Databricks Free Edition is **serverless-only**, caps usage (overruns pause compute for the day), and **restricts notebook outbound internet** — hence the external ingestion above.

**Bureau of Meteorology** — public Sydney observations, joined in Silver for the rain-vs-delay analysis.

Attribution: contains data sourced from Transport for NSW and the Bureau of Meteorology. Used for non-commercial, educational purposes.

## Phases

- [x] **Phase 0 — Setup & verify.** Repo + `uv` env (Py 3.12); TfNSW key + Databricks Free Edition created. Endpoints verified **live**: Sydney Trains trip updates on **`/v2/gtfs/realtime/sydneytrains`** (v1 → 404), vehicle positions on `/v2/gtfs/vehiclepos/...`, static GTFS on `/v1/gtfs/schedule/...`. *Live smoke test: 446 trip updates parsed, feed @ 2026-06-18 10:20 AEST.*
- [~] **Phase 1 — Bronze.** Local ingesters built (`trackrecord-collect`, `trackrecord-gtfs`) → raw Parquet → **UC Volume** (`workspace.bronze.raw`) → **Bronze Delta** via `trackrecord-bronze`. Tables: `gtfs_routes` 137, `gtfs_trips` 64,995, `gtfs_stops` 1,214, `gtfs_stop_times` 1,187,560, `gtfs_calendar` 121, `rt_trip_updates` 3,699, `rt_vehicle_positions` 216. *Quirk: Free Edition blocks new catalogs (`InvalidState`) → we use the pre-provisioned `workspace` catalog. Remaining: GitHub Actions cron for ongoing live capture; Metro/Ferry historical backfill.*
- [ ] **Phase 2 — Silver.** delay = actual − scheduled per stop; clean/dedupe; data-quality checks; BOM weather join.
- [ ] **Phase 3 — Gold + dashboard.** OTP by line/hour/day/station; AI/BI dashboard; headline stat.
- [ ] **Phase 4 — ML (MLflow).** ">5 min late" classifier; track + register; batch score.
- [ ] **Phase 5 — Orchestrate & polish.** Databricks Workflow; `FINDINGS.md` write-up; final diagram.

## Quickstart (local)

```bash
uv sync                              # create .venv + install deps
uv run trackrecord-smoke --selftest  # offline: verify the protobuf toolchain

cp .env.example .env                 # paste your TfNSW API key
uv run trackrecord-smoke             # live: Sydney Trains trip updates
```
