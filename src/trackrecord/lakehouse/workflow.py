"""Phase 5: a Databricks Workflow (multi-task Job) that refreshes the medallion
bronze -> silver -> gold on serverless, reading the Volume the cron populates.

Notebooks are generated from the SAME SQL the local modules use (single source of
truth), uploaded to the workspace, then wired into a 3-task Job with
dependencies and run once to prove it. Ingestion stays external (GitHub Actions
cron — Free Edition egress limits); model training is the separate MLflow job.

    uv run trackrecord-workflow
"""

from __future__ import annotations

import os
import time

from databricks.sdk.service.jobs import (CronSchedule, JobSettings, NotebookTask,
                                          PauseStatus, Task, TaskDependency)
from databricks.sdk.service.workspace import ImportFormat, Language

from .common import client
from .gold import STATEMENTS as GOLD
from .silver import SILVER_SQL

CAT = os.environ.get("TR_CATALOG", "workspace")
JOB_NAME = "NSW Public Transport Tracker — medallion refresh"
VROOT = f"/Volumes/{CAT}/bronze/raw"
# Daily at 06:00 Sydney — rebuilds the medallion from the Volume the cron fills.
SCHEDULE = CronSchedule(quartz_cron_expression="0 0 6 * * ?",
                        timezone_id="Australia/Sydney", pause_status=PauseStatus.UNPAUSED)

BRONZE_STMTS = [
    f"CREATE OR REPLACE TABLE {CAT}.bronze.rt_trip_updates AS "
    f"SELECT * FROM read_files('{VROOT}/realtime/sydneytrains/tripupdate/', format => 'parquet')",
    f"CREATE OR REPLACE TABLE {CAT}.bronze.rt_vehicle_positions AS "
    f"SELECT * FROM read_files('{VROOT}/realtime/sydneytrains/vehiclepos/', format => 'parquet')",
    f"CREATE OR REPLACE TABLE {CAT}.bronze.weather_sydney AS "
    f"SELECT * FROM read_files('{VROOT}/weather/sydney/weather.parquet', format => 'parquet')",
]


def _notebook_source(statements: list[str]) -> bytes:
    lines = ["# Databricks notebook source"]
    for s in statements:
        lines.append('spark.sql("""' + s.strip() + '""")')
    lines.append('print("ok")')
    return ("\n\n".join(lines) + "\n").encode()


def main(argv: list[str] | None = None) -> int:
    w = client()
    user = w.current_user.me().user_name
    base = f"/Users/{user}/trackrecord"
    w.workspace.mkdirs(base)

    notebooks = {
        "bronze_refresh": BRONZE_STMTS,
        "silver_build": [SILVER_SQL],
        "gold_build": list(GOLD.values()),
    }
    for name, stmts in notebooks.items():
        w.workspace.upload(f"{base}/{name}", _notebook_source(stmts),
                           format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=True)
        print(f"uploaded notebook {base}/{name}")

    tasks = [
        Task(task_key="bronze_refresh",
             notebook_task=NotebookTask(notebook_path=f"{base}/bronze_refresh")),
        Task(task_key="silver_build",
             notebook_task=NotebookTask(notebook_path=f"{base}/silver_build"),
             depends_on=[TaskDependency(task_key="bronze_refresh")]),
        Task(task_key="gold_build",
             notebook_task=NotebookTask(notebook_path=f"{base}/gold_build"),
             depends_on=[TaskDependency(task_key="silver_build")]),
    ]

    existing = next((j for j in w.jobs.list(name=JOB_NAME) if j.settings.name == JOB_NAME), None)
    if existing:
        job_id = existing.job_id
        w.jobs.reset(job_id=job_id,
                     new_settings=JobSettings(name=JOB_NAME, tasks=tasks, schedule=SCHEDULE))
        print(f"updated job {job_id}")
    else:
        job_id = w.jobs.create(name=JOB_NAME, tasks=tasks, schedule=SCHEDULE).job_id
        print(f"created job {job_id}")

    run = w.jobs.run_now(job_id=job_id)
    print(f"started run {run.run_id}; waiting...")
    deadline = time.time() + 600
    while time.time() < deadline:
        r = w.jobs.get_run(run_id=run.run_id)
        state = r.state
        life = state.life_cycle_state
        if str(life) in ("RunLifeCycleState.TERMINATED", "TERMINATED"):
            print(f"result: {state.result_state}")
            print(f"URL: {w.config.host}/jobs/{job_id}/runs/{run.run_id}")
            return 0 if str(state.result_state).endswith("SUCCESS") else 1
        time.sleep(10)
    print("timed out waiting for run")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
