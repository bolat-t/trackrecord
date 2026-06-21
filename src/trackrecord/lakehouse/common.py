"""Shared Databricks helpers: client, SQL warehouse, statement execution."""

from __future__ import annotations

import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from .. import config  # noqa: F401  imported for its load_dotenv() side-effect


def client() -> WorkspaceClient:
    """WorkspaceClient auth'd from DATABRICKS_HOST/TOKEN (loaded from .env by config)."""
    return WorkspaceClient()


def warehouse_id(w: WorkspaceClient) -> str:
    for wh in w.warehouses.list():
        return wh.id
    raise RuntimeError("no SQL warehouse available in this workspace")


def run_sql(w: WorkspaceClient, wid: str, statement: str, timeout_s: int = 240):
    """Execute a SQL statement on a serverless warehouse, polling to completion.

    The warehouse auto-starts on the first call, so the first statement may take
    longer while it spins up.
    """
    resp = w.statement_execution.execute_statement(
        warehouse_id=wid, statement=statement, wait_timeout="30s",
    )
    deadline = time.time() + timeout_s
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.time() > deadline:
            raise TimeoutError(f"SQL timed out after {timeout_s}s")
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error
        msg = getattr(err, "message", err)
        raise RuntimeError(f"SQL failed ({resp.status.state}): {msg}\n--\n{statement[:300]}")
    return resp


def rows(resp) -> list[list]:
    res = resp.result
    return list(res.data_array) if res and res.data_array else []


def df(resp):
    """Build a pandas DataFrame from a statement-execution response."""
    import pandas as pd

    cols = [c.name for c in resp.manifest.schema.columns]
    return pd.DataFrame(rows(resp), columns=cols)
