"""
pipeline_status.py
==================
Shared utility to write/read pipeline run status to a JSON log file
(data/pipeline_status.json) so both the Airflow DAG and the Streamlit
dashboard see the same state in real-time.
"""

from __future__ import annotations

import json
import os
import traceback
from datetime import datetime
from pathlib import Path


# ── Resolve the project root ──────────────────────────────────────────────────
def _project_root() -> Path:
    """Walk up from this file until we find a known project marker."""
    here = Path(__file__).resolve().parent
    for candidate in [here.parent, here]:
        if (candidate / "dashboard.py").exists() or (candidate / "dags").exists():
            return candidate
    return here.parent          # fallback


STATUS_FILE = _project_root() / "data" / "pipeline_status.json"


# ── Data structure ────────────────────────────────────────────────────────────
def _make_record(
    status: str,          # "RUNNING" | "SUCCESS" | "ERROR"
    dag_id: str,
    task_id: str,
    run_id: str,
    message: str,
    exception: str = "",
    try_number: int = 1,
) -> dict:
    return {
        "status":       status,
        "dag_id":       dag_id,
        "task_id":      task_id,
        "run_id":       run_id,
        "message":      message,
        "exception":    exception,
        "try_number":   try_number,
        "timestamp":    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
    }


# ── Writer helpers (called from DAG callbacks) ────────────────────────────────
def write_status(record: dict) -> None:
    """Atomically write the status record to pipeline_status.json."""
    try:
        STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATUS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(record, indent=2), encoding="utf-8")
        tmp.replace(STATUS_FILE)   # atomic on most OSes
    except Exception as exc:       # noqa: BLE001
        print(f"[pipeline_status] Could not write status file: {exc}")


def write_running(dag_id: str, task_id: str, run_id: str) -> None:
    write_status(_make_record("RUNNING", dag_id, task_id, run_id,
                              f"Task {task_id} is executing…"))


def write_success(dag_id: str, run_id: str) -> None:
    write_status(_make_record("SUCCESS", dag_id, "log_pipeline_finish", run_id,
                              "Pipeline completed successfully."))


def write_error(
    dag_id: str,
    task_id: str,
    run_id: str,
    exception,
    try_number: int = 1,
) -> None:
    if exception:
        try:
            exc_text = "".join(traceback.format_exception(exception))
        except TypeError:
            # Python < 3.10 fallback
            exc_text = "".join(
                traceback.format_exception(type(exception), exception, exception.__traceback__)  # type: ignore[arg-type]
            )
    else:
        exc_text = "Unknown error"


    write_status(_make_record(
        status      = "ERROR",
        dag_id      = dag_id,
        task_id     = task_id,
        run_id      = run_id,
        message     = f"Task '{task_id}' failed (attempt {try_number}).",
        exception   = exc_text,
        try_number  = try_number,
    ))


# ── Reader (called from the dashboard) ───────────────────────────────────────
def read_status() -> dict | None:
    """Return the latest pipeline status dict, or None if no file exists yet."""
    try:
        if STATUS_FILE.exists():
            return json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    except Exception:       # noqa: BLE001
        pass
    return None
