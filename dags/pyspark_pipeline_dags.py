"""
pyspark_pipeline_dags.py
========================
Two DAGs:
  1. daily_pyspark_pipeline       — scheduled 05:00 UTC daily
  2. event_driven_pyspark_pipeline — triggers when a new file lands in data/input/

Both DAGs:
  • Write a pipeline_status.json on START / SUCCESS / ERROR so the
    Streamlit dashboard shows a live status banner.
  • Send an email via Resend on SUCCESS and immediately on ERROR
    (before retries, so the owner knows right away).
"""

from __future__ import annotations

import os
import sys
import traceback
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator    # pyright: ignore[reportMissingImports]
from airflow.sensors.filesystem import FileSensor      # pyright: ignore[reportMissingImports]

# ── Resolve project root and put it on the path ──────────────────────────────
_DAGS_DIR    = Path(__file__).resolve().parent        # .../dags/
_PROJECT_ROOT = _DAGS_DIR.parent                      # .../Data-Engineering-Pipeline/
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ── Import shared utilities (graceful fallback if not installed) ──────────────
try:
    from utils.notifications import send_pipeline_notification
except ImportError:
    def send_pipeline_notification(*args, **kwargs):  # type: ignore[misc]
        """No-op when resend / utils is unavailable."""

try:
    from utils.pipeline_status import write_running, write_success, write_error
except ImportError:
    def write_running(*args, **kwargs):  # type: ignore[misc]
        pass
    def write_success(*args, **kwargs):  # type: ignore[misc]
        pass
    def write_error(*args, **kwargs):  # type: ignore[misc]
        pass

# ── Constants ─────────────────────────────────────────────────────────────────
PROJECT_ROOT     = os.environ.get("PROJECT_ROOT", str(_PROJECT_ROOT))
SPARK_JOB_PATH   = os.environ.get(
    "SPARK_JOB_PATH",
    os.path.join(PROJECT_ROOT, "spark_jobs", "medallion_pipeline.py"),
)
INPUT_FILE_GLOB  = os.environ.get("INPUT_FILE_GLOB", "/data/input/*")
FS_CONN_ID       = os.environ.get("FS_CONN_ID", "fs_default")

SPARK_SUBMIT_CMD = [
    "/opt/spark/bin/spark-submit",
    "--packages", "io.delta:delta-spark_2.12:3.2.0",
    "--conf", "spark.jars.ivy=/tmp/.ivy2",
    "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
]


# ── Task callables ─────────────────────────────────────────────────────────────
def log_pipeline_start(**context) -> None:
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    ti     = context["ti"]

    msg = f"Pipeline started | dag={dag_id} | run={run_id}"
    ti.log.info(msg)
    write_running(dag_id, ti.task_id, run_id)


def log_pipeline_finish(**context) -> None:
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    ti     = context["ti"]

    msg = f"Pipeline finished successfully | dag={dag_id} | run={run_id}"
    ti.log.info(msg)

    write_success(dag_id, run_id)

    send_pipeline_notification(
        status  = "SUCCESS",
        message = f"Pipeline '{dag_id}' completed successfully.",
        details = f"Run ID  : {run_id}\nSchedule: automatic",
    )


def on_task_failure(context) -> None:
    """
    Airflow `on_failure_callback`.
    Called immediately when a task fails (before any retry delay).
    1. Writes ERROR to the shared status file → dashboard shows red banner.
    2. Sends an email with full traceback to the owner.
    """
    ti        = context["task_instance"]
    exception = context.get("exception")

    # 1 ── Capture traceback
    if exception:
        tb_lines = traceback.format_exception(
            type(exception), exception, exception.__traceback__
        )
        tb_text = "".join(tb_lines)
    else:
        tb_text = "No traceback available."

    summary  = (
        f"Task '{ti.task_id}' in DAG '{ti.dag_id}' failed "
        f"(attempt {ti.try_number} of {ti.max_tries + 1})."
    )
    details = (
        f"DAG ID    : {ti.dag_id}\n"
        f"Task ID   : {ti.task_id}\n"
        f"Run ID    : {ti.run_id}\n"
        f"Attempt   : {ti.try_number} / {ti.max_tries + 1}\n"
        f"Exception : {str(exception)}\n\n"
        f"─── Full Traceback ───\n{tb_text}"
    )

    ti.log.error("[PIPELINE FAILURE] %s\n%s", summary, details)

    # 2 ── Write to shared status file → dashboard picks this up immediately
    write_error(
        dag_id     = ti.dag_id,
        task_id    = ti.task_id,
        run_id     = ti.run_id,
        exception  = exception,
        try_number = ti.try_number,
    )

    # 3 ── Send email immediately (fire-and-forget; don't let email failure
    #       mask the original pipeline error)
    try:
        send_pipeline_notification(
            status  = "ERROR",
            message = summary,
            details = details,
        )
    except Exception as notify_exc:  # noqa: BLE001
        ti.log.warning("Email notification failed: %s", notify_exc)


# ── Spark runner ───────────────────────────────────────────────────────────────
def _submit_to_spark(script_path: str, context: dict) -> None:
    """Run a PySpark script inside the running Spark container via Docker SDK."""
    import docker  # lazy import

    ti_log = context["ti"].log
    ti_log.info("[PIPELINE] Submitting → %s", script_path)

    client = docker.from_env()
    try:
        container = client.containers.get("spark")
    except docker.errors.NotFound as exc:
        raise RuntimeError(
            "Spark container 'spark' is not running. "
            "Start it with: docker compose up -d spark"
        ) from exc

    exec_id = client.api.exec_create(
        container.id, SPARK_SUBMIT_CMD + [script_path]
    )["Id"]

    for chunk in client.api.exec_start(exec_id, stream=True):
        for line in chunk.decode("utf-8", errors="replace").splitlines():
            if line.strip():
                ti_log.info(line)

    exit_code = client.api.exec_inspect(exec_id)["ExitCode"]
    ti_log.info("[PIPELINE] Done | exit_code=%s | script=%s", exit_code, script_path)

    if exit_code != 0:
        raise RuntimeError(
            f"spark-submit exited with code {exit_code} for: {script_path}"
        )


def _run_spark_job(**context) -> None:
    _submit_to_spark(SPARK_JOB_PATH, context)


# ── Shared default args ────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":           "data-eng",
    "depends_on_past": False,
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
    # Global fallback — individual tasks override with on_failure_callback
    "on_failure_callback": on_task_failure,
}


# ══════════════════════════════════════════════════════════════════════════════
# DAG 1 — Daily scheduled pipeline
# ══════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id      = "daily_pyspark_pipeline",
    description = "Runs the PySpark Delta pipeline daily at 5 AM UTC",
    start_date  = pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule    = "0 5 * * *",
    catchup     = False,
    default_args = DEFAULT_ARGS,
    tags         = ["spark", "delta", "scheduled"],
) as daily_dag:

    d_start = PythonOperator(
        task_id         = "log_pipeline_start",
        python_callable = log_pipeline_start,
    )

    d_spark = PythonOperator(
        task_id              = "run_process_data",
        python_callable      = _run_spark_job,
        on_failure_callback  = on_task_failure,  # ensure immediate callback
    )

    d_finish = PythonOperator(
        task_id         = "log_pipeline_finish",
        python_callable = log_pipeline_finish,
        trigger_rule    = "all_done",            # runs even if upstream failed
    )

    d_start >> d_spark >> d_finish


# ══════════════════════════════════════════════════════════════════════════════
# DAG 2 — Event-driven pipeline (FileSensor)
# ══════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id      = "event_driven_pyspark_pipeline",
    description = "Triggers when a new file appears in data/input/",
    start_date  = pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule    = "* * * * *",
    catchup     = False,
    default_args = DEFAULT_ARGS,
    tags         = ["spark", "delta", "event-driven", "sensor"],
) as event_dag:

    e_sensor = FileSensor(
        task_id      = "detect_new_input_file",
        fs_conn_id   = FS_CONN_ID,
        filepath     = INPUT_FILE_GLOB,
        poke_interval = 1,
        timeout      = 3600,
        mode         = "poke",
        on_failure_callback = on_task_failure,
    )

    e_start = PythonOperator(
        task_id         = "log_pipeline_start",
        python_callable = log_pipeline_start,
    )

    e_spark = PythonOperator(
        task_id             = "run_process_data",
        python_callable     = _run_spark_job,
        on_failure_callback = on_task_failure,
    )

    e_finish = PythonOperator(
        task_id         = "log_pipeline_finish",
        python_callable = log_pipeline_finish,
        trigger_rule    = "all_done",
    )

    e_sensor >> e_start >> e_spark >> e_finish
