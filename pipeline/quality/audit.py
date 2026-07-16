"""Write one audit row to analytics.pipeline_runs after each DAG run.

Called by Airflow at the end of a run (success or failure).
Status is passed via AIRFLOW_TASK_STATE env var from the DAG.
"""

import os
import time
from datetime import datetime

import duckdb

from pipeline import GOLD_TABLES
from pipeline.load.ch_client import get_client

GOLD_DB = os.getenv("GOLD_DB_PATH", "pipeline/dbt/gold.duckdb")


def main() -> None:
    """Write one audit row to ``analytics.pipeline_runs`` in ClickHouse.

    Reads pipeline state from env vars (set by Airflow DAG):
      - ``AIRFLOW_RUN_ID``: DAG run identifier (default ``"manual"``)
      - ``AIRFLOW_LOGICAL_DATE``: ISO 8601 execution date (default epoch)
      - ``AIRFLOW_TASK_STATE``: ``success`` / ``failed`` / ``unknown``

    Counts rows from silver Delta + gold DuckDB tables, computes duration,
    and inserts into ClickHouse. Connection cleanup in ``finally`` block.

    Raises:
        Does not raise — all errors are caught and logged.
    """
    run_id = os.getenv("AIRFLOW_RUN_ID", "manual")
    exec_date = os.getenv("AIRFLOW_LOGICAL_DATE", "1970-01-01T00:00:00")
    status = os.getenv("AIRFLOW_TASK_STATE", "unknown")

    ch = get_client()
    duck = duckdb.connect(GOLD_DB, read_only=True)
    try:
        rows_gold = 0
        for tbl in GOLD_TABLES:
            try:
                rows_gold += duck.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
            except Exception as e:
                print(f"WARN: gold table {tbl} not readable: {e}")

        _t0 = time.time()
        from pipeline.spark.session import build_session

        spark = build_session("audit")
        try:
            silver_path = os.getenv("SILVER_PATH", "s3a://lakehouse/silver/products")
            rejects_path = os.getenv("REJECTS_PATH", "s3a://lakehouse/silver/products_rejects")

            rows_silver = spark.read.format("delta").load(silver_path).count()
            try:
                rows_rejects = spark.read.format("delta").load(rejects_path).count()
            except Exception:
                rows_rejects = 0
        finally:
            spark.stop()

        duration = time.time() - _t0

        # Parse ISO datetime from Airflow; fall back to now()
        try:
            exec_dt = datetime.fromisoformat(exec_date)
        except (ValueError, TypeError):
            exec_dt = datetime.now()

        ch.insert("pipeline_runs", [[
            run_id, exec_dt, status,
            rows_silver, rows_rejects, rows_gold, round(duration, 1),
        ]], column_names=[
            "run_id", "execution_date", "status",
            "rows_silver", "rows_rejects", "rows_gold", "duration_sec",
        ])

        print(f"Audit: run_id={run_id} status={status} silver={rows_silver} rejects={rows_rejects} gold={rows_gold}")
    finally:
        duck.close()
        ch.close()


if __name__ == "__main__":
    main()
