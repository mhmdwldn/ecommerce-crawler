"""Monthly data retention: prune Delta tables beyond retention window."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = "/opt/airflow/repo"

with DAG(
    dag_id="data_retention",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
) as dag:
    retention_bronze = BashOperator(
        task_id="retention_bronze",
        bash_command=f"cd {REPO} && python -m pipeline.spark.retention --bronze-hours 2160",
    )
    retention_silver = BashOperator(
        task_id="retention_silver",
        bash_command=f"cd {REPO} && python -m pipeline.spark.retention --silver-hours 4320",
    )

    retention_bronze >> retention_silver
