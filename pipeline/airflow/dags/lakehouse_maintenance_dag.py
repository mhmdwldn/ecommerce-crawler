"""Weekly lakehouse maintenance: OPTIMIZE + VACUUM bronze & silver Delta tables,
OPTIMIZE FINAL on ClickHouse ReplacingMergeTree dims (product, shop, category)."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = "/opt/airflow/repo"

with DAG(
    dag_id="lakehouse_maintenance",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
) as dag:
    optimize_bronze = BashOperator(
        task_id="optimize_bronze",
        bash_command=f"cd {REPO} && python -m pipeline.spark.maintenance bronze",
    )
    optimize_silver = BashOperator(
        task_id="optimize_silver",
        bash_command=f"cd {REPO} && python -m pipeline.spark.maintenance silver",
    )
    optimize_clickhouse = BashOperator(
        task_id="optimize_clickhouse",
        bash_command=(
            f"cd {REPO} && python -c \""
            "from pipeline.load.ch_client import get_client; "
            "ch = get_client(); "
            "ch.command('OPTIMIZE TABLE analytics.dim_product FINAL'); "
            "ch.command('OPTIMIZE TABLE analytics.dim_shop FINAL'); "
            "ch.command('OPTIMIZE TABLE analytics.dim_category FINAL'); "
            "print('ClickHouse OPTIMIZE FINAL done (product, shop, category)'); "
            'ch.close()"'
        ),
    )

    optimize_bronze >> optimize_silver >> optimize_clickhouse
