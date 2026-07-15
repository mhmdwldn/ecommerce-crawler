"""Hourly Tokopedia products pipeline: crawl -> Kafka -> bronze -> silver -> gold -> mart.

Keyword and max_pages are managed via Airflow Variables (UI or CLI):
    airflow variables set crawl_keyword "poco f8"
    airflow variables set crawl_max_pages 2

Trigger-time overrides still work via dag_run.conf (takes precedence).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = "/opt/airflow/repo"

with DAG(
    dag_id="tokopedia_products",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
) as dag:
    crawl = BashOperator(
        task_id="crawl",
        env={
            "CRAWL_KEYWORD": (
                "{{ dag_run.conf.get('keyword', var.value.get('crawl_keyword', 'poco f8')) }}"
            ),
            "CRAWL_MAX_PAGES": (
                "{{ dag_run.conf.get('max_pages', var.value.get('crawl_max_pages', 2)) }}"
            ),
            "KAFKA_TOPIC": "tokopedia.products.raw",
            "KAFKA_BOOTSTRAP": "kafka:29092",
        },
        append_env=True,
        bash_command=(
            # ponytail: random sleep 0-300s jitter — avoids all hourly runs hitting
            # the API at exactly :00. Remove when multiple keywords spread load naturally.
            f"sleep $((RANDOM % 300)) && "
            f"cd {REPO}/source && python main.py crawler --mode full --type search-product "
            f'--keyword "$CRAWL_KEYWORD" --max-pages "$CRAWL_MAX_PAGES" '
            f"-d kafka -o $KAFKA_TOPIC --bootstrap-servers $KAFKA_BOOTSTRAP"
        ),
    )
    bronze = BashOperator(
        task_id="bronze",
        bash_command=f"cd {REPO} && python -m pipeline.spark.stream_bronze",
    )
    silver = BashOperator(
        task_id="silver",
        bash_command=f"cd {REPO} && python -m pipeline.spark.silver",
    )
    quality_check = BashOperator(
        task_id="quality_check",
        bash_command=f"cd {REPO} && python -m pipeline.quality.checks",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {REPO}/pipeline/dbt && dbt build --profiles-dir .",
    )
    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command=f"cd {REPO} && python -m pipeline.load.load_to_postgres",
    )
    load_clickhouse = BashOperator(
        task_id="load_clickhouse",
        bash_command=f"cd {REPO} && python -m pipeline.load.load_to_clickhouse",
    )
    write_audit = BashOperator(
        task_id="write_audit",
        env={
            "AIRFLOW_RUN_ID": "{{ run_id }}",
            "AIRFLOW_LOGICAL_DATE": "{{ logical_date }}",
            "AIRFLOW_TASK_STATE": "{{ dag_run.get_state() if dag_run else 'unknown' }}",
        },
        append_env=True,
        bash_command=f"cd {REPO} && python -m pipeline.quality.audit",
        trigger_rule="all_done",
    )

    crawl >> bronze >> silver >> quality_check >> dbt_build >> [load_postgres, load_clickhouse] >> write_audit
