"""Hourly Tokopedia products pipeline: crawl -> Kafka -> bronze -> silver -> gold -> mart.

Crawl targets managed via Asset Registry (Postgres control.crawl_assets).
Add/edit keywords: Streamlit UI (assets/app.py) or seed YAML (assets/seeds/targets.yaml).
Fallback: dag_run.conf keyword if registry is empty.
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
    max_active_tasks=2,  # konservatif — batasi paralel crawl (2.5.5)
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
) as dag:
    crawl = BashOperator(
        task_id="crawl",
        env={
            "REPO": REPO,
            "CRAWL_KEYWORD": "{{ dag_run.conf.get('keyword', 'poco f8') }}",
            "CRAWL_MAX_PAGES": "{{ dag_run.conf.get('max_pages', 2) }}",
            "KAFKA_TOPIC": "tokopedia.products.raw",
            "KAFKA_BOOTSTRAP": "kafka:29092",
            "CONTROL_DSN": "host=postgres port=5432 dbname=mart user=mart password=mart",
        },
        append_env=True,
        bash_command=(
            # ponytail: random sleep 0-120s jitter — reduced from 300s since
            # multiple keywords naturally spread load across the hour.
            f"sleep $((RANDOM % 120)) && "
            f"cd {REPO} && python -m pipeline.load.crawl_assets"
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
