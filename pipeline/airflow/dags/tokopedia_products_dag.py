"""Hourly Tokopedia products pipeline + manual retry DAG.

Two DAGs share the same task definitions:
  - tokopedia_products  — @hourly, priority_weight=10 (always wins pool slot)
  - tokopedia_retry     — manual only, priority_weight=1  (triggered from Streamlit)

Both DAGs use ``pipeline_pool`` (1 slot) to serialize all pipeline tasks.
"""

import sys as _sys
from datetime import datetime, timedelta

if "/opt/airflow/repo" not in _sys.path:
    _sys.path.insert(0, "/opt/airflow/repo")

from airflow import DAG
from airflow.operators.bash import BashOperator

from pipeline.airflow.alerting import webhook_failure

REPO = "/opt/airflow/repo"
POOL = "pipeline_pool"


def _make_tasks(dag: DAG, priority: int) -> BashOperator:
    """Build all pipeline tasks for a DAG and wire their dependencies.

    Creates 8 ``BashOperator`` tasks (crawl → bronze → silver → quality_check →
    dbt_build → [load_postgres, load_clickhouse] → write_audit). All tasks
    use the same ``pipeline_pool`` pool for serialization — only one task from
    any DAG run executes at a time.

    Args:
        dag: The DAG instance to attach tasks to.
        priority: ``priority_weight`` for all tasks (10 = scheduled, 1 = manual retry).
            Higher priority wins the pool slot when multiple DAG runs are queued.

    Returns:
        The ``crawl`` ``BashOperator`` (entry point task).
    """
    crawl = BashOperator(
        task_id="crawl",
        pool=POOL,
        priority_weight=priority,
        env={
            "REPO": REPO,
            "CRAWL_KEYWORD": "{{ dag_run.conf.get('keyword', 'poco f8') }}",
            "CRAWL_MAX_PAGES": "{{ dag_run.conf.get('max_pages', 2) }}",
            "CRAWL_ASSET_ID": "{{ dag_run.conf.get('asset_id', '') }}",
            "KAFKA_TOPIC": "tokopedia.products.raw",
            "KAFKA_BOOTSTRAP": "kafka:29092",
            "CONTROL_DSN": "host=postgres port=5432 dbname=mart user=mart password=mart",
        },
        append_env=True,
        bash_command=(
            f"sleep $((RANDOM % 120)) && "
            f"cd {REPO} && python -m pipeline.load.crawl_assets"
        ),
    )
    bronze = BashOperator(
        task_id="bronze",
        pool=POOL,
        priority_weight=priority,
        bash_command=f"cd {REPO} && python -m pipeline.spark.stream_bronze",
    )
    silver = BashOperator(
        task_id="silver",
        pool=POOL,
        priority_weight=priority,
        bash_command=f"cd {REPO} && python -m pipeline.spark.silver",
    )
    quality_check = BashOperator(
        task_id="quality_check",
        pool=POOL,
        priority_weight=priority,
        bash_command=f"cd {REPO} && python -m pipeline.quality.checks",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        pool=POOL,
        priority_weight=priority,
        bash_command=f"cd {REPO}/pipeline/dbt && dbt build --profiles-dir .",
    )
    load_postgres = BashOperator(
        task_id="load_postgres",
        pool=POOL,
        priority_weight=priority,
        bash_command=f"cd {REPO} && python -m pipeline.load.load_to_postgres",
    )
    load_clickhouse = BashOperator(
        task_id="load_clickhouse",
        pool=POOL,
        priority_weight=priority,
        bash_command=f"cd {REPO} && python -m pipeline.load.load_to_clickhouse",
    )
    write_audit = BashOperator(
        task_id="write_audit",
        pool=POOL,
        priority_weight=priority,
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
    return crawl


# ---- DAG: Scheduled @hourly ----
with DAG(
    dag_id="tokopedia_products",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    on_failure_callback=webhook_failure,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
) as scheduled_dag:
    _make_tasks(scheduled_dag, priority=10)

# ---- DAG: Manual retry (Streamlit) ----
with DAG(
    dag_id="tokopedia_retry",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # manual trigger only
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    on_failure_callback=webhook_failure,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
) as retry_dag:
    _make_tasks(retry_dag, priority=1)
