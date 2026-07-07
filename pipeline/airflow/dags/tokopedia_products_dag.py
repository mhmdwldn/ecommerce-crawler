"""Daily Tokopedia products pipeline: crawl -> Kafka -> bronze -> silver -> gold -> mart."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = "/opt/airflow/repo"
KEYWORD = "{{ dag_run.conf.get('keyword', 'poco f8') }}"
MAX_PAGES = "{{ dag_run.conf.get('max_pages', 2) }}"

with DAG(
    dag_id="tokopedia_products",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1},
) as dag:
    crawl = BashOperator(
        task_id="crawl",
        bash_command=(
            f"cd {REPO}/source && python main.py crawler --mode full --type search-product "
            f"--keyword '{KEYWORD}' --max-pages {MAX_PAGES} "
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
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {REPO}/pipeline/dbt && dbt build --profiles-dir .",
    )
    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command=f"cd {REPO} && python -m pipeline.load.load_to_postgres",
    )

    crawl >> bronze >> silver >> dbt_build >> load_postgres
