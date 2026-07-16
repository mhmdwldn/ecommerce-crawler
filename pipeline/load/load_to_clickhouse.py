"""Load gold tables from DuckDB into ClickHouse serving layer.

ponytail: full reload for dims (ReplacingMergeTree handles dedup),
truncate-partition-insert for fct. At current scale (~200 rows)
this is instant; switch to incremental MERGE when rows > 10M.
"""

import os

import duckdb

from pipeline import GOLD_TABLES
from pipeline.load.ch_client import get_client

CH_DB = os.getenv("CLICKHOUSE_DB", "analytics")

# Engine per table: determines idempotency strategy.
#   MergeTree → truncate-partition-insert (fct)
#   ReplacingMergeTree → insert-only + OPTIMIZE FINAL in maintenance DAG (dims)
_TABLE_ENGINE: dict[str, str] = {
    "dim_product": "ReplacingMergeTree",
    "dim_shop": "ReplacingMergeTree",
    "dim_category": "ReplacingMergeTree",
    "fct_product_snapshot": "MergeTree",
}

_TABLES_WITH_PARTITION = {"fct_product_snapshot"}


def main() -> None:
    gold_db = os.getenv("GOLD_DB_PATH", "pipeline/dbt/gold.duckdb")

    duck = duckdb.connect(gold_db, read_only=True)
    ch = get_client()
    try:
        for table in GOLD_TABLES:
            rows = duck.execute(f"SELECT * FROM {table}").fetchall()
            cols = [d[0] for d in duck.description]

            if not rows:
                print(f"loaded {table}: 0 rows (empty source)")
                continue

            if table in _TABLES_WITH_PARTITION:
                # MergeTree: drop partitions that overlap with gold data, then insert.
                months = duck.execute(
                    "SELECT DISTINCT strftime(crawled_at, '%Y%m') FROM fct_product_snapshot"
                ).fetchall()
                for (m,) in months:
                    try:
                        ch.command(f"ALTER TABLE {CH_DB}.{table} DROP PARTITION '{m}'")
                    except Exception:
                        pass  # partition doesn't exist yet — first load of the month
                ch.insert(table, rows, column_names=cols)
            else:
                # ReplacingMergeTree: insert-only — OPTIMIZE FINAL in maintenance DAG.
                ch.insert(table, rows, column_names=cols)

            count = ch.query(f"SELECT count() FROM {table}").first_row[0]
            print(f"loaded {table}: {count} rows")
    finally:
        duck.close()
        ch.close()


if __name__ == "__main__":
    main()
