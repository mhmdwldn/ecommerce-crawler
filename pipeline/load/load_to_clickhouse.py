"""Load gold tables from DuckDB into ClickHouse serving layer.

ponytail: full reload for dims (ReplacingMergeTree handles dedup),
truncate-partition-insert for fct. At current scale (~200 rows)
this is instant; switch to incremental MERGE when rows > 10M.
"""

import os

import clickhouse_connect
import duckdb

TABLES = ["dim_product", "dim_shop", "fct_product_snapshot"]
CH_DB = os.getenv("CLICKHOUSE_DB", "analytics")


def _ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "ch_user"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "ch_pass"),
        database=CH_DB,
    )


def main() -> None:
    gold_db = os.getenv("GOLD_DB_PATH", "pipeline/dbt/gold.duckdb")

    duck = duckdb.connect(gold_db, read_only=True)
    ch = _ch_client()

    for table in TABLES:
        rows = duck.execute(f"SELECT * FROM {table}").fetchall()
        cols = [d[0] for d in duck.description]

        if not rows:
            print(f"loaded {table}: 0 rows (empty source)")
            continue

        if table == "fct_product_snapshot":
            # Idempotency: drop partitions that overlap with gold data, then insert.
            months = duck.execute(
                "SELECT DISTINCT strftime(crawled_at, '%Y%m') FROM fct_product_snapshot"
            ).fetchall()
            for (m,) in months:
                ch.command(f"ALTER TABLE {CH_DB}.{table} DROP PARTITION '{m}'")
            ch.insert(table, rows, column_names=cols)
        else:
            # dim_product / dim_shop: ReplacingMergeTree deduplicates by ORDER BY key.
            # Insert-only — OPTIMIZE FINAL runs in scheduled maintenance DAG.
            ch.insert(table, rows, column_names=cols)

        count = ch.query(f"SELECT count() FROM {table}").first_row[0]
        print(f"loaded {table}: {count} rows")

    duck.close()
    ch.close()


if __name__ == "__main__":
    main()
