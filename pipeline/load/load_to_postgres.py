"""Load gold tables from DuckDB into the Postgres mart.

ponytail: drop-and-recreate full reload; switch to upserts when the mart
gets consumers that can't tolerate the swap.
"""

import os

import duckdb

from pipeline import GOLD_TABLES


def main() -> None:
    """Load all GOLD_TABLES from DuckDB into Postgres mart (full drop-and-recreate).

    Strategy: ``DROP TABLE IF EXISTS ... CREATE TABLE AS SELECT *`` for each table.
    Uses DuckDB's PostgreSQL extension (INSTALL postgres; ATTACH). Idempotent —
    rerunning the same gold data produces identical Postgres rows.

    Env vars:
        ``GOLD_DB_PATH`` — DuckDB gold database (default ``pipeline/dbt/gold.duckdb``)
        ``POSTGRES_DSN`` — Postgres connection string
    """
    gold_db = os.getenv("GOLD_DB_PATH", "pipeline/dbt/gold.duckdb")
    dsn = os.getenv("POSTGRES_DSN", "host=localhost port=5433 dbname=mart user=mart password=mart")

    con = duckdb.connect(gold_db, read_only=True)
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{dsn}' AS pg (TYPE postgres, READ_WRITE true)")
    for table in GOLD_TABLES:
        con.execute(f"DROP TABLE IF EXISTS pg.public.{table}")
        con.execute(f"CREATE TABLE pg.public.{table} AS SELECT * FROM {table}")
        count = con.execute(f"SELECT count(*) FROM pg.public.{table}").fetchone()[0]
        print(f"loaded {table}: {count} rows")


if __name__ == "__main__":
    main()
