"""Tests for load_to_clickhouse — verifies row counts match between
DuckDB gold and ClickHouse after loading. Requires ClickHouse and
DuckDB to be available (run inside airflow container)."""

import os

import pytest

# ---------------------------------------------------------------------------
# Skip the entire suite when ClickHouse is not reachable
# ---------------------------------------------------------------------------
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
GOLD_DB = os.getenv("GOLD_DB_PATH", "pipeline/dbt/gold.duckdb")

pytestmark = pytest.mark.skipif(
    not os.path.exists(GOLD_DB),
    reason=f"gold DuckDB not found at {GOLD_DB} — run dbt build first",
)


@pytest.fixture(scope="module")
def ch_client():
    """Real ClickHouse client — test is skipped if unreachable."""
    try:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "ch_user"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "ch_pass"),
            database=os.getenv("CLICKHOUSE_DB", "analytics"),
        )
        client.query("SELECT 1")
        return client
    except Exception as e:
        pytest.skip(f"ClickHouse not reachable: {e}")


@pytest.fixture(scope="module")
def duck_conn():
    """DuckDB connection to gold database."""
    import duckdb

    return duckdb.connect(GOLD_DB, read_only=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

GOLD_TABLES = ["dim_product", "dim_shop", "fct_product_snapshot"]


def test_all_tables_exist_in_clickhouse(ch_client):
    """All three gold tables should exist in ClickHouse after a load."""
    rows = ch_client.query(
        "SELECT name FROM system.tables WHERE database = currentDatabase()"
    ).result_rows
    tables = {r[0] for r in rows}
    for t in GOLD_TABLES:
        assert t in tables, f"{t} missing from ClickHouse"


def test_row_counts_match_gold(ch_client, duck_conn):
    """Row counts in ClickHouse must equal DuckDB gold (after OPTIMIZE for dims)."""
    # Optimize dims to deduplicate ReplacingMergeTree
    ch_client.command("OPTIMIZE TABLE analytics.dim_product FINAL")
    ch_client.command("OPTIMIZE TABLE analytics.dim_shop FINAL")

    for table in GOLD_TABLES:
        gold_count = duck_conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        ch_count = ch_client.query(
            f"SELECT count() FROM analytics.{table}"
        ).first_row[0]
        assert ch_count == gold_count, (
            f"{table}: ClickHouse {ch_count} != DuckDB {gold_count}"
        )


def test_load_is_idempotent(ch_client, duck_conn):
    """Running load_to_clickhouse twice should not double the row counts."""
    import duckdb as ddb

    from pipeline.load import load_to_clickhouse

    # Snapshot counts before re-run
    before = {}
    for table in GOLD_TABLES:
        before[table] = ch_client.query(
            f"SELECT count() FROM analytics.{table}"
        ).first_row[0]

    # Re-run the loader (same data)
    load_to_clickhouse.main()

    # Optimize dims
    ch_client.command("OPTIMIZE TABLE analytics.dim_product FINAL")
    ch_client.command("OPTIMIZE TABLE analytics.dim_shop FINAL")

    # Counts after must match counts before (no duplicates)
    for table in GOLD_TABLES:
        after = ch_client.query(
            f"SELECT count() FROM analytics.{table}"
        ).first_row[0]
        assert after == before[table], (
            f"{table}: idempotency failed — {before[table]} → {after}"
        )
