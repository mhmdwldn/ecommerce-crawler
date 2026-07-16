"""Shared ClickHouse client builder — single point of configuration."""

import os

import clickhouse_connect


def get_client():
    """Build a ClickHouse client from env vars (single point of configuration).

    Returns:
        ``clickhouse_connect.Client`` connected to ``CLICKHOUSE_HOST`` (default
        ``clickhouse``, port ``8123``, database ``analytics``).
    """
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "ch_user"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "ch_pass"),
        database=os.getenv("CLICKHOUSE_DB", "analytics"),
    )
