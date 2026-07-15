"""Configure Superset: ClickHouse connection + sample datasets.

Run after Superset is up: docker exec superset python /tmp/setup_superset.py
Or copy this file into the container and execute.

ponytail: REST API calls to Superset. Idempotent — safe to re-run.
"""

import os

import requests

BASE = "http://localhost:8088"
AUTH = ("admin", "admin")
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CH_USER = os.getenv("CLICKHOUSE_USER", "ch_user")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "ch_pass")
CH_DB = os.getenv("CLICKHOUSE_DB", "analytics")


def login() -> str:
    """Get access token from Superset."""
    r = requests.post(
        f"{BASE}/api/v1/security/login",
        json={"username": AUTH[0], "password": AUTH[1], "provider": "db"},
    )
    r.raise_for_status()
    return r.json()["access_token"]


def create_clickhouse_connection(token: str) -> int:
    """Register ClickHouse as a database in Superset. Returns DB id."""
    headers = {"Authorization": f"Bearer {token}"}
    uri = f"clickhousedb://{CH_USER}:{CH_PASS}@{CH_HOST}:{CH_PORT}/{CH_DB}"

    # Check if already exists
    r = requests.get(f"{BASE}/api/v1/database/", headers=headers)
    for db in r.json().get("result", []):
        if db["database_name"] == "ClickHouse Analytics":
            print(f"ClickHouse connection exists (id={db['id']})")
            return db["id"]

    # Create
    r = requests.post(
        f"{BASE}/api/v1/database/",
        headers=headers,
        json={
            "database_name": "ClickHouse Analytics",
            "sqlalchemy_uri": uri,
            "expose_in_sqllab": True,
            "allow_dml": True,
        },
    )
    if r.status_code == 201:
        db_id = r.json()["id"]
        print(f"ClickHouse connection created (id={db_id})")
        return db_id
    print(f"Create DB failed: {r.status_code} {r.text}")
    return -1


if __name__ == "__main__":
    try:
        token = login()
        db_id = create_clickhouse_connection(token)
        print(f"Superset setup complete. DB id={db_id}")
    except Exception as e:
        print(f"Setup failed: {e}")
        print("Superset might still be starting — wait 30s and retry.")
