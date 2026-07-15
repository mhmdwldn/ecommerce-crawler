"""Metabase setup — Postgres mart connection.

Run after Metabase is up on http://localhost:3000.
First-time: create admin user via UI (http://localhost:3000/setup), then run this script.
"""

import os
import requests

BASE = "http://localhost:3000"
MB_USER = os.getenv("MB_USER", "admin@local.com")
MB_PASS = os.getenv("MB_PASS", "admin12345")
PG_HOST = os.getenv("MB_PG_HOST", "postgres")
PG_PORT = os.getenv("MB_PG_PORT", "5432")
PG_DB = os.getenv("MB_PG_DB", "mart")
PG_USER = os.getenv("MB_PG_USER", "mart")
PG_PASS = os.getenv("MB_PG_PASS", "mart")


def get_token() -> str:
    r = requests.post(
        f"{BASE}/api/session",
        json={"username": MB_USER, "password": MB_PASS},
    )
    r.raise_for_status()
    return r.json()["id"]


def create_postgres_connection(token: str) -> int:
    headers = {"X-Metabase-Session": token}

    # Check existing
    r = requests.get(f"{BASE}/api/database", headers=headers)
    for db in r.json().get("data", []):
        if db.get("name") == "Postgres Mart":
            print(f"Connection exists (id={db['id']})")
            return db["id"]

    # Create
    r = requests.post(
        f"{BASE}/api/database",
        headers=headers,
        json={
            "name": "Postgres Mart",
            "engine": "postgres",
            "details": {
                "host": PG_HOST,
                "port": int(PG_PORT),
                "dbname": PG_DB,
                "user": PG_USER,
                "password": PG_PASS,
                "ssl": False,
            },
            "is_full_sync": True,
        },
    )
    if r.status_code == 200:
        db_id = r.json()["id"]
        print(f"Connection created (id={db_id})")
        return db_id
    print(f"Create failed: {r.status_code} {r.text}")
    return -1


if __name__ == "__main__":
    try:
        token = get_token()
        db_id = create_postgres_connection(token)
        print(f"Metabase setup complete. DB id={db_id}")
    except Exception as e:
        print(f"Setup failed: {e}")
        print("Run Metabase setup first at http://localhost:3000/setup")
