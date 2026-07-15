"""Setup Superset + Metabase: databases, datasets, sample charts.

Run from host machine (needs requests):
    pip install requests
    python dashboards/setup_all.py
"""

import time

import requests

# =============================================================================
# Superset Setup
# =============================================================================
SUPERSET = "http://localhost:8088"
SUPERSET_AUTH = ("admin", "admin")

# ClickHouse tables to register as Superset datasets
CH_TABLES = [
    ("fct_product_snapshot", "analytics"),
    ("dim_product", "analytics"),
    ("dim_shop", "analytics"),
    ("pipeline_runs", "analytics"),
]


def setup_superset():
    print("=== Superset Setup ===")

    # 1. Login via form (avoids CSRF complexity)
    s = requests.Session()
    # Get login page to grab CSRF cookie
    s.get(f"{SUPERSET}/login/")
    # Post login
    r = s.post(
        f"{SUPERSET}/login/",
        data={"username": "admin", "password": "admin"},
        allow_redirects=True,
    )
    if r.status_code != 200 or "login" in r.url.lower():
        print(f"  WARN: Login might have failed (status={r.status_code})")

    # Get CSRF token from cookies
    csrf = s.cookies.get("csrf_access_token") or s.cookies.get("session")
    headers = {
        "Content-Type": "application/json",
        "X-CSRFToken": csrf or "",
        "Referer": SUPERSET,
    }

    # 2. Register ClickHouse database connection
    uri = "clickhousedb://ch_user:ch_pass@clickhouse:8123/analytics"
    r = s.get(f"{SUPERSET}/api/v1/database/", headers=headers)
    db_id = None
    for db in r.json().get("result", []):
        if db["database_name"] == "ClickHouse Analytics":
            db_id = db["id"]
            print(f"  Database exists: id={db_id}")
            break

    if db_id is None:
        r = s.post(
            f"{SUPERSET}/api/v1/database/",
            headers=headers,
            json={
                "database_name": "ClickHouse Analytics",
                "sqlalchemy_uri": uri,
                "expose_in_sqllab": True,
            },
        )
        if r.status_code in (201, 200):
            db_id = r.json().get("id", r.json().get("result", {}).get("id"))
            print(f"  Database created: id={db_id}")
        else:
            print(f"  Database create failed: {r.status_code} {r.text[:200]}")
            return

    # 3. List existing datasets
    r = s.get(f"{SUPERSET}/api/v1/dataset/", headers=headers)
    existing = {
        (d["table_name"], d["schema"])
        for d in r.json().get("result", [])
        if d.get("database", {}).get("id") == db_id
    }

    # 4. Create datasets for each ClickHouse table
    for table_name, schema in CH_TABLES:
        if (table_name, schema) in existing:
            print(f"  Dataset exists: {schema}.{table_name}")
            continue
        r = s.post(
            f"{SUPERSET}/api/v1/dataset/",
            headers=headers,
            json={
                "database": db_id,
                "table_name": table_name,
                "schema": schema,
            },
        )
        if r.status_code in (201, 200):
            ds_id = r.json().get("id", r.json().get("result", {}).get("id"))
            print(f"  Dataset created: {schema}.{table_name} (id={ds_id})")
        else:
            print(f"  Dataset failed: {schema}.{table_name} -> {r.status_code} {r.text[:100]}")
            # Try alternative: create via SQL Lab warmup
            _warmup_via_sqllab(s, headers, db_id, table_name, schema)

    print(f"  Done. Open {SUPERSET}/dataset/add/ to verify.")


def _warmup_via_sqllab(s, headers, db_id, table, schema):
    """Fallback: run a query to warm up the table in Superset's cache."""
    try:
        r = s.post(
            f"{SUPERSET}/api/v1/sqllab/execute/",
            headers=headers,
            json={
                "database_id": db_id,
                "sql": f"SELECT * FROM {schema}.{table} LIMIT 1",
            },
        )
        if r.status_code == 200:
            print(f"    Warmed up via SQL Lab: {schema}.{table}")
    except Exception as e:
        print(f"    Warmup failed: {e}")


# =============================================================================
# Metabase Setup
# =============================================================================
METABASE = "http://localhost:3000"
MB_USER = "admin@local.com"
MB_PASS = "admin12345"


def setup_metabase():
    print("\n=== Metabase Setup ===")

    s = requests.Session()

    # 1. Check if Metabase needs first-time setup
    r = s.get(f"{METABASE}/api/session/properties")
    if r.status_code == 200:
        token = r.json().get("setup-token")
        if token:
            print("  First-time setup detected...")
            r = s.post(
                f"{METABASE}/api/setup",
                json={
                    "token": token,
                    "user": {
                        "first_name": "Admin",
                        "last_name": "User",
                        "email": MB_USER,
                        "password": MB_PASS,
                    },
                    "prefs": {"site_name": "E-Commerce Crawler", "allow_tracking": False},
                },
            )
            if r.status_code == 200:
                print(f"  Admin user created: {MB_USER}")
            else:
                print(f"  Setup failed: {r.status_code} {r.text[:200]}")
                # Maybe already set up — continue
        else:
            print("  Already set up (no setup token)")

    # 2. Login to get session token
    r = s.post(
        f"{METABASE}/api/session",
        json={"username": MB_USER, "password": MB_PASS},
    )
    if r.status_code != 200:
        print(f"  Login failed: {r.status_code} {r.text[:200]}")
        print(f"  Open {METABASE}/setup to create admin user first.")
        return

    session_token = r.json()["id"]
    headers = {"X-Metabase-Session": session_token}

    # 3. Check if Postgres connection exists
    r = s.get(f"{METABASE}/api/database", headers=headers)
    pg_id = None
    for db in r.json().get("data", []):
        if db.get("name") == "Postgres Mart":
            pg_id = db["id"]
            print(f"  Database exists: id={pg_id}")
            break

    # 4. Create Postgres mart connection
    if pg_id is None:
        r = s.post(
            f"{METABASE}/api/database",
            headers=headers,
            json={
                "name": "Postgres Mart",
                "engine": "postgres",
                "details": {
                    "host": "postgres",
                    "port": 5432,
                    "dbname": "mart",
                    "user": "mart",
                    "password": "mart",
                    "ssl": False,
                },
                "is_full_sync": True,
            },
        )
        if r.status_code == 200:
            pg_id = r.json()["id"]
            print(f"  Database created: id={pg_id}")
        else:
            print(f"  Database create failed: {r.status_code} {r.text[:200]}")
            # Try alternative: check if already exists by scanning again
            r2 = s.get(f"{METABASE}/api/database", headers=headers)
            for db in r2.json().get("data", []):
                if db.get("name") == "Postgres Mart":
                    pg_id = db["id"]
                    print(f"  Database found on second scan: id={pg_id}")
                    break
            if pg_id is None:
                return

    # 5. Trigger sync to discover tables
    r = s.post(f"{METABASE}/api/database/{pg_id}/sync_schema", headers=headers)
    if r.status_code == 200:
        print(f"  Schema sync triggered for database id={pg_id}")
    else:
        print(f"  Schema sync skipped: {r.status_code}")

    # 6. Force re-scan
    time.sleep(2)
    r = s.post(f"{METABASE}/api/database/{pg_id}/rescan_values", headers=headers)
    print(f"  Rescan: {'OK' if r.status_code == 200 else r.status_code}")

    print(f"  Done. Open {METABASE}/browse to see tables.")


# =============================================================================
# Main
# =============================================================================
if __name__ == "__main__":
    print("E-Commerce Crawler — BI Setup\n")
    try:
        setup_superset()
    except Exception as e:
        print(f"  Superset setup error: {e}")
    try:
        setup_metabase()
    except Exception as e:
        print(f"  Metabase setup error: {e}")
    print("\nSetup complete. URLs:")
    print(f"  Superset: {SUPERSET} (admin / admin)")
    print(f"  Metabase: {METABASE} ({MB_USER} / {MB_PASS})")
