# E-Commerce Crawler Pipeline — Standard Operating Procedure

**Audience:** Internal Engineer / Operations  
**Version:** 1.3  
**Last updated:** 2026-07-16  
**Prerequisites:** Docker Desktop, Git, Python 3.10+

---

## 1. Startup & Deployment

### 1.1 First-Time Setup

```bash
# 1. Clone and install dependencies
git clone https://github.com/mhmdwldn/ecommerce-crawler
cd ecommerce-crawler
pip install -r source/requirements.txt

# 2. Start all services in correct order (~3 minutes, ~6.5 GB RAM)
#    start.sh handles: ZK→Kafka→PG→DDL+seed→Kafka topic+ES index→pool→all other services
bash start.sh

# 3. Verify all services are healthy
docker compose -f source/deployment/compose.yaml ps
```

**Expected output:** All 18 containers show `Up` or `Up (healthy)`. Script prints service endpoints at the end.

**What `start.sh` does differently from `make up`:**
- Waits for Zookeeper to accept connections before starting Kafka (prevents `NodeExists` crash)
- Cleans stale ZK broker nodes before starting Kafka
- Waits for Kafka broker to be fully ready, not just container started
- Auto-applies Asset Registry DDL + seed (no manual step needed)
- Bootstraps Kafka topic + ES index
- **Rebuilds Airflow image with `--build` flag** (ensures latest dependencies installed)
- **Auto-creates `pipeline_pool` (1 slot)** in Airflow after webserver is ready

### 1.2 Verify Pipeline End-to-End

```bash
# Trigger a scheduled DAG run
docker compose -f source/deployment/compose.yaml exec airflow \
  airflow dags trigger tokopedia_products \
  --conf '{"keyword": "laptop gaming", "max_pages": 1}'

# Or trigger a manual retry DAG (from Streamlit)
docker compose -f source/deployment/compose.yaml exec airflow \
  airflow dags trigger tokopedia_retry \
  --conf '{"keyword": "poco f8", "max_pages": 2, "asset_id": 1}'
```

Open Airflow UI at `http://localhost:8080` (admin/admin). Watch the DAG run through all 8 tasks.

**DAG types:**
| DAG ID | Schedule | Priority | Purpose |
|---|---|---|---|
| `tokopedia_products` | @hourly | 10 | Crawl all due assets from registry |
| `tokopedia_retry` | None (manual) | 1 | Retry single/batch assets from Streamlit |

Both DAGs share `pipeline_pool` (1 slot) — only one task runs at a time. Scheduled always wins.

### 1.3 Verify BI Tools

| Tool | URL | Credentials |
|---|---|---|
| Metabase | http://localhost:3000 | `admin@tokocrawl.local` / `admin12345` |
| Superset | http://localhost:8088 | `admin` / `admin` |

Run the all-in-one setup script (needs `requests`):

```bash
pip install requests && python dashboards/setup_all.py
```

If Metabase shows no tables: **Admin → Databases → Postgres Mart → Sync database schema now**.
If Superset datasets appear empty: **Data → Datasets → click dataset → Sync columns from source**.

If Superset shows "clickhouse_connect not found": the driver was installed at runtime. Restart Superset and re-run setup.

### 1.4 Configure Alerting (Optional)

```bash
# Set webhook URL in the Airflow container environment
# Telegram example:
export ALERT_WEBHOOK_URL="https://api.telegram.org/bot<TOKEN>/sendMessage"
export TELEGRAM_CHAT_ID="123456"

# Discord example:
export ALERT_WEBHOOK_URL="https://discord.com/api/webhooks/<ID>/<TOKEN>"
```

Add these to `source/deployment/compose.yaml` under the Airflow service `environment` section, then restart Airflow:

```bash
docker compose -f source/deployment/compose.yaml restart airflow
```

---

## 2. Daily Monitoring

### 2.1 Check the Audit Trail

```bash
# Latest 10 pipeline runs
docker exec clickhouse clickhouse-client \
  --user ch_user --password ch_pass --query "
    SELECT run_id, status, rows_silver, rows_rejects, rows_gold, duration_sec
    FROM analytics.pipeline_runs
    ORDER BY execution_date DESC
    LIMIT 10
    FORMAT Pretty
  "
```

**What to look for:**

| Signal | Meaning | Action |
|---|---|---|
| `status = 'failed'` | Pipeline task failed | See §3.1 |
| `rows_rejects > 0` | Malformed data entered bronze | See §3.2 |
| `rows_silver = 0` | Crawler returned no data or Kafka empty | Check Airflow crawl task logs |
| `duration_sec` growing over time | Spark jobs slowing down | Schedule `OPTIMIZE` on Delta tables (§4.1) |
| Consecutive runs with `status = 'failed'` | Persistent issue | Check alerting webhook, investigate root cause |

### 2.2 Check Asset Registry Health

```bash
docker exec postgres-mart psql -U mart -d mart -c "
  SELECT category,
         count(*) AS total,
         count(*) FILTER (WHERE is_active) AS active,
         count(*) FILTER (WHERE NOT is_active) AS disabled,
         max(consecutive_failures) AS max_failures
  FROM control.crawl_assets
  GROUP BY category
  ORDER BY total DESC;
"
```

**What to look for:**

- `disabled > 0` → Circuit breaker activated. See §3.3.
- `max_failures >= 4` → Asset is close to circuit breaker threshold. Investigate.

### 2.3 Check Batch Retry Queue

```bash
# List pending retries (assets waiting for DAG execution)
docker exec postgres-mart psql -U mart -d mart -c "
  SELECT asset_id, label, last_status, last_crawled_at
  FROM control.crawl_assets
  WHERE last_status = 'pending'
  ORDER BY asset_id;
"
```

**Note:** `pending` status means a retry was triggered from Streamlit but the DAG hasn't run yet (queued behind scheduled run or waiting for pool slot).

### 2.4 Check Quality Gate

```bash
docker exec airflow bash -c "
  cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo python -m pipeline.quality.checks
"
```

**Expected output:** `5/5 passed`. Any `FAIL` line requires immediate investigation (§3.2).

### 2.5 Verify Alerting (Weekly)

1. Temporarily set `ALERT_WEBHOOK_URL` to a test channel
2. Force a DAG failure by setting an invalid keyword via `dag_run.conf`
3. Verify the webhook message arrives within 10 seconds of task failure
4. Restore the original `ALERT_WEBHOOK_URL` (if changed)

---

## 3. Troubleshooting Failures

### 3.1 Pipeline Task Failure

**Symptom:** Airflow task shows `failed` state.

**Procedure:**

1. **Identify the failing task.** Open Airflow UI → `tokopedia_products` (or `tokopedia_retry`) → click the failed DAG run → view task logs.

2. **Act by task:**

| Failing task | Common cause | Action |
|---|---|---|
| `crawl` | Tokopedia API rate-limit (HTTP 429) | Wait 5 minutes and re-run. Check `RATE_LIMIT_RPS` in config. |
| `crawl` | Invalid keyword or API schema change | Verify the asset's `payload` in `control.crawl_assets`. |
| `crawl` | `ModuleNotFoundError: No module named 'clickhouse_connect'` | Airflow image is stale. Rebuild: `docker compose -f source/deployment/compose.yaml build airflow --no-cache && docker compose -f source/deployment/compose.yaml up -d airflow` |
| `crawl` | `NameError: name 'mark_success' is not defined` | Code bug (import moved to top-level in crawl_assets.py). Fixed in latest commit. Pull latest code. |
| `crawl` | `relation "control.v_due_assets" does not exist` | DDL not applied. Run `bash start.sh` or manually: `cat assets/ddl/crawl_assets.sql \| docker exec -i postgres-mart psql -U mart -d mart` |
| `crawl` | `get_due_assets()` returns empty | All assets recently crawled (cadence not expired). This is normal. |
| `bronze` | Kafka broker unreachable (`kafka:29092` DNS fail) | `docker ps --filter "name=kafka"` — if exited, `docker start kafka`. If `NodeExists`, use `bash start.sh` instead of direct start. |
| `bronze` | Stale checkpoint (offset mismatch) | Delete checkpoint objects from MinIO: `_checkpoints/bronze_products/`. |
| `bronze` | `Set(topic-partition-X) are gone` | Topic recreated with fewer partitions. Delete checkpoint: `mc rm --recursive --force local/lakehouse/_checkpoints/bronze_products/` |
| `silver` | Bronze table empty or corrupted | Run `SELECT count(*) FROM delta.\`s3a://lakehouse/bronze/products\`` |
| `silver` | `category_sk` column is NULL for all rows | `add_category_columns()` not applied. Run full refresh: `docker exec airflow bash -c "cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo python -m pipeline.spark.silver --full-refresh"` |
| `quality_check` | Any quality check failed | See §3.2 |
| `dbt_build` | DuckDB or dbt model error | Run `dbt build` manually in Airflow container to see full error |
| `dbt_build` | `dim_category not found` | DDL not applied to ClickHouse. Run: `cat warehouse/clickhouse/ddl/dim_category.sql \| docker exec -i clickhouse clickhouse-client --user ch_user --password ch_pass` |
| `load_postgres` / `load_clickhouse` | Database unreachable | Verify service: `docker exec postgres-mart pg_isready -U mart` |
| `load_clickhouse` | `ModuleNotFoundError: No module named 'clickhouse_connect'` | Same fix as crawl task — rebuild Airflow image |

3. **Clear the failed task** and re-run:
```bash
docker compose -f source/deployment/compose.yaml exec airflow \
  airflow tasks clear tokopedia_products \
  --task-regex ".*" --start-date "2026-01-01" --end-date "2026-12-31" \
  --dag-run-id "<FAILED_RUN_ID>"
```

### 3.2 Quality Gate Failure

**Symptom:** `quality_check` task shows `failed`. Check which rule triggered.

**Procedure:**

1. **Run quality checks manually** to see the failure detail:
```bash
docker exec airflow bash -c "
  cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo python -m pipeline.quality.checks
"
```

2. **Act by rule:**

| Rule | Meaning | Root cause investigation | Fix |
|---|---|---|---|
| `row_count = FAIL` | Silver has 0 rows | Kafka topic empty? Bronze corrupted? | Check Kafka offsets |
| `null_pct = FAIL` | Key column has >5% nulls | Tokopedia API changed response schema? | Inspect `value_json` in bronze. Update `PRODUCT_SCHEMA` in `silver.py`. |
| `price_positive = FAIL` | Products with `price_idr <= 0` | Tokopedia listing with free/zero price? Crawler parsing error? | Check bronze `value_json` for the affected product IDs. |
| `rejects_ratio = FAIL` | >10% of rows are unparseable | Large batch of malformed JSON in bronze | Query `_rejects` table, fix source, rebuild silver. |
| `freshness = FAIL` | Most recent `crawled_at` > 2 hours old | Crawler stuck? Airflow scheduler down? | Check DAG schedule, verify scheduler. Check `max(last_crawled_at)` in registry. |

3. **After fixing the root cause,** re-run silver manually then re-check.

### 3.3 Circuit Breaker Activation

**Symptom:** Asset's `is_active` column shows `false`, `consecutive_failures >= 5`.

**Procedure:**

1. **List disabled assets:**
```bash
docker exec postgres-mart psql -U mart -d mart -c "
  SELECT asset_id, label, last_status, consecutive_failures, last_crawled_at
  FROM control.crawl_assets
  WHERE NOT is_active
  ORDER BY asset_id;
"
```

2. **Investigate the failure cause.** Check the Airflow logs for the `crawl` task at the timestamps matching `last_crawled_at`.

3. **Fix the root cause.** Common issues:
   - Tokopedia API schema change → update `graphql_queries.py` and `schemas.py`
   - Invalid keyword returns 0 results → change to a broader keyword
   - Rate limiting (HTTP 429) → increase `cadence_min`, reduce `RATE_LIMIT_RPS`

4. **Reactivate the asset:**
```sql
-- Via SQL
UPDATE control.crawl_assets
SET is_active = true, consecutive_failures = 0, last_status = NULL
WHERE asset_id = <ASSET_ID>;

-- Or via Streamlit UI: Tab Bermasalah → klik "Aktifkan"
```

### 3.4 Airflow API Authentication (401 Unauthorized)

**Symptom:** Streamlit retry button returns "API error: 401 Unauthorized". `curl -u admin:admin` gets 401.

**Root cause:** Airflow 2.10.4 uses session-based API auth by default. Basic auth requires explicit config.

**Fix (permanent):** `AIRFLOW__API__AUTH_BACKENDS` is configured in `compose.yaml`:
```yaml
AIRFLOW__API__AUTH_BACKENDS: airflow.api.auth.backend.session,airflow.api.auth.backend.basic_auth
```

If the env var is missing (fresh setup or image rebuild), add it and restart Airflow:
```bash
docker compose -f source/deployment/compose.yaml up -d airflow
```

**Verify:**
```bash
curl -s -u admin:admin "http://localhost:8080/api/v1/dags" | python -c "import sys,json; print(json.load(sys.stdin).get('total_entries','FAIL'))"
```

### 3.5 Streamlit Status Stuck at "pending"

**Symptom:** After clicking retry, asset status shows "pending" even after DAG completes.

**Root cause:** DAG didn't call `mark_success()`/`mark_failure()` for that specific asset. Common reasons:
- Old code where `crawl_assets.py` didn't handle `CRAWL_ASSET_ID` env var
- DAG triggered `tokopedia_products` instead of `tokopedia_retry` (old Streamlit code)

**Fix:** Pull latest code. `trigger_dag` in `app.py` now:
1. Sends `asset_id` in `dag_run.conf`
2. DAG injects `CRAWL_ASSET_ID` env var
3. `crawl_assets.py` reads it, calls `_crawl_one()` → `mark_success()`/`mark_failure()`

### 3.6 Prometheus / Grafana / Alertmanager

**Prometheus target DOWN (merah di Targets page):**

1. Cek service: `docker ps --filter "name=<service>"`
2. Kalau mati: `docker start <service>`
3. Kalau hidup tapi DOWN: cek metrics endpoint: `docker exec prometheus wget -qO- http://<service>:<port>/metrics`

**Alertmanager tidak kirim notifikasi:**

1. Cek `monitoring/alertmanager.yml` — pastikan `webhook_configs` terisi
2. Restart: `docker restart alertmanager`
3. Cek log: `docker logs alertmanager`

### 3.7 Vault Issues

**Vault tidak bisa diakses:**

1. Cek container: `docker ps --filter "name=vault"`
2. Kalau mati: `docker start vault`
3. Dev mode tidak persist — semua secret hilang setelah restart. Simpan ulang via `python dashboards/setup_all.py`.

### 3.8 Metabase / Superset Issues

**Metabase: "No tables found"**

1. Login → Settings → Admin → Databases → Postgres Mart
2. Klik **Sync database schema now**
3. Tunggu 30 detik, refresh

**Superset: "No module named clickhouse_connect"**

```bash
docker exec superset bash -c '
cp -r /app/superset_home/.local/lib/python3.10/site-packages/clickhouse_connect* /app/.venv/lib/python3.10/site-packages/
/app/.venv/bin/python -c "import clickhouse_connect; print(\"OK\")"
'
docker restart superset
```

---

## 4. Scheduled Maintenance

### 4.1 Weekly Maintenance DAG

The `lakehouse_maintenance` DAG runs automatically every week:

| Task | Action | Purpose |
|---|---|---|
| `optimize_bronze` | `OPTIMIZE delta.\`s3a://lakehouse/bronze/products\`` | Compacts small Parquet files |
| `vacuum_bronze` | `VACUUM ... RETAIN 168 HOURS` | Removes stale files older than 7 days |
| `optimize_silver` | Same pattern | Compacts silver files |
| `vacuum_silver` | Same pattern | Removes stale silver files |
| `optimize_clickhouse` | `OPTIMIZE TABLE analytics.dim_product FINAL` + dim_shop + dim_category | Deduplicates ReplacingMergeTree dims |

### 4.2 Manual Maintenance Execution

```bash
docker compose -f source/deployment/compose.yaml exec airflow \
  airflow dags trigger lakehouse_maintenance
```

---

## 5. Reverse Proxy (Caddy)

Semua service bisa diakses lewat satu port: `http://localhost:8081/<service>/`

| Path | Backend |
|---|---|
| `/airflow/` | Airflow :8080 |
| `/metabase/` | Metabase :3000 |
| `/superset/` | Superset :8088 |
| `/grafana/` | Grafana :3001 |
| `/prometheus/` | Prometheus :9090 |
| `/vault/` | Vault :8200 |
| `/minio/` | MinIO :9001 |

Untuk production TLS: ubah port ke 443, tambah domain, Caddy auto-request Let's Encrypt.

---

## 6. Secret Management (Vault)

### Mengakses Vault

```bash
# UI
open http://localhost:8200 → token: root-token-dev

# API
curl -H "X-Vault-Token: root-token-dev" http://localhost:8200/v1/secret/data/postgres
```

### Menambah secret baru

```bash
curl -X POST http://localhost:8200/v1/secret/data/my-service \
  -H "X-Vault-Token: root-token-dev" \
  -d '{"data":{"username":"admin","password":"secret123"}}'
```

---

## 7. Backup & Restore

### Menjalankan backup manual

```bash
./backup.sh
# Output: ./backups/YYYYMMDD_HHMMSS/
```

### Restore dari backup

```bash
# 1. Recreate DDL
docker exec -i postgres-mart psql -U mart -d mart < assets/ddl/crawl_assets.sql

# 2. Restore data atau seed
python assets/seed.py

# 3. Verify
docker exec postgres-mart psql -U mart -d mart -c "SELECT count(*) FROM control.crawl_assets"
```

---

## 8. Deploy (Rolling Update)

```bash
make deploy     # Pull GHCR → restart → health check → auto-rollback
make rollback   # Restore image sebelumnya
```

---

## 9. K8s Deployment (Helm)

```bash
helm install ecommerce-crawler ./deployment/helm
# Minimal:
helm install ecommerce-crawler ./deployment/helm --set elasticsearch.enabled=false --set metabase.enabled=false
```

---

## 10. Cold Storage

```bash
# Export old data to Parquet before VACUUM
docker exec airflow bash -c "cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo python -m pipeline.spark.retention --cold-storage"

# Dry run first
docker exec airflow bash -c "cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo python -m pipeline.spark.retention --cold-storage --dry-run"
```

---

## 11. Data Retention & Incremental

### Menjalankan retention manual
```bash
docker exec airflow bash -c "cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo python -m pipeline.spark.retention --dry-run"
docker exec airflow bash -c "cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo python -m pipeline.spark.retention"
```

### Menjalankan silver incremental
```bash
# Incremental (MERGE new rows only)
docker exec airflow bash -c "cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo python -m pipeline.spark.silver --incremental"

# Full refresh (default)
docker exec airflow bash -c "cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo python -m pipeline.spark.silver --full-refresh"
```

---

## 12. Quick Reference: Pool & DAG Management

### Check pool status
```bash
docker exec airflow bash -c "airflow pools list 2>/dev/null"
# Output should show: pipeline_pool | 1 | Pipeline serializer
```

### Check DAG run queue
```bash
# List queued DAG runs (waiting for pool slot)
docker exec airflow bash -c "airflow dags list-runs -d tokopedia_products --state queued 2>/dev/null"
docker exec airflow bash -c "airflow dags list-runs -d tokopedia_retry --state queued 2>/dev/null"
```

### Manually create pool (if missing)
```bash
docker exec airflow bash -c "airflow pools set pipeline_pool 1 'Pipeline serializer'"
```
