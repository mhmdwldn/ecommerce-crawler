# E-Commerce Crawler Pipeline — Standard Operating Procedure

**Audience:** Internal Engineer / Operations  
**Version:** 1.0  
**Last updated:** 2026-07-15  
**Prerequisites:** Docker Desktop, Git, Python 3.10+

---

## 1. Startup & Deployment

### 1.1 First-Time Setup

```bash
# 1. Clone and install dependencies
git clone https://github.com/mhmdwldn/ecommerce-crawler
cd ecommerce-crawler
pip install -r source/requirements.txt

# 2. Start all 11 services (~2 minutes, ~5.3 GB RAM)
make up

# 3. Retrieve Airflow admin password
docker compose -f source/deployment/compose.yaml exec airflow \
  cat /opt/airflow/standalone_admin_password.txt

# 4. Verify all services are healthy
docker compose -f source/deployment/compose.yaml ps
```

**Expected output:** All 11 services show `Up` or `Up (healthy)`.

### 1.2 Seed the Asset Registry

```bash
# Apply DDL (if not already applied)
docker exec postgres-mart psql -U mart -d mart -f assets/ddl/crawl_assets.sql

# Seed initial targets (idempotent — safe to re-run)
python assets/seed.py
```

**Expected output:** `Selesai: 23 ok, 0 gagal`

### 1.3 Verify Pipeline End-to-End

```bash
# Trigger a manual DAG run
make smoke KEYWORD="laptop gaming"

# Or via Airflow CLI
docker compose -f source/deployment/compose.yaml exec airflow \
  airflow dags trigger tokopedia_products \
  --conf '{"keyword": "laptop gaming", "max_pages": 1}'
```

Open Airflow UI at `http://localhost:8080`. Watch the DAG run through all 8 tasks.

### 1.4 Verify BI Tools

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

### 1.5 Configure Alerting (Optional)

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

### 2.3 Check Quality Gate

```bash
docker exec airflow bash -c "
  cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo python -m pipeline.quality.checks
"
```

**Expected output:** `5/5 passed`. Any `FAIL` line requires immediate investigation (§3.2).

### 2.4 Verify Alerting (Weekly)

1. Temporarily set `ALERT_WEBHOOK_URL` to a test channel
2. Force a DAG failure by setting an invalid keyword via `dag_run.conf`
3. Verify the webhook message arrives within 10 seconds of task failure
4. Restore the original `ALERT_WEBHOOK_URL` (if changed)

---

### 3.4 BI Tool Issues

**Prometheus target DOWN (merah di Targets page):**

1. Cek service yang bersangkutan: `docker ps --filter "name=<service>"`
2. Kalau mati: `docker start <service>`
3. Kalau hidup tapi DOWN: cek metrics endpoint dari dalam container Prometheus
   ```bash
   docker exec prometheus wget -qO- http://<service>:<port>/metrics
   ```
4. Untuk ClickHouse: pastikan `monitoring/clickhouse-prometheus.xml` ter-mount di `/etc/clickhouse-server/config.d/`

**Grafana dashboard kosong:**

1. Cek Prometheus datasource: Configuration → Data Sources → Prometheus → Test
2. Kalau gagal, pastikan Prometheus container hidup dan reachable dari Grafana: `docker exec grafana wget -qO- http://prometheus:9090`

**Alertmanager tidak kirim notifikasi:**

1. Cek `monitoring/alertmanager.yml` — pastikan `webhook_configs` tidak kosong
2. Restart Alertmanager: `docker restart alertmanager`
3. Cek log: `docker logs alertmanager`

**Vault tidak bisa diakses:**

1. Cek container: `docker ps --filter "name=vault"`
2. Kalau mati: `docker start vault`
3. Dev mode tidak persist — semua secret hilang setelah restart. Simpan ulang via `python dashboards/setup_all.py` atau manual curl.

### 3.5 Metabase Issues

**Metabase: "No tables found"**

1. Login ke `http://localhost:3000` (admin@tokocrawl.local / admin12345)
2. Settings (gear) → Admin → Databases → Postgres Mart
3. Klik **Sync database schema now**
4. Tunggu 30 detik, refresh Browse

**Metabase: Forgot password / can't login**

```bash
# Reset Metabase (fresh start, all dashboards lost)
docker stop metabase
docker exec postgres-mart psql -U mart -d mart -c "DROP DATABASE IF EXISTS metabase"
docker exec postgres-mart psql -U mart -d mart -c "CREATE DATABASE metabase"
docker start metabase
# Wait 30s, then:
pip install requests && python dashboards/setup_all.py
```

**Superset: "Couldn't parse datetime string"**

Datasets corrupted — re-create via Superset UI:
1. Data → Datasets → delete all 4 datasets
2. Data → Databases → ClickHouse Analytics → Sync all tables
3. Wait 60 detik untuk schema refresh

**Superset: "No module named clickhouse_connect"**

Driver hilang setelah container restart:
```bash
docker exec superset bash -c '
cp -r /app/superset_home/.local/lib/python3.10/site-packages/clickhouse_connect* /app/.venv/lib/python3.10/site-packages/
/app/.venv/bin/python -c "import clickhouse_connect; print(\"OK\")"
'
docker restart superset
```

**Superset: Dataset keliatan tapi data kosong**

1. Data → Datasets → klik dataset
2. Klik tombol **Sync columns from source**
3. Ulangi untuk semua 4 dataset
4. Refresh halaman

---

## 2.5 Monitoring Dashboards

### Grafana (Port 3001)

1. Buka `http://localhost:3001` → login `admin` / `admin`
2. Dashboards → **Pipeline Health** (auto-loaded)
3. Panel: Services UP/DOWN, Service Status table, Prometheus scrape duration, Postgres connections

### Prometheus (Port 9090)

1. Buka `http://localhost:9090` → Status → Targets
2. Semua target harus UP (hijau). Kalau ada yang DOWN (merah), cek service tersebut.
3. Graph → coba query: `up`, `rate(prometheus_http_requests_total[5m])`

### Alertmanager (Port 9093)

1. Buka `http://localhost:9093` → Alerts
2. Kalau ada alert firing, cek deskripsinya.
3. Konfigurasi webhook: edit `monitoring/alertmanager.yml`, uncomment `webhook_configs`, restart `alertmanager` service.

## 2.6 Secret Management (Vault)

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

### Vault di Airflow

Airflow membaca Connections dari Vault path `connections/`. Untuk menambah koneksi baru, simpan ke Vault lalu restart Airflow.

## 2.7 Backup & Restore

### Menjalankan backup manual

```bash
./backup.sh
# Output: ./backups/YYYYMMDD_HHMMSS/
#   pg_mart_schema.sql    — Postgres mart DDL
#   pg_control_schema.sql — Asset registry DDL
#   pg_control_data.sql   — Asset registry data
#   ch_ddl.sql             — ClickHouse DDL
```

Backup otomatis: tambahkan ke crontab (`0 3 * * * /path/to/backup.sh`).

### Restore dari backup

```bash
# 1. Recreate table dari DDL
docker exec -i postgres-mart psql -U mart -d mart < assets/ddl/crawl_assets.sql

# 2. Restore data
cat backups/<TIMESTAMP>/pg_control_data.sql | docker exec -i postgres-mart psql -U mart -d mart

# 3. Atau gunakan seed (lebih aman — upsert)
python assets/seed.py

# 4. Verifikasi
docker exec postgres-mart psql -U mart -d mart -c "SELECT count(*) FROM control.crawl_assets"
```

## 2.8 Deploy (Rolling Update)

### Deploy versi terbaru

```bash
make deploy
# Pull image dari GHCR → restart Airflow → health check 60s → done
```

### Rollback

```bash
make rollback
# Restore image versi sebelumnya (disimpan sebagai tag :rollback)
```

### Lihat versi yang terpasang

```bash
docker images ghcr.io/mhmdwldn/ecommerce-crawler-airflow --format "table {{.Tag}}\t{{.CreatedAt}}"
```

---

## 3. Troubleshooting Failures

### 3.1 Pipeline Task Failure

**Symptom:** Airflow task shows `failed` state.

**Procedure:**

1. **Identify the failing task.** Open Airflow UI → `tokopedia_products` → click the failed DAG run → view task logs.

2. **Act by task:**

| Failing task | Common cause | Action |
|---|---|---|
| `crawl` | Tokopedia API rate-limit (HTTP 429) | Wait 5 minutes and re-run. Check `RATE_LIMIT_RPS` in config. |
| `crawl` | Invalid keyword or API schema change | Verify the asset's `payload` in `control.crawl_assets`. |
| `bronze` | Kafka broker unreachable | `docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:29092` |
| `bronze` | Stale checkpoint (offset mismatch) | Delete checkpoint objects from MinIO: `_checkpoints/bronze_products/` |
| `silver` | Bronze table empty or corrupted | Run `SELECT count(*) FROM delta.\`s3a://lakehouse/bronze/products\`` |
| `quality_check` | Any quality check failed | See §3.2 |
| `dbt_build` | DuckDB or dbt model error | Run `dbt build` manually in Airflow container to see full error |
| `load_postgres` / `load_clickhouse` | Database unreachable | Verify service: `docker exec postgres-mart pg_isready -U mart` |

3. **Clear the failed task** and re-run:
```bash
docker compose -f source/deployment/compose.yaml exec airflow \
  airflow tasks clear tokopedia_products \
  --task-regex ".*" --start-date "2026-01-01" --end-date "2026-12-31" \
  --dag-run-id "<FAILED_RUN_ID>"
```

4. **Re-trigger the DAG** after the fix is in place.

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
| `row_count = FAIL` | Silver has 0 rows | Kafka topic empty? Bronze corrupted? | Check Kafka offsets: `docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server localhost:29092 --topic tokopedia.products.raw --time -1` |
| `null_pct = FAIL` | Key column has >5% nulls | Tokopedia API changed response schema? | Inspect `value_json` in bronze for null fields. Update `PRODUCT_SCHEMA` in `silver.py`. |
| `price_positive = FAIL` | Products with `price_idr <= 0` | Tokopedia listing with free/zero price? Crawler parsing error? | Check bronze `value_json` for the affected product IDs. If legitimate (free product), adjust check threshold. |
| `rejects_ratio = FAIL` | >10% of rows are unparseable | Large batch of malformed JSON in bronze | Query `_rejects` table: `SELECT value_json FROM delta.\`s3a://lakehouse/silver/products_rejects\` LIMIT 10`. Fix the source issue, then rebuild silver. |
| `freshness = FAIL` | Most recent `crawled_at` > 2 hours old | Crawler stuck? Airflow scheduler down? | Check DAG schedule, verify Airflow scheduler is running. Check last crawl timestamp: `docker exec postgres-mart psql -U mart -d mart -c "SELECT max(last_crawled_at) FROM control.crawl_assets"` |

3. **After fixing the root cause,** re-run silver manually:
```bash
docker exec airflow bash -c "
  cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo python -m pipeline.spark.silver
"
```
Then re-run quality check to confirm `5/5 passed`.

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

-- Or via Streamlit UI: Assets → Edit → toggle "Aktif" → Save
```

5. **Re-trigger the DAG** to verify the asset now crawls successfully.

---

## 4. Scheduled Maintenance

### 4.1 Weekly Maintenance DAG

The `lakehouse_maintenance` DAG runs automatically every week. It performs:

| Task | Action | Purpose |
|---|---|---|
| `optimize_bronze` | `OPTIMIZE delta.\`s3a://lakehouse/bronze/products\`` | Compacts small Parquet files into larger ones (reduces read overhead) |
| `vacuum_bronze` | `VACUUM delta.\`s3a://lakehouse/bronze/products\` RETAIN 168 HOURS` | Removes stale Parquet files older than 7 days |
| `optimize_silver` | `OPTIMIZE delta.\`s3a://lakehouse/silver/products\`` | Same as bronze — compacts files |
| `vacuum_silver` | `VACUUM delta.\`s3a://lakehouse/silver/products\` RETAIN 168 HOURS` | Same as bronze |
| `optimize_clickhouse` | `OPTIMIZE TABLE analytics.dim_product FINAL; OPTIMIZE TABLE analytics.dim_shop FINAL` | Deduplicates ReplacingMergeTree dimension tables |

### 4.2 Verify Maintenance Execution

```bash
# Check latest maintenance DAG run
docker compose -f source/deployment/compose.yaml exec airflow \
  airflow dags list-runs -d lakehouse_maintenance -o plain | head -3
```

### 4.3 Manual Maintenance Execution

If the scheduled maintenance was missed or you need to run it immediately:

```bash
docker compose -f source/deployment/compose.yaml exec airflow \
  airflow dags trigger lakehouse_maintenance
```

### 4.4 Manual Delta Table Inspection

```bash
docker exec airflow bash -c "
  cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo python -c \"
from pipeline.spark.session import build_session
spark = build_session('inspect')

# Check bronze file count
bronze = spark.read.format('delta').load('s3a://lakehouse/bronze/products')
print(f'Bronze: {bronze.count()} rows')

# Check silver file count
silver = spark.read.format('delta').load('s3a://lakehouse/silver/products')
print(f'Silver: {silver.count()} rows')

# Check if VACUUM is needed (many small files)
from pyspark.sql import functions as F
bronze_files = spark.sql('DESCRIBE DETAIL delta.\`s3a://lakehouse/bronze/products\`')
bronze_files.select('numFiles', 'sizeInBytes').show()

spark.stop()
  \"
"
```

**Rule of thumb:** If `numFiles` exceeds 50 for a table with <1000 rows, run `OPTIMIZE` manually.

### 4.5 Post-Maintenance Health Check

After any manual or scheduled maintenance, verify the pipeline still works:

```bash
# 1. Quality check
docker exec airflow bash -c "
  cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo python -m pipeline.quality.checks
"

# 2. Run full test suite
make test
docker compose -f source/deployment/compose.yaml exec airflow \
  bash -c "cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo pytest pipeline/tests/ -q"

# 3. Trigger a smoke DAG run
docker compose -f source/deployment/compose.yaml exec airflow \
  airflow dags trigger tokopedia_products
```
