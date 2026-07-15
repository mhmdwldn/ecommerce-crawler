# Baseline & Verification Notes — Living Document

Setiap fase punya section sendiri. Setelah selesai jalankan satu fase, tambah entry di sini:
apa yang diverifikasi, error yang kena, dan durasi/ resource baru.

**Mesin:** Windows 11, Docker Desktop, Python 3.10.11, RAM 7.6 GB

---

## Fase 0 — Validasi Baseline

**Tanggal:** 2026-07-15
**Tujuan:** Verifikasi pipeline existing jalan end-to-end tanpa ubah kode.

### Yang jalan

| Komponen | Status | Detail |
|---|---|---|
| Crawler scrape (`search-product`) | ✅ | 20 produk, HTTP 200, ~1 detik |
| Crawler scrape (`search-shop`) | ✅ | 20 toko, HTTP 200 |
| Crawler full → Kafka | ✅ | 20 event ke `tokopedia.products.raw`, 3 partisi |
| Spark `stream_bronze` | ✅ | Kafka → Delta di MinIO (`s3a://lakehouse/bronze/products`) |
| Spark `silver` | ✅ | Bronze → typed + dedup di MinIO, 0 rejects |
| dbt `dbt build` | ✅ | 4 model + 7 test, 11/11 PASS |
| `load_to_postgres` | ✅ | DuckDB → Postgres mart, full reload |
| DAG `tokopedia_products` | ✅ | crawl → bronze → silver → dbt_build → load_postgres, semua SUCCESS |
| Kafka broker | ✅ | `localhost:9092` |
| Elasticsearch | ✅ | `localhost:9200`, cluster GREEN |
| MinIO | ✅ | `localhost:9000` (API), `:9001` (console), bucket `lakehouse` |
| Postgres mart | ✅ | `localhost:5433`, user/pass `mart/mart`, db `mart` |
| Airflow | ✅ | `localhost:8080`, standalone mode |

### Data quality

- Semua 20 produk punya `id`, `name`, `price`, `shop`, `rating` (0 null)
- Silver: 120 rows, 0 rejects (semua JSON valid)
- dbt tests: unique + not_null di semua PK — PASS
- Postgres: dim_product=92, dim_shop=41, fct_product_snapshot=180 (setelah 2 DAG run)

### Resource usage

| Service | RAM | Note |
|---|---|---|
| Airflow | 1.55 GB | standalone = webserver + scheduler + DB |
| Elasticsearch | 1.13 GB | JVM heap 512 MB |
| Kibana | 586 MB | |
| Kafka | 357 MB | |
| Zookeeper | 115 MB | |
| MinIO | 100 MB | |
| Postgres | 23 MB | |
| **Total** | **~3.9 GB** | dari 7.6 GB tersedia |

### Durasi satu DAG run

| Task | Waktu |
|---|---|
| crawl | ~3 detik |
| bronze | ~15 detik (Spark startup + read Kafka) |
| silver | ~20 detik (Spark startup + transform) |
| dbt_build | ~5 detik |
| load_postgres | ~1 detik |
| **Total** | **~90 detik** |

Spark cold-start (Ivy dependency resolve) mendominasi durasi bronze + silver.

### Error & patch

1. **Kafka `NodeExistsException`** — ZK menyimpan broker ID lama. Fix: `docker compose down` (bukan `stop`), lalu restart.
2. **Airflow PID conflict** — Volume `airflow-data` menyimpan PID file stale. Fix: `docker volume rm ecommerce-crawler_airflow-data`.
3. **Spark stale checkpoint** — Delta checkpoint mereferensi offset Kafka lama setelah topic re-create. Fix: hapus checkpoint objects dari `lakehouse/_checkpoints/bronze_products/`.
4. **`ModuleNotFoundError: No module named 'library'`** — PYTHONPATH tidak diset. Fix: `PYTHONPATH=/opt/airflow/repo python ...`.
5. **`docker exec` path mangling di Windows** — Git Bash translate Linux path ke Windows. Workaround: `docker exec <container> bash -c "<cmd>"`.

### Artifak baru

- `docs/baseline-notes.md` — file ini
- `.github/workflows/ci.yml` — ruff + pytest on push
- `ruff.toml` — linter config (line-length 120)
- `Makefile` — up/down/crawl/smoke/test/lint
- `README.md` badge CI + troubleshooting section

---

## Fase 1 — ClickHouse Serving Layer

**Tanggal:** 2026-07-15
**Tujuan:** FR-1, FR-2 — tambah ClickHouse sebagai serving layer untuk BI tools.

### Yang diverifikasi

| Komponen | Status | Detail |
|---|---|---|
| Service ClickHouse | ✅ | `clickhouse/clickhouse-server:24.8`, port 8123, 347 MB RAM |
| DDL 3 tabel | ✅ | `fct_product_snapshot` (MergeTree), `dim_product` + `dim_shop` (ReplacingMergeTree) |
| Loader `clickhouse-connect` | ✅ | 50 baris Python, mirror `load_to_postgres.py` |
| Idempotensi fct | ✅ | DROP PARTITION → INSERT, rerun tidak duplikasi |
| Idempotensi dims | ✅ | ReplacingMergeTree + OPTIMIZE FINAL |
| DAG `load_clickhouse` | ✅ | 6/6 SUCCESS, paralel dengan `load_postgres` |
| Test suite | ✅ | 3/3 passed (tables exist, row counts match, idempotent) |
| Data CH == PG | ✅ | 112/52/200 identik setelah DAG run |

### Data quality

- Row count CH == DuckDB gold == Postgres mart
- Pytest: `test_all_tables_exist_in_clickhouse`, `test_row_counts_match_gold`, `test_load_is_idempotent`

### Resource tambahan

| Service | RAM |
|---|---|
| ClickHouse | 347 MB |
| Total stack | ~4.3 GB (dari 3.9 GB baseline) |

### Error & patch

1. **`formatDateTime` vs `strftime`** — DuckDB SQL beda dari ClickHouse. Fix: pakai `strftime()` di DuckDB.
2. **dbt-clickhouse butuh `git`** — Airflow container tidak ada git. Tidak blocker (pilih Opsi A).

### Artifak baru

- `warehouse/clickhouse/ddl/` — 3 file DDL
- `pipeline/load/load_to_clickhouse.py` — loader
- `pipeline/tests/test_clickhouse_load.py` — 3 tests
- `docs/decisions/ADR-001-clickhouse-loader.md` — ADR

---

## Fase 2 — Hourly + Quality

**Tanggal:** 2026-07-15
**Tujuan:** FR-3, FR-6, FR-7 — hourly schedule, quality checks, audit logging, maintenance DAG

### Yang diverifikasi

| Komponen | Status | Detail |
|---|---|---|
| DAG @hourly | ✅ | schedule @hourly, jitter 0-300s, max_active_runs=1 |
| Airflow Variables | ✅ | crawl_keyword + crawl_max_pages, dag_run.conf fallback |
| quality/checks.py | ✅ | 5 checks: row_count, null_pct, price_positive, rejects_ratio, freshness |
| quality/audit.py | ✅ | pipeline_runs di ClickHouse, pakai shared CH client |
| DAG 8-task | ✅ | crawl → bronze → silver → quality_check → dbt → [pg, ch] → write_audit |
| Maintenance DAG | ✅ | @weekly OPTIMIZE + VACUUM bronze/silver, OPTIMIZE FINAL CH dims |
| Uji negatif price=0 | ✅ | quality_check FAIL — price_positive detected |
| Uji negatif rejects | ✅ | 52/372 rejects (14%) → quality_check FAIL — rejects_ratio detected |
| Reprocess | ✅ | delete bronze → re-stream dari Kafka → row count identik |
| Semua DAG run | ✅ | 4+ manual runs, semua SUCCESS |

### Data quality

- 5/5 quality checks pass dalam kondisi normal
- Pipeline gagal dengan benar saat data rusak (price=0, rejects 14%)
- Audit rows tercatat di ClickHouse `pipeline_runs`

### Error & patch

1. **`DROP PARTITION IF EXISTS` tidak support di CH 24.8** — Fix: Python try/except di load_to_clickhouse.py
2. **Audit always-record-success bug** — Fix: pass `dag_run.get_state()` via env var
3. **Connection leak di audit.py + checks.py** — Fix: try/finally untuk spark.stop(), duck.close(), ch.close()
4. **CH client duplicated 4x** — Fix: extract ke `pipeline/load/ch_client.py`
5. **Silver VACUUM missing** — Fix: tambah ke maintenance.py

### Artifak baru

- `pipeline/quality/checks.py` — 5 quality checks
- `pipeline/quality/audit.py` — audit logger
- `pipeline/quality/__init__.py` — quality package
- `pipeline/load/ch_client.py` — shared ClickHouse client
- `pipeline/spark/maintenance.py` — maintenance job
- `pipeline/airflow/dags/lakehouse_maintenance_dag.py` — weekly DAG
- `warehouse/clickhouse/ddl/pipeline_runs.sql` — audit table DDL

---

## Fase 2.5 — Asset Registry

**Tanggal:** 2026-07-15
**Tujuan:** FR-17, FR-18, FR-19 — crawl target registry, DAG auto-fan-out, circuit breaker

### Yang diverifikasi

| Komponen | Status | Detail |
|---|---|---|
| DDL + seed | ✅ | 23 assets, idempotent upsert |
| repository.py | ✅ | get_due_assets, mark_success, mark_failure |
| DAG integration | ✅ | crawl_assets.py reads registry, 10/run |
| Airflow Variables removed | ✅ | Registry single source of truth |
| Circuit breaker | ✅ | 5 consecutive failures → is_active=false |
| DAG runs | ✅ | 2 SUCCESS, 13 assets recorded |

### Error & patch

1. **loguru not in Airflow container** — main.py needs loguru. Fix: install + pipeline/requirements.txt.
2. **DDL failed via pipe** — `docker exec psql -f -` tidak apply. Fix: inline `psql -c` dengan SQL langsung.

### Artifak baru

- `pipeline/load/crawl_assets.py` — registry-driven crawler

---

## Loguru Migration

**Tanggal:** 2026-07-15

- InterceptHandler di main.py → semua logging.getLogger() jadi loguru
- Format warna + nama logger rata kiri 30 karakter
- Pipeline tidak berubah, 60/60 tests ok

---

## Fase 3 — Dual BI

**Tanggal:** 2026-07-15
**Tujuan:** FR-4, FR-5 — dua BI tools, 5 dashboard, serialized exports

### Yang diverifikasi

| Komponen | Status | Detail |
|---|---|---|
| Metabase | ✅ | v0.53.5, port 3000, metadata Postgres, connect to Postgres mart |
| Superset | ✅ | latest, port 8088, metadata Postgres, connect to ClickHouse |
| US-1 Price Trend | ✅ | SQL documented, dual dialect |
| US-2 Top Price Drops | ✅ | SQL documented |
| US-3 Shop/Kota | ✅ | SQL documented |
| Pipeline Health | ✅ | SQL documented (US-6) |
| Asset Health | ✅ | SQL documented (FR-20) |
| Setup scripts | ✅ | setup_metabase.py + setup_superset.py |
| Exports | ✅ | metabase_exports/ + superset_exports/ |

### Resource

| Service | RAM |
|---|---|
| Metabase | 831 MB |
| Superset | 225 MB |
| Total stack | ~5.3 GB (11 services) |

### Artifak baru

- `dashboards/dashboards.sql` — 5 dashboard queries
- `dashboards/setup_metabase.py` — API setup
- `dashboards/setup_superset.py` — API setup
- `dashboards/metabase_exports/` + `dashboards/superset_exports/`
- `source/deployment/compose.yaml` — +metabase +superset

---

## Fase 4 — Dokumentasi & Alerting

**Tanggal:** 2026-07-15
**Tujuan:** FR-8, FR-10 — BI comparison, alerting, README quickstart

### Yang diverifikasi

| Komponen | Status | Detail |
|---|---|---|
| BI comparison | ✅ | `docs/bi-comparison.md` — Metabase vs Superset |
| Alerting | ✅ | `pipeline/airflow/alerting.py` — webhook callback |
| architecture.md | ✅ | Updated to Fase 3 |
| README quickstart | ✅ | <15 menit, 5 commands |

### Artifak baru
- `docs/bi-comparison.md`
- `pipeline/airflow/alerting.py`

### Tersisa
- 4.5 ✋ Minta teman test quickstart

---

## Fase 6 — Production Hardening: Monitoring + Secrets + CI/CD + DR

**Tanggal:** 2026-07-15
**Tujuan:** Monitoring stack, Vault secrets, CI/CD pipeline, backup/DR

### Yang diverifikasi

| Komponen | Status | Detail |
|---|---|---|
| Prometheus | ✅ | `:9090`, scrape 6 targets (4 UP: prometheus/airflow/postgres/CH) |
| Grafana | ✅ | `:3001` (admin/admin), Pipeline Health dashboard |
| Alertmanager | ✅ | `:9093`, webhook config ready |
| postgres-exporter | ✅ | `:9187`, Postgres metrics → Prometheus |
| airflow-statsd | ✅ | StatsD→Prometheus bridge for Airflow metrics |
| Vault | ✅ | `:8200` (dev mode), 4 secrets stored, Airflow backend |
| CI/CD | ✅ | 5 test jobs + build → GHCR + smoke test |
| Rolling deploy | ✅ | `deploy.sh`: pull → restart → health check → auto-rollback |
| Backup | ✅ | `backup.sh`: PG dump + CH DDL + MinIO sync, 7-day retention |
| DR test | ✅ | Drop crawl_assets → DDL + seed → 23/23 restored |

### Resource

| Service | RAM |
|---|---|
| Prometheus | ~200 MB |
| Grafana | ~300 MB |
| Vault | ~100 MB |
| Exporters | ~50 MB |
| Total stack | ~6.5 GB (16 services) |

### Artifak baru
- `monitoring/prometheus.yml`, `monitoring/alerts.yml`, `monitoring/alertmanager.yml`
- `monitoring/statsd-mapping.yml`, `monitoring/clickhouse-prometheus.xml`
- `monitoring/dashboards/pipeline-health.json`
- `deploy.sh`, `backup.sh`
- `source/deployment/compose.cd.yaml`
- `.github/workflows/cd.yml`

---

## Fase 7 — Data Retention + Security + Logging

**Tanggal:** 2026-07-15
**Tujuan:** Retention, incremental silver, TLS proxy, log aggregation, env promotion

### Yang diverifikasi

| Komponen | Status |
|---|---|
| Data retention DAG @monthly | ✅ VACUUM bronze 90d, silver 180d |
| Silver incremental `--incremental` | ✅ MERGE via watermark |
| Backfill `--full-refresh` | ✅ Explicit full rebuild |
| Caddy reverse proxy `:8081` | ✅ Routes to 7 services |
| Fluent Bit → ES → Kibana | ✅ Log aggregation |
| Vault env promotion | ✅ `dev/staging/prod` paths |
| Credential rotation | ✅ Vault API pattern |

### Artifak
- `pipeline/spark/retention.py`, `pipeline/airflow/dags/data_retention_dag.py`
- `monitoring/Caddyfile`, `monitoring/fluent-bit.conf`
- Updated `silver.py` with incremental mode

### Stack: 18 services

---

## Fase 5 — AWS S3 ⏭️ SKIPPED (2026-07-15)

Belum ada akun AWS. Rencana: S3 bucket, ganti MinIO endpoint, dokumentasi migrasi.

## Backlog v2 ⏭️ SKIPPED (2026-07-15)

Beanstalkd, product-detail tracking, ES search, SCD Type 2, price drop alert.
