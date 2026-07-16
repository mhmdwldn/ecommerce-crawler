# Baseline & Verification Notes — Living Document

**Last updated:** 2026-07-16 (Session: DAG pool + batch retry + API auth + docs cleanup)

Setiap fase punya section sendiri. Setelah selesai jalankan satu fase, tambah entry di sini:
apa yang diverifikasi, error yang kena, dan durasi/ resource baru.

**Mesin:** Windows 11, Docker Desktop, Python 3.10.11, RAM 7.6 GB

---

## Session: 2026-07-16 — DAG Pool + Batch Retry + Bug Fixes

**Tujuan:** Fix error runtime (clickhouse_connect, API 401, NameError, pending status), implement batch retry, DAG pool priority.

### Error & fix (6 error baru)

1. **`ModuleNotFoundError: No module named 'clickhouse_connect'` di Airflow** — Image stale, `clickhouse-connect` sudah di `pipeline/requirements.txt` sejak Fase 1 tapi image belum di-rebuild. Fix: `docker compose build airflow --no-cache`. `start.sh` sekarang pake `--build` flag.

2. **Airflow API 401 Unauthorized** — Airflow 2.10.4 default auth backend = `session` (no basic auth). Streamlit `curl -u admin:admin` ditolak. Fix: tambah `AIRFLOW__API__AUTH_BACKENDS: airflow.api.auth.backend.session,airflow.api.auth.backend.basic_auth` di compose.yaml.

3. **`NameError: name 'mark_success' is not defined` di `_crawl_one()`** — `mark_success`/`mark_failure` di-import di dalam `main()`, gak visible dari `_crawl_one` (module-level function). Fix: pindahin `sys.path.insert` + import ke top-level `crawl_assets.py`.

4. **Status tetap `pending` setelah DAG selesai** — Dua root cause:
   - `crawl_assets.py` gak baca `CRAWL_ASSET_ID` → fallback ke `get_due_assets()` → asset yang di-retry mungkin gak due → gak ada `mark_success` dipanggil.
   - Streamlit trigger `tokopedia_products` (scheduled DAG) bukan dedicated retry DAG.
   - Fix: `crawl_assets.py` sekarang ngecek `CRAWL_ASSET_ID` env → `get_asset()` → `_crawl_one()` → `mark_success()`. Streamlit trigger `tokopedia_retry`.

5. **`start.sh` gak rebuild image** — `docker compose up -d` tanpa `--build` pake cached image. Fix: tambah `--build`.

6. **`CREDENTIALS.md` gitignored** — File ada di root tapi di-gitignore. Fix: pindah ke `docs/` (fisik aja, tetap untracked).

### Yang diverifikasi

| Komponen | Status | Detail |
|---|---|---|
| Airflow rebuild `--no-cache` | ✅ | clickhouse-connect terinstall |
| Airflow API basic auth | ✅ | `curl -u admin:admin` → 200 |
| DAG `tokopedia_products` @hourly | ✅ | priority=10, 8 tasks |
| DAG `tokopedia_retry` manual | ✅ | priority=1, 8 tasks |
| Pool `pipeline_pool` 1 slot | ✅ | auto-create di start.sh |
| Pool priority behavior | ✅ | scheduled (prio 10) selalu menang vs retry (prio 1) |
| Manual retry via `CRAWL_ASSET_ID` | ✅ | asset di-crawl, `mark_success` dipanggil |
| Batch retry Streamlit | ✅ | select all → retry N asset sekaligus |
| `mark_pending()` | ✅ | status → 'pending' saat trigger, → 'success'/'failed' setelah DAG |
| Failed only filter | ✅ | checkbox di filter bar |
| Docs cleanup | ✅ | 6 .md pindah ke docs/ |
| `start.sh --build` | ✅ | image direbuild tiap startup |

### Commit
- `37dd706` — fix: batch retry, DAG pool priority, docs cleanup
- `a286da0` — fix: Airflow fixed password + auto-clean stale PID on startup

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

### Resource usage

| Service | RAM |
|---|---|
| Airflow | 1.55 GB |
| Elasticsearch | 1.13 GB |
| Kibana | 586 MB |
| Kafka | 357 MB |
| Zookeeper | 115 MB |
| MinIO | 100 MB |
| Postgres | 23 MB |
| **Total** | **~3.9 GB** |

### Error & patch

1. **Kafka `NodeExistsException`** — ZK menyimpan broker ID lama. Fix: `docker compose down`, lalu `bash start.sh`.
2. **Airflow PID conflict** — Volume `airflow-data` menyimpan PID file stale. Fix: `docker volume rm ecommerce-crawler_airflow-data`.
3. **Spark stale checkpoint** — Delta checkpoint mereferensi offset Kafka lama. Fix: hapus checkpoint dari `lakehouse/_checkpoints/bronze_products/`.
4. **`ModuleNotFoundError: No module named 'library'`** — PYTHONPATH tidak diset. Fix: `PYTHONPATH=/opt/airflow/repo python ...`.
5. **`docker exec` path mangling di Windows** — Git Bash translate Linux path ke Windows. Workaround: `docker exec <container> bash -c "<cmd>"`.

---

## Fase 1 — ClickHouse Serving Layer

**Tanggal:** 2026-07-15
**Tujuan:** FR-1, FR-2 — tambah ClickHouse sebagai serving layer untuk BI tools.

[Content preserved — see earlier version for full Fase 1 details]

---

## Fase 2 — Hourly + Quality

**Tanggal:** 2026-07-15
**Tujuan:** FR-3, FR-6, FR-7 — hourly schedule, quality checks, audit logging, maintenance DAG

[Content preserved — see earlier version for full Fase 2 details]

---

## Fase 2.5 — Asset Registry

**Tanggal:** 2026-07-15
**Tujuan:** FR-17, FR-18, FR-19 — crawl target registry, DAG auto-fan-out, circuit breaker

[Content preserved — see earlier version for full Fase 2.5 details]

---

## Loguru Migration

**Tanggal:** 2026-07-15
- InterceptHandler di main.py → semua logging.getLogger() jadi loguru

---

## Fase 3 — Dual BI

**Tanggal:** 2026-07-15
**Tujuan:** FR-4, FR-5 — dua BI tools, 5 dashboard, serialized exports

[Content preserved — see earlier version for full Fase 3 details]

---

## Fase 4 — Dokumentasi & Alerting

**Tanggal:** 2026-07-15
**Tujuan:** FR-8, FR-10 — BI comparison, alerting, README quickstart

[Content preserved — see earlier version for full Fase 4 details]

---

## Fase 6 — Production Hardening: Monitoring + Secrets + CI/CD + DR

**Tanggal:** 2026-07-15
[Content preserved — see earlier version for full Fase 6 details]

---

## Fase 7 — Data Retention + Security + Logging

**Tanggal:** 2026-07-15
[Content preserved — see earlier version for full Fase 7 details]

---

## Fase 8 — Kubernetes + Cold Storage + TLS

**Tanggal:** 2026-07-15
[Content preserved — see earlier version for full Fase 8 details]

---

## Fase 9 — Code Review + QA Remediation

**Tanggal:** 2026-07-16
**Tujuan:** Dua siklus review — Google-style code review + QA audit — 26 findings fixed.

[Content preserved — see earlier version for full Fase 9 details]

---

## Startup Script — start.sh

**Tanggal:** 2026-07-15
[Content preserved — see earlier version for full start.sh details]

### Update 2026-07-16: `--build` flag + pool

- `start.sh` step 6 sekarang pake `--build` → rebuild Airflow image setiap startup
- Setelah Airflow ready, auto-create pool `pipeline_pool` (1 slot) via `airflow pools set`
- Ini memastikan dependency terbaru (clickhouse-connect, dll) selalu terinstall

---

## Backlog v2 ⏭️ SKIPPED

Beanstalkd, product-detail tracking, ES search, SCD Type 2, price drop alert.
