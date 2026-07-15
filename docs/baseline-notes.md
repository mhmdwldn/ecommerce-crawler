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

**Tanggal:** (belum)

### Yang diverifikasi

_(isi setelah fase selesai)_

### Error & patch

_(isi setelah fase selesai)_

---

## Fase 2.5 — Asset Registry

**Tanggal:** (belum)

### Yang diverifikasi

_(isi setelah fase selesai)_

### Error & patch

_(isi setelah fase selesai)_

---

## Fase 3 — Dual BI

**Tanggal:** (belum)

### Yang diverifikasi

_(isi setelah fase selesai)_

---

## Fase 4 — Dokumentasi & Alerting

**Tanggal:** (belum)

### Yang diverifikasi

_(isi setelah fase selesai)_

---

## Fase 5 — AWS S3

**Tanggal:** (belum)

### Yang diverifikasi

_(isi setelah fase selesai)_
