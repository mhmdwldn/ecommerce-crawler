# TASKS.md — Breakdown Eksekusi (v0.6, sinkron dengan PRD v0.6)
> Aturan main: **satu task = satu sesi AI**. Sebelum mulai task, suruh AI baca `CLAUDE.md`.
> Format: `[ ]` todo · `[~]` in progress · `[x]` done. Tulis tanggal selesai + catatan singkat.
> Jangan lompat fase. Task ✋ = butuh keputusan/aksi manual lo, bukan AI.

---

## Fase 0 — Validasi Baseline (JANGAN ubah kode apa pun di fase ini)
Ref: PRD_00 (kondisi awal), PRD_40 keputusan #4

- [x] 0.1 Baca `CLAUDE.md` dan `README.md` — didokumentasikan, 4 crawler type, full pipeline guide
- [x] 0.2 `docker compose up` — 7 service healthy, 3.9 GB RAM, ES + Airflow terberat. Fix: Kafka NodeExists, Airflow PID stale
- [x] 0.3 Crawler scrape stdout — 20 produk "poco f8", 0 null, 20/20 rating
- [x] 0.4 Crawler → Kafka — 20 event di 3 partisi, console consumer verified
- [x] 0.5 `stream_bronze` → MinIO — 20 row baru, stale checkpoint issue fixed
- [x] 0.6 Silver + dbt + load → 120 silver rows, 11/11 dbt PASS, Postgres 120 fct snapshots
- [x] 0.7 DAG trigger — 5/5 tasks SUCCESS, 2 runs (manual + backfill), ~90s
- [x] 0.8 ✋ `docs/baseline-notes.md` — 5 error/fix, resource, durasi, rekomendasi
- [x] 0.9 CI GitHub Actions — ruff clean + 60 pytest pass, badge di README
- [x] 0.10 `Makefile` (up/down/crawl/smoke/test/lint) + `.env.example` (sudah ada)
- **Definition of Done fase 0:** satu trigger DAG menghasilkan data baru di Postgres tanpa intervensi manual

## Fase 1 — ClickHouse Serving Layer (FR-1, FR-2)
Ref: PRD_10 (DDL guideline), PRD_40 ADR-001

- [ ] 1.1 Tambah service `clickhouse` di compose + healthcheck + volume. Verifikasi: `clickhouse-client` bisa connect
- [ ] 1.2 Tulis DDL di `warehouse/clickhouse/ddl/`: `fct_product_snapshot`, `dim_product`, `dim_shop` (MergeTree, partisi toYYYYMM, ORDER BY sesuai PRD_10)
- [ ] 1.3 ✋ Spike ADR-001: coba dua opsi loader secara kasar (script DuckDB→CH vs dbt-clickhouse), 1–2 jam per opsi
- [ ] 1.4 ✋ Tulis `docs/decisions/ADR-001-clickhouse-loader.md`: konteks, opsi, keputusan, konsekuensi — **wajib mencakup strategi idempotensi** (ReplacingMergeTree vs truncate-partition-insert). Update PRD_40
- [ ] 1.5 Implementasi loader sesuai ADR-001 + idempotent (rerun tidak menduplikasi data)
- [ ] 1.6 Tambah task `load_clickhouse` di DAG setelah dbt_build → trigger DAG → data sampai ClickHouse
- [ ] 1.7 Test: `pipeline/tests/test_clickhouse_load.py` (row count CH == row count gold)
- **DoD fase 1:** trigger DAG → `select count(*) from fct_product_snapshot` di ClickHouse bertambah

## Fase 2 — Hourly + Quality (FR-3, FR-6, FR-7)
Ref: PRD_20, PRD_40 (risiko hourly)

- [ ] 2.1 Pindahkan keyword & max_pages ke Airflow Variables; default konservatif (1 keyword, 2 pages)
- [ ] 2.2 Ubah schedule ke `@hourly` + jitter (sleep acak 0–5 menit di task pertama)
- [ ] 2.3 Biarkan jalan ±6 jam ✋ → cek: ada run gagal? error rate crawler? duplikasi data?
- [ ] 2.4 Buat `pipeline/quality/checks.py`: row_count>0, null%<5, price_idr>0, freshness<2 jam, **rejects ratio<10%** — exit non-zero jika gagal
- [ ] 2.5 Tambah task `quality_check` di DAG (setelah silver, sebelum dbt)
- [ ] 2.6 Tambah dbt tests di `schema.yml`: unique+not_null pada snapshot_id, product_id, shop_id
- [ ] 2.7 Uji negatif: inject data rusak (price=0) ke silver → pipeline HARUS gagal di quality_check
- [ ] 2.8 Uji negatif rejects: inject event dengan schema rusak ke Kafka → rejects naik → quality_check menangkap (anti silent failure)
- [ ] 2.9 Audit table: DDL `pipeline_runs` di ClickHouse + tiap run DAG menulis 1 baris (run_id, rows per layer, rejects, durasi, status) (FR-14)
- [ ] 2.10 Uji reprocess (FR-16): hapus silver → rebuild dari bronze → row count & sample identik. Catat langkahnya di docs
- [ ] 2.11 DAG `lakehouse_maintenance` @weekly: OPTIMIZE + VACUUM bronze & silver (FR-15)
- **DoD fase 2:** pipeline jalan tiap jam semalaman tanpa gagal, terbukti menolak data buruk (termasuk silent failure via rejects), reprocess dari bronze terbukti, dan tiap run tercatat di `pipeline_runs`

## Fase 2.5 — Asset Registry / Control Plane (FR-17, FR-18, FR-19)
Ref: **PRD_50** (wajib dibaca), PRD_40 keputusan #7 & #8

- [ ] 2.5.1 DDL `assets/ddl/crawl_assets.sql` di Postgres (schema lengkap ada di PRD_50)
- [ ] 2.5.2 Seed file `assets/seeds/targets.yaml` (mulai 3–5 keyword saja) + `assets/seed.py` (upsert YAML→Postgres, idempotent)
- [ ] 2.5.3 `assets/repository.py`: `get_due_assets()` (aturan due di PRD_50) + `update_status(asset_id, status)`
- [ ] 2.5.4 Refactor DAG: task `get_due_assets` → `crawl.expand(asset)` dynamic task mapping → `update_asset_status`
- [ ] 2.5.5 Set `max_active_tasks` di DAG (mitigasi fan-out) — mulai konservatif, mis. 2
- [ ] 2.5.6 Hapus keyword dari Airflow Variables (registry jadi satu-satunya sumber kebenaran) — update PRD_20 FR-3 kalau perlu
- [ ] 2.5.7 Circuit breaker (FR-19): `consecutive_failures >= 5` → `is_active=false`. Uji: bikin asset sengaja invalid → 5 run → nonaktif otomatis
- [ ] 2.5.8 Test: `tests/test_asset_repository.py` (due logic, circuit breaker, seed idempotent)
- [x] 2.5.9 ✋ Uji US-7: tambah keyword lewat UI Streamlit → muncul di antrian due tanpa sentuh kode ✅ diverifikasi
- [x] 2.5.10 Admin UI `assets/app.py` (FR-22): tab Daftar/Tambah/Edit-Hapus/Bermasalah — dibangun & diuji jalan (HTTP 200, tanpa error) ✅ **[revisi: awalnya Non-Goal di PRD_50, dicabut — keputusan #9]**
- [x] 2.5.11 Seed data awal: 23 asset (14 elektronik: POCO F8/F8 Pro/X7 Pro/M7 Pro, iPhone 17/17 Pro Max/16, Galaxy S25 Ultra, dll; 9 fashion: sneakers, hoodie, dress, dll) — `assets/seeds/targets.yaml` ✅
- [x] 2.5.12 Test suite `assets/tests/test_asset_registry.py`: due-logic, circuit breaker, CRUD, guard duplikat, seed builder — **15/15 pass** ✅
- **DoD fase 2.5:** keyword dikelola 100% dari registry lewat UI; DAG fan-out otomatis per asset; asset gagal beruntun mati sendiri — **[status: kode+test selesai, tinggal integrasi ke DAG riil — task 2.5.4]**

## Fase 3 — Dual BI (FR-4, FR-5)
Ref: PRD_20 US-1..US-3

- [ ] 3.1 Tambah Metabase di compose dengan `--profile bi-metabase` + koneksi ke ClickHouse (driver CH untuk Metabase)
- [ ] 3.2 Dashboard Metabase: US-1 (price trend 30 hari), US-2 (top price drops today), US-3 (perbandingan shop/kota)
- [ ] 3.3 Export/serialize dashboard Metabase → `dashboards/metabase_exports/`
- [ ] 3.4 Tambah Superset dengan `--profile bi-superset` (butuh metadata DB Postgres sendiri — jangan pakai Postgres mart) + koneksi ClickHouse (clickhouse-connect)
- [ ] 3.5 Dashboard Superset: US-1..US-3 yang sama
- [ ] 3.6 Export dashboard Superset → `dashboards/superset_exports/`
- [ ] 3.7 Dashboard "Pipeline Health" dari tabel `pipeline_runs` (US-6): rows/run, rejects trend, durasi, status — di salah satu BI tool
- [ ] 3.8 Dashboard "Asset Health" (FR-20): asset aktif vs nonaktif, last_crawled_at, failure rate per asset
- **DoD fase 3:** US-1..US-3 terjawab di KEDUA tools + dashboard pipeline health jalan; export tersimpan di repo

## Fase 4 — Dokumentasi & Alerting (FR-8, FR-10)
- [ ] 4.1 ✋ Tulis `docs/bi-comparison.md`: setup effort, kemudahan, fitur, performa query CH, verdict per use case
- [ ] 4.2 Alerting: `on_failure_callback` DAG → Telegram/email
- [ ] 4.3 Tulis `docs/architecture.md` (diagram + penjelasan aliran data + kenapa medallion)
- [ ] 4.4 Rewrite README: quickstart <30 menit, arsitektur ringkas, screenshot dashboard
- [ ] 4.5 ✋ Minta 1 teman coba jalankan dari README → catat di mana dia nyangkut → perbaiki
- **DoD fase 4:** orang lain bisa reproduce dari nol dalam <30 menit

## Fase 5 — AWS S3 (FR-9)
Ref: PRD_40 (risiko biaya)

- [ ] 5.1 ✋ Setup akun AWS: billing alert $1, IAM user khusus project (bukan root), credentials via env
- [ ] 5.2 Buat bucket S3 + ganti endpoint/credentials via env → bronze menulis ke S3 asli
- [ ] 5.3 Jalankan full pipeline dengan lakehouse di S3; verifikasi silver+dbt membaca dari S3
- [ ] 5.4 Dokumentasikan langkah migrasi + perbandingan MinIO vs S3 di `docs/architecture.md`
- **DoD fase 5:** pipeline penuh jalan dengan lakehouse di AWS S3, biaya tetap $0

## Backlog v2 (JANGAN dikerjakan sebelum fase 5 selesai)
- **FR-21 Beanstalkd frontier (Pola B)** — ✋ CEK PRASYARAT di PRD_50 dulu: Pola A stabil ≥1 minggu DAN ada alasan nyata (asset >100 / butuh scale-out worker / butuh retry semantics). Kalau belum, JANGAN dibangun.
  - [ ] ADR-002: beanstalkd vs Redis+RQ vs Celery
  - [ ] Service beanstalkd di compose + tube `crawl.tokopedia`
  - [ ] DAG jadi producer (push job) — putuskan cara tahu batch selesai (sensor `pipeline_runs`?)
  - [ ] Crawler worker long-running: reserve → crawl → publish Kafka → delete/release(delay)/bury
  - [ ] Uji: worker mati di tengah job → job kembali ke queue setelah TTR (tidak hilang)
- FR-11 tracking product-detail per produk terpilih
- FR-12 Elasticsearch product search (driver sudah ada)
- SCD Type 2 untuk dim_product
- Price drop alert ke Telegram (fitur, bukan cuma alerting error)
