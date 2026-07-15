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

- [x] 1.1 Tambah service `clickhouse` di compose + healthcheck + volume — ✅ ClickHouse 24.8, port 8123, 347 MB RAM
- [x] 1.2 Tulis DDL di `warehouse/clickhouse/ddl/` — ✅ 3 tabel (2 ReplacingMergeTree + 1 MergeTree), toYYYYMM
- [x] 1.3 ✋ Spike ADR-001 — ✅ Opsi A (script) vs B (dbt-clickhouse), pilih A: single transform source, consistent pattern
- [x] 1.4 ✋ Tulis `docs/decisions/ADR-001-clickhouse-loader.md` — ✅ diputuskan: Opsi A, truncate-partition-insert untuk fct, ReplacingMergeTree untuk dims
- [x] 1.5 Implementasi loader sesuai ADR-001 + idempotent — ✅ `pipeline/load/load_to_clickhouse.py`, fct truncate-partition-insert, dims ReplacingMergeTree, CH == PG (92/41/180)
- [x] 1.6 Tambah task `load_clickhouse` di DAG — ✅ 6/6 SUCCESS, CH == PG (112/52/200)
- [x] 1.7 Test: `pipeline/tests/test_clickhouse_load.py` — ✅ 3/3 passed (tables exist, row counts match, idempotent)
- [x] **DoD fase 1:** trigger DAG → fct 180→200 di ClickHouse ✅

## Fase 2 — Hourly + Quality (FR-3, FR-6, FR-7)
Ref: PRD_20, PRD_40 (risiko hourly)

- [x] 2.1 Airflow Variables — ✅ `crawl_keyword`, `crawl_max_pages`; DAG reads var → dag_run.conf fallback
- [x] 2.2 @hourly + jitter — ✅ schedule @hourly, sleep 0-300s, max_active_runs=1, retry_delay=2m
- [ ] 2.3 ✋ Biarkan jalan ±6 jam — cek error rate, duplikasi. **Belum — butuh run overnight.**
- [x] 2.4 `pipeline/quality/checks.py` — ✅ row_count, null_pct, price_positive, rejects_ratio (4 checks, exit non-zero)
- [x] 2.5 `quality_check` di DAG — ✅ silver >> quality_check >> dbt_build
- [x] 2.6 dbt tests schema.yml — ✅ sudah ada 7 tests (unique+not_null di semua PK)
- [x] 2.7 Uji negatif price=0 — ✅ inject price_idr=0 → quality_check FAIL (price_positive)
- [x] 2.8 Uji negatif rejects — ✅ inject 50 bad rows → rejects 14% → quality_check FAIL (rejects_ratio)
- [x] 2.9 Audit `pipeline_runs` — ✅ DDL ClickHouse + `pipeline/quality/audit.py` + `write_audit` task
- [x] 2.10 Uji reprocess — ✅ delete bronze → re-stream dari Kafka → silver count identik (220)
- [x] 2.11 DAG `lakehouse_maintenance` — ✅ @weekly, OPTIMIZE + VACUUM bronze/silver, OPTIMIZE FINAL CH dims
- **DoD fase 2:** pipeline jalan tiap jam semalaman tanpa gagal, terbukti menolak data buruk (termasuk silent failure via rejects), reprocess dari bronze terbukti, dan tiap run tercatat di `pipeline_runs`

## Fase 2.5 — Asset Registry / Control Plane (FR-17, FR-18, FR-19)
Ref: **PRD_50** (wajib dibaca), PRD_40 keputusan #7 & #8

- [x] 2.5.1 DDL `assets/ddl/crawl_assets.sql` — ✅ applied to Postgres (control.crawl_assets + v_due_assets)
- [x] 2.5.2 Seed — ✅ 23/23 assets (14 elektronik + 9 fashion), idempotent upsert
- [x] 2.5.3 `assets/repository.py` — ✅ get_due_assets(), mark_success(), mark_failure() + circuit breaker
- [x] 2.5.4 DAG integration — ✅ `pipeline/load/crawl_assets.py` replaces fixed-keyword crawl, reads registry
- [x] 2.5.5 `max_active_tasks=2` — ✅ set in DAG
- [x] 2.5.6 Airflow Variables removed — ✅ registry is single source of truth, dag_run.conf fallback
- [x] 2.5.7 Circuit breaker — ✅ 5 consecutive failures → is_active=false, verified
- [x] 2.5.8 Tests — ✅ 15/15 pass (pre-existing)
- [x] 2.5.9 ✋ Streamlit UI — ✅ pre-built
- [x] 2.5.10 Admin UI — ✅ pre-built (tabs: Daftar/Tambah/Edit/Bermasalah)
- [x] 2.5.11 Seed data — ✅ 23 assets pre-built
- [x] 2.5.12 Test suite — ✅ 15/15 pass (pre-existing)
- [x] **DoD fase 2.5:** keyword 100% dari registry; DAG crawl otomatis per due asset; circuit breaker berfungsi ✅

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
