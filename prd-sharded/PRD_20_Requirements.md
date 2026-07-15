# PRD 20 — User Stories & Functional Requirements
> Versi 0.6 · Baca PRD_00 dulu. FR di bawah = DELTA terhadap kondisi awal repo.

## User Stories
- US-1: Sebagai analyst, gw mau lihat grafik harga produk selama 30 hari, supaya tahu kapan waktu terbaik membeli.
- US-2: Sebagai analyst, gw mau lihat produk dengan penurunan harga terbesar hari ini (flash sale detection).
- US-3: Sebagai analyst, gw mau membandingkan harga & rating antar shop/kota.
- US-4: Sebagai engineer, gw mau semua job termonitor di Airflow + tahu saat quality check gagal.
- US-5: Sebagai engineer, gw mau bisa reprocess dari bronze tanpa crawl ulang — dan ini DIUJI, bukan diasumsikan.
- US-6: Sebagai engineer, gw mau melihat kesehatan pipeline lintas run (rows, rejects, durasi) di satu dashboard.
- US-7: Sebagai operator, gw mau menambah/menonaktifkan keyword target lewat **UI**, tanpa menulis SQL manual atau menyentuh kode/DAG.

## Functional Requirements
| ID | Requirement | Prioritas |
|----|-------------|-----------|
| FR-1 | ClickHouse service di compose + DDL tabel gold | P0 |
| FR-2 | Loader gold → ClickHouse, **idempotent** (strategi via ADR-001: ReplacingMergeTree vs truncate-partition) | P0 |
| FR-3 | DAG `@hourly` + jitter; keyword & max_pages via Airflow Variables | P0 |
| FR-4 | Metabase di compose (profile `bi`), connect ClickHouse, dashboard US-1..US-3 | P0 |
| FR-5 | Superset di compose (profile `bi`, + metadata DB Postgres sendiri), connect ClickHouse, dashboard US-1..US-3 | P0 |
| FR-6 | Task `quality_check`: row count > 0, null % < 5, price_idr > 0, freshness < 2 jam, **rejects ratio < 10%** (anti silent failure) | P0 ↑ |
| FR-7 | dbt tests di schema.yml (unique, not_null pada keys) | P1 |
| FR-8 | Alerting DAG gagal (email/Telegram callback) | P1 |
| FR-9 | Lakehouse pindah ke AWS S3 asli (env-driven) | P1 |
| FR-10 | `docs/bi-comparison.md`: Metabase vs Superset dari pengalaman langsung | P1 |
| FR-13 | CI via GitHub Actions: lint + pytest tiap push, badge di README | P0 |
| FR-14 | Audit table `pipeline_runs` di ClickHouse (lihat PRD_10) + dashboard pipeline health di BI | P1 |
| FR-15 | DAG `lakehouse_maintenance` @weekly: OPTIMIZE + VACUUM Delta | P1 |
| FR-16 | Uji reprocess: rebuild silver dari bronze tanpa crawl ulang, hasil identik | P1 |
| FR-17 | Tabel `crawl_assets` di Postgres + seed file YAML + skrip seeding (PRD_50) | P0 |
| FR-18 | DAG membaca due assets → `crawl.expand()` dynamic task mapping → update `last_crawled_at`/`last_status` | P0 |
| FR-19 | Circuit breaker: `consecutive_failures >= 5` → asset di-nonaktifkan otomatis | P1 |
| FR-20 | Dashboard "Asset Health": asset aktif, terakhir di-crawl, failure rate per asset | P2 |
| FR-22 | Admin UI Streamlit (`assets/app.py`) untuk CRUD asset registry — tab Daftar/Tambah/Edit-Hapus/Bermasalah (PRD_50) | P0 |
| FR-21 | Beanstalkd frontier + crawler worker (Pola B, upgrade) — hanya jika prasyarat PRD_50 terpenuhi | P2 |
| FR-11 | Tracking product-detail untuk harga lebih akurat per produk terpilih | P2 |
| FR-12 | Aktifkan jalur Elasticsearch untuk product search | P2 |

## Success Metrics
1. `docker compose up` (profile default) → semua service healthy.
2. DAG @hourly jalan otomatis; satu run: crawl → dashboard ter-update tanpa langkah manual.
3. **Reprocess dari bronze terbukti via FR-16** (bukan klaim).
4. Dashboard US-1..US-3 tersedia di **kedua** BI tools + dashboard pipeline health (US-6).
5. Quality check pernah menangkap data buruk, TERMASUK skenario rejects membengkak.
6. Lakehouse di AWS S3 dengan biaya ~Rp0 (free tier).
7. CI hijau; badge tampil di README.
