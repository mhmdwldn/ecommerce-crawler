# PRD 40 — Risks, Mitigasi & Keputusan
> Versi 0.6 · Baca PRD_00 dulu.

## Risks & Mitigasi
| Risiko | Mitigasi |
|--------|----------|
| Tokopedia mengubah GraphQL schema / memblokir (ToS area abu-abu) | Rate limit sudah ada; scope kecil (Variable `max_pages` rendah); jitter; jika diblokir → pipeline tetap teruji via replay bronze / file driver |
| **Silent failure**: schema berubah → rejects membengkak diam-diam, DAG tetap hijau | Quality check `rejects ratio < 10%` (FR-6) + audit table `pipeline_runs` (FR-14) + dashboard pipeline health |
| **Delta small files**: hourly run → ribuan file kecil → query lambat | DAG `lakehouse_maintenance` @weekly: OPTIMIZE + VACUUM (FR-15) |
| Rerun DAG menduplikasi data di ClickHouse | Strategi idempotensi diputuskan eksplisit di ADR-001 (ReplacingMergeTree vs truncate-partition) |
| Hourly = 24x request @daily | Mulai 1 keyword × 1–2 halaman; monitor error rate via pipeline_runs; naikkan bertahap |
| Laptop berat (Kafka+Spark+CH+Airflow+2 BI) | Docker profiles: `default`, `bi-metabase`/`bi-superset` (satu saja), `search` off |
| Dua serving layer (Postgres + ClickHouse) membingungkan | ADR-001 memutuskan; Postgres di-deprecate setelah CH stabil |
| Biaya AWS | Hanya S3 free tier 5GB; billing alert $1; credentials via .env (ada .env.example), tidak pernah di-commit |
| Timezone campur aduk UTC/WIB di dashboard | Keputusan #6: simpan UTC di semua layer, konversi hanya di BI |
| **Fan-out crawl**: registry bertambah → dynamic task mapping membanjiri Tokopedia sekaligus | Batasi `max_active_tasks` di DAG + `priority`/`cadence_min` per asset + jitter; naikkan jumlah asset bertahap |
| Asset yang diblokir terus di-retry → memperburuk blocking | Circuit breaker FR-19: 5 kegagalan beruntun → `is_active=false` |
| Over-engineering: beanstalkd dibangun sebelum dibutuhkan | Prasyarat eksplisit di PRD_50 (Pola A stabil ≥1 minggu + alasan nyata) sebelum Pola B boleh dimulai |

## Keputusan
- [x] #1 Target: **Tokopedia** via repo existing (GraphQL httpx, bukan Playwright).
- [x] #2 BI: **Metabase DAN Superset** → docs/bi-comparison.md.
- [x] #3 Frekuensi: **@hourly** + jitter.
- [x] #4 Strategi: **build on top of existing repo**; fase 0 = validasi baseline.
- [x] #6 Timezone: **simpan UTC di semua layer**, konversi WIB hanya di BI layer.
- [x] #7 Asset registry: **Postgres + JSONB**, bukan MongoDB (alasan di PRD_50).
- [x] #9 Admin UI: **Streamlit CRUD** (`assets/app.py`) — mencabut Non-Goal awal di PRD_50. Alasan: operasi harian (tambah/nonaktif keyword) lewat SQL manual tidak praktis.
- [x] #8 Frontier: **bertahap** — Pola A (Airflow dynamic task mapping) di v1; beanstalkd (Pola B) sebagai upgrade P2 dengan prasyarat.
- [ ] #5 ADR-001 (fase 1): loader ClickHouse — DuckDB→CH script vs dbt-clickhouse — **WAJIB mencakup strategi idempotensi** (ReplacingMergeTree vs truncate-partition-insert).

## Changelog
- v0.1 draft · v0.2 resolved Tokopedia/dual BI/hourly · v0.3 rewrite pasca-audit repo + sharded.
- **v0.6:** Streamlit admin UI dibangun (`assets/app.py`, 4 tab) + 23 asset seed (14 elektronik incl. POCO F8/F8 Pro/X7 Pro/M7 Pro, iPhone 17/17 Pro Max/16, dst; 9 fashion). Semua diuji nyata: DDL applied, seed idempotent (23→23 setelah rerun), due-logic, circuit breaker 5x-gagal→nonaktif, CRUD, guard duplikat — 15/15 pytest pass. Non-Goal 'UI registry' di PRD_50 dicabut → keputusan #9. +FR-22.
- **v0.5:** +G8 control plane; +PRD_50 (asset registry & frontier); +FR-17..21; +US-7; +fase 2.5; +keputusan #7 (Postgres+JSONB) & #8 (bertahap Pola A→B); +risiko fan-out & over-engineering.
- **v0.4 hasil review:** +G7 engineering hygiene; +FR-13 CI, FR-14 audit table pipeline_runs, FR-15 Delta maintenance, FR-16 uji reprocess; FR-6 diperluas rejects ratio (naik P0); FR-5 dicatat butuh metadata DB; +keputusan #6 timezone UTC; +risiko silent failure & small files; ADR-001 diperluas idempotensi; +Makefile, .env.example, CI di struktur folder.
