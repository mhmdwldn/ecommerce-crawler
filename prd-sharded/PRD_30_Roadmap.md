# PRD 30 — Milestones & Struktur Folder
> Versi 0.6 · Baca PRD_00 dulu.

## Milestones
| Fase | Deliverable | Estimasi |
|------|-------------|----------|
| 0 | Repo jalan end-to-end di laptop: compose up → trigger DAG → data sampai Postgres mart. **Validasi baseline sebelum mengubah apa pun** | 2–4 hari |
| 1 | ClickHouse: service + DDL + loader (FR-1, FR-2) + ADR-001 | 1 minggu |
| 2 | DAG @hourly + Variables + quality check + dbt tests (FR-3, FR-6, FR-7) | 1 minggu |
| 2.5 | **Asset registry** (control plane): tabel + seed + dynamic task mapping + circuit breaker (FR-17..19) | 1 minggu |
| 3 | Metabase + Superset + 3 dashboard masing-masing (FR-4, FR-5) | 1–2 minggu |
| 4 | bi-comparison.md + alerting + polish README (FR-8, FR-10) | 3–5 hari |
| 5 | Migrasi lakehouse ke AWS S3 free tier + billing alert (FR-9) | 1 minggu |

## Struktur Folder (repo existing + tambahan ▶)
```
ecommerce-crawler/
├── CLAUDE.md                       ▶ entrypoint AI (router dokumen)
├── .github/workflows/ci.yml        ▶ BARU: lint + pytest (FR-13)
├── Makefile                        ▶ BARU: make up / crawl / smoke / test
├── .env.example                    ▶ BARU: template credentials (wajib sebelum fase 5)
├── PRD_00..40 *.md                 ▶ sharded PRD (dokumen ini)
├── TASKS.md                        ▶ breakdown task per fase
├── source/                         # crawler (ADA — jangan restrukturisasi)
│   └── deployment/compose.yaml     ▶ extend: +clickhouse, +metabase, +superset (profiles)
├── pipeline/
│   ├── spark/                      # ADA
│   ├── dbt/                        # ADA ▶ +tests di schema.yml
│   ├── load/load_to_clickhouse.py  ▶ BARU (jika ADR-001 pilih opsi A)
│   ├── quality/checks.py           ▶ BARU (+ rejects ratio, + tulis pipeline_runs)
│   └── airflow/dags/               # ADA ▶ @hourly + quality + dag lakehouse_maintenance
├── assets/                         ▶ BARU: control plane (PRD_50)
│   ├── ddl/crawl_assets.sql        ▶ tabel registry
│   ├── seeds/targets.yaml          ▶ daftar keyword awal (versioned di git)
│   ├── seed.py                     ▶ upsert YAML → Postgres
│   └── repository.py               ▶ get_due_assets(), update_status()
├── warehouse/clickhouse/ddl/       ▶ BARU
├── dashboards/{metabase,superset}_exports/  ▶ BARU
└── docs/
    ├── architecture.md             ▶ BARU
    ├── bi-comparison.md            ▶ BARU
    └── decisions/                  ▶ BARU (ADR-001 dst.)
```
