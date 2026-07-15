# PRD 60 — Production Hardening
> Versi 1.0 · Baca PRD_00 dulu. FR di bawah = DELTA terhadap kondisi Fase 0–4 (complete).
> Scope: upgrade dari portfolio-grade ke production-ready.

## Ringkasan

Pipeline sudah berjalan end-to-end (Fase 0–4). Yang belum: security, monitoring, CI/CD deployment, data retention, dan incremental processing. Dokumen ini mendefinisikan apa yang perlu ditambah supaya pipeline bisa di-deploy ke production (AWS atau on-prem).

---

## User Stories (Production Ops)

| ID | Story |
|---|---|
| US-10 | Sebagai SRE, gw mau ada dashboard monitoring (Grafana/Prometheus) yang nunjukin health semua service, latency DAG, dan throughput Kafka — tanpa harus SSH ke server. |
| US-11 | Sebagai DevOps, gw mau semua secret (password, API key, token) dikelola lewat Vault/Secrets Manager, bukan env vars atau file `.env` di repo. |
| US-12 | Sebagai engineer, gw mau pipeline otomatis ter-deploy (CI/CD) tiap push ke main — build image, run test, deploy ke staging → production. |
| US-13 | Sebagai data engineer, gw mau ada retention policy: bronze disimpan 90 hari, silver 180 hari, gold permanent — dengan enforcement otomatis, bukan manual. |
| US-14 | Sebagai data engineer, gw mau silver diproses secara incremental (bukan full rebuild) setelah data > 10,000 rows — supaya runtime gak naik linear. |
| US-15 | Sebagai security engineer, gw mau semua koneksi antar service pakai TLS, API key di-rotate otomatis, dan access log tersentralisasi. |

---

## Functional Requirements

| ID | Requirement | Prioritas | Fase |
|----|-------------|-----------|------|
| **FR-30** | **Monitoring stack:** Prometheus (metrics) + Grafana (dashboard) di compose, ekspor metrics dari Airflow, Kafka, Spark, ClickHouse, Postgres | P0 | 6 |
| FR-31 | Dashboard Grafana pre-built: DAG success rate, Kafka consumer lag, Spark job duration, ClickHouse query latency, service health (UP/DOWN) | P0 | 6 |
| FR-32 | Alerting rule Prometheus: DAG gagal 2x berturut-turut → Alertmanager → Telegram/Discord (gantikan webhook callback saat ini) | P0 | 6 |
| **FR-33** | **Secret management:** Ganti semua hardcoded password/env vars dengan HashiCorp Vault (dev) atau AWS Secrets Manager (prod) | P0 | 6 |
| FR-34 | Airflow Connections ke Kafka/Postgres/ClickHouse lewat Vault backend, bukan env vars | P0 | 6 |
| FR-35 | Rotasi credential otomatis: Postgres password, ClickHouse password, MinIO access key — rotate tiap 90 hari via Vault | P1 | 7 |
| **FR-36** | **CI/CD pipeline:** GitHub Actions build Docker image → run integration test → push ke registry → deploy ke staging → smoke test → deploy ke production | P0 | 6 |
| FR-37 | Deployment strategy: rolling update (docker compose) atau blue-green (k8s). Rollback otomatis kalau health check gagal. | P0 | 6 |
| FR-38 | Environment promotion: `dev` (local) → `staging` (dedicated VM) → `prod` (AWS/k8s). Tiap environment punya config sendiri via Vault path. | P1 | 7 |
| **FR-39** | **Data retention policy:** Bronze Delta — 90 hari (VACUUM dengan retain 2160 hours). Silver Delta — 180 hari. Gold DuckDB — permanent (compact only). | P0 | 7 |
| FR-40 | Retention enforcement via DAG `data_retention` @monthly — otomatis VACUUM data di luar retention window. | P0 | 7 |
| FR-41 | Cold storage: data > 180 hari di-export ke Parquet di S3 bucket terpisah (glacier) sebelum dihapus dari lakehouse. | P2 | 8 |
| **FR-42** | **Incremental silver:** Ganti `mode("overwrite")` dengan `MERGE` incremental — baca hanya offset baru dari bronze, update silver. | P1 | 7 |
| FR-43 | Benchmark: full rebuild vs incremental. Target: incremental < 10% runtime full rebuild untuk data 10,000+ rows. | P1 | 7 |
| FR-44 | Backfill mode: tetap support full rebuild via flag `--full-refresh` untuk koreksi historis. | P2 | 7 |
| **FR-45** | **TLS/SSL:** HTTPS di semua endpoint eksternal (Airflow, Superset, Metabase, MinIO console) via reverse proxy (nginx/Caddy) + Let's Encrypt | P1 | 7 |
| FR-46 | Internal service-to-service: Kafka SASL_SSL, Postgres TLS, ClickHouse TLS — enable via config flag. | P2 | 8 |
| FR-47 | Access log aggregation: Fluentd/Fluent Bit → Elasticsearch → Kibana (existing) — semua service log terpusat. | P1 | 7 |
| **FR-48** | **Disaster recovery:** Backup Postgres mart + control schema harian ke S3. Backup ClickHouse via `clickhouse-backup`. Restore procedure terdokumentasi + teruji. | P0 | 6 |
| FR-49 | Recovery Time Objective (RTO): 4 jam. Recovery Point Objective (RPO): 1 jam (data di Kafka + Delta tidak hilang; yang perlu restore: Postgres + ClickHouse). | P0 | 6 |
| **FR-50** | **Kubernetes migration (opsional):** Helm chart untuk semua service. Gunakan & adaptasi manifest existing di `source/deployment/`. | P2 | 8 |

---

## Non-Goals (tetap defer)

| Item | Alasan |
|---|---|
| Multi-AZ / multi-region deployment | Overhead biaya; single-AZ cukup untuk skala portfolio-to-production awal |
| Auto-scaling (KEDA/HPA) | Traffic Tokopedia API dibatasi client-side rate limit; auto-scale gak relevan sebelum asset > 1000 |
| Data mesh / data catalog (DataHub/Amundsen) | Overkill untuk 3 tabel gold |
| Real-time streaming dashboard (sub-second) | `@hourly` batch sudah memenuhi use case harga harian |
| SLA 99.9% uptime | Single-node deployment; targetkan 99% (8.7 jam downtime/bulan acceptable) |

---

## Success Metrics (Production)

1. **Monitoring:** Grafana dashboard menunjukkan semua service UP > 99% dalam 30 hari.
2. **Alerting:** Prometheus alert terkirim < 60 detik setelah DAG failure.
3. **Secrets:** 0 hardcoded credential tersisa di repo atau env vars. Semua lewat Vault/Secrets Manager.
4. **CI/CD:** Push ke main → deploy ke production < 15 menit, full automated.
5. **Retention:** VACUUM berjalan otomatis, data > 90 hari (bronze) tidak ada di lakehouse.
6. **Incremental:** Silver incremental runtime < 10% full rebuild.
7. **DR:** Restore dari backup berhasil dalam < 4 jam (diuji per kuartal).
8. **TLS:** 0 endpoint HTTP plaintext — semua HTTPS.

---

## Dependensi & Risiko

| Risiko | Mitigasi |
|---|---|
| Vault setup kompleks | Mulai dengan Vault dev mode di compose; production pakai AWS Secrets Manager (lebih sederhana) |
| Incremental MERGE bisa lambat tanpa partition pruning | Pastikan partition strategy (toYYYYMM) mendukung incremental; benchmark sebelum switch |
| CI/CD butuh runner self-hosted untuk Spark test | Gunakan GitHub Actions larger runner (ubuntu-latest-8-cores) atau Docker-in-Docker |
| TLS certificate renewal | Gunakan Caddy (auto HTTPS + Let's Encrypt) — zero-config renewal |
| Prometheus + Grafana resource usage | Mulai dengan scrape interval 30s (bukan 15s), retention 7 hari; tambah resource kalau perlu |
