# PRD 00 — Overview, Goals & Non-Goals
> Versi 0.6 · Bagian dari sharded PRD. Entrypoint: CLAUDE.md

## Problem Statement
Harga produk e-commerce berubah dinamis (diskon, flash sale), tapi tidak ada cara mudah memantau pergerakan harga lintas waktu. Project ini **melanjutkan repo `ecommerce-crawler` yang sudah ada** menjadi pipeline analitik end-to-end: crawl harga Tokopedia per jam → Kafka → lakehouse (Delta di MinIO/S3) → ClickHouse → dashboard BI (Metabase & Superset).

**Tujuan sekunder:** menguasai PySpark, Airflow, Kafka, Delta Lake, dbt, ClickHouse, S3, konsep DWH & ETL/ELT — sebagai portfolio data engineering.

## Kondisi Awal (as-is, sudah ada di repo)
| Komponen | Status | Detail |
|----------|--------|--------|
| Crawler Tokopedia | ✅ | httpx → GraphQL gateway; 4 mode (search-product, search-shop, product-detail, product-reviews); rate limit + retry + Pydantic |
| Output drivers | ✅ | stdout, file, **kafka**, elasticsearch |
| Bronze | ✅ | Spark Structured Streaming, Kafka → Delta di MinIO, trigger `availableNow`, checkpoint |
| Silver | ✅ | PySpark: parse → typed table, dedup, rejects path |
| Gold | ✅ | dbt + DuckDB: `dim_product`, `dim_shop`, `fct_product_snapshot` |
| Mart | ✅ | DuckDB → Postgres (drop-and-recreate) |
| Orkestrasi | ✅ | Airflow DAG `tokopedia_products` (@daily) |
| Infra | ✅ | Compose: zookeeper, kafka, ES, kibana, minio, postgres, airflow |
| Tests | ✅ | crawler + pipeline (bronze, silver) |

## Goals (to-be)
1. **G1** — ClickHouse sebagai serving layer untuk gold tables.
2. **G2** — Dual BI: Metabase & Superset connect ke ClickHouse; dokumentasi perbandingan.
3. **G3** — Pipeline @hourly + jitter, idempotent.
4. **G4** — Data quality: task Airflow + dbt tests.
5. **G5** — Migrasi lakehouse MinIO → AWS S3 (free tier).
6. **G6** — Dokumentasi portfolio: architecture.md, bi-comparison.md, README reproducible <30 menit.
7. **G7** — Engineering hygiene: CI (lint+test tiap push), Makefile, observability via audit table `pipeline_runs`.
8. **G8** — Control plane terpisah: asset registry (Postgres+JSONB) sebagai sumber keyword/target crawl, menggantikan hardcode/Airflow Variable. Lihat PRD_50.

## Non-Goals (v1)
- ❌ Rewrite crawler — sudah jadi, dipakai apa adanya.
- ❌ ML / price prediction.
- ❌ Elasticsearch analytics (driver ada, tapi ES/Kibana off by default di v1).
- ❌ Real-time sub-detik; hourly micro-batch sudah sesuai karakter data harga.
- ❌ High availability / multi-region.
- ❌ MongoDB untuk asset registry (lihat PRD_50, keputusan #7).
- ❌ Beanstalkd di v1 — Pola A (Airflow dynamic task mapping) dulu; queue adalah upgrade P2.
