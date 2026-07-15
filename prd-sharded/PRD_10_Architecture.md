# PRD 10 — Arsitektur & Data Model
> Versi 0.5 · Baca PRD_00 dulu untuk konteks. Control plane: lihat PRD_50.

## Aliran Data (to-be)
```
Asset Registry @ Postgres (control plane) [BARU — G8, PRD_50]
   ↓ get_due_assets() → dynamic task mapping
Crawler Tokopedia (httpx→GraphQL)          [ADA]
   ↓ publish KafkaEvent
Kafka topic: tokopedia.products.raw        [ADA]
   ↓ Spark Structured Streaming (availableNow)
Delta BRONZE @ MinIO → AWS S3 (G5)         [ADA → migrasi]
   ↓ PySpark
Delta SILVER (typed, dedup, rejects)       [ADA]
   ↓ dbt (DuckDB)
GOLD: stg + dim_product/dim_shop/
      fct_product_snapshot                 [ADA]
   ↓ load
ClickHouse serving layer                   [BARU — G1]
   ↓
Metabase  +  Superset                      [BARU — G2]

Airflow @hourly mengorkestrasi semua       [ADA — ubah schedule, G3]
+ task data quality + audit pipeline_runs  [BARU — G4, G7]
```

## Pola ETL/ELT
EL streaming (crawler→Kafka→bronze raw), lalu T bertahap (silver via PySpark, gold via dbt) — pattern ELT medallion. Load gold → ClickHouse = tahap serving.

## Data Model (gold — sudah ada di dbt)
```
fct_product_snapshot: snapshot_id (md5 product_id+crawled_at), product_id,
                      shop_id, price_idr, discount_pct, rating, crawled_at
dim_product: product_id, name, url, category, ...
dim_shop:    shop_id, name, city, tier
```

## Audit Table (BARU — G7)
```
pipeline_runs @ ClickHouse:
  run_id, dag_run_ts, rows_crawled, rows_bronze, rows_silver,
  rows_rejects, rows_loaded_ch, duration_sec, status
```
Setiap run DAG menulis satu baris. Dipakai untuk: (a) deteksi silent failure
(rejects membengkak saat DAG tetap hijau), (b) dashboard "pipeline health" di BI.

## ClickHouse DDL Guideline
- Partisi: `toYYYYMM(crawled_at)` · `ORDER BY (product_id, crawled_at)` · DDL di `warehouse/clickhouse/ddl/`
- **Idempotensi loader (wajib diputuskan di ADR-001):** opsi (a) `ReplacingMergeTree` keyed by snapshot_id — dedup oleh engine; opsi (b) `MergeTree` + truncate-partition-then-insert per run. Rerun DAG tidak boleh menduplikasi data.

## Konvensi Timezone (keputusan #6)
Semua timestamp disimpan **UTC** di seluruh layer (bronze→ClickHouse). Konversi ke WIB (Asia/Jakarta) dilakukan HANYA di BI layer (setting timezone Metabase/Superset). Jangan pernah menyimpan waktu lokal di data.

## Maintenance Lakehouse (BARU)
Hourly run × Delta = akumulasi file kecil → query lambat. DAG terpisah `lakehouse_maintenance` (@weekly): `OPTIMIZE` (compaction) + `VACUUM` (retensi 7 hari) untuk bronze & silver.
