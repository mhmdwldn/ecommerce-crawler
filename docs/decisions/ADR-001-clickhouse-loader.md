# ADR-001 — ClickHouse Loader: Script vs dbt-clickhouse

**Status:** Diputuskan (2026-07-15)
**Konteks:** Fase 1 FR-1, FR-2 — tambah ClickHouse sebagai serving layer untuk BI tools (Metabase, Superset)

---

## Konteks

Pipeline existing sudah punya Postgres mart yang diisi dari DuckDB gold via script `load_to_postgres.py`. Sekarang kita tambah ClickHouse sebagai serving layer kedua untuk BI — tujuannya query analytics cepat (time-series, agregasi) tanpa bebani Postgres.

Pertanyaannya: gimana cara mindahin data dari gold (DuckDB) ke ClickHouse?

Ada 2 opsi yang di-spike (task 1.3):

---

## Opsi A: Script DuckDB → ClickHouse (`clickhouse-connect`)

Pattern yang sama dengan `load_to_postgres.py` yang sudah ada.

```python
duck = duckdb.connect(gold_db, read_only=True)
ch = clickhouse_connect.get_client(host=..., port=..., user=..., password=...)
for table in ["dim_product", "dim_shop", "fct_product_snapshot"]:
    rows = duck.execute(f"SELECT * FROM {table}").fetchall()
    ch.insert(table, rows, column_names=cols)
```

**Kelebihan:**
- 30 baris Python, copy dari pattern yang sudah terbukti
- 1 dependency (`clickhouse-connect`, pure Python)
- DuckDB tetap satu-satunya transformation engine — tidak ada logika yang dipecah
- Kalau nanti tambah serving layer lain (BigQuery, etc.), tinggal copy script
- Kecepatan: ~2 detik untuk 313 rows

**Kekurangan:**
- Idempotensi tidak built-in — harus di-handle manual
- Tidak ada dbt lineage untuk ClickHouse tables (tapi untuk serving layer ini gak relevan)
- Perlu jadwalkan OPTIMIZE FINAL untuk dedup dim tables

**Strategi idempotensi:**
- `fct_product_snapshot`: **truncate-partition-insert** — hapus partisi bulan ini sebelum insert. Sederhana, deterministik, tidak bergantung pada OPTIMIZE.
- `dim_product` & `dim_shop`: **ReplacingMergeTree + OPTIMIZE FINAL terjadwal** — dimensi adalah latest-state, jadi ReplacingMergeTree sudah cukup. OPTIMIZE FINAL dijadwalkan di DAG maintenance, atau query pakai `FINAL` di view BI.

## Opsi B: dbt-clickhouse

dbt models yang langsung materialize ke ClickHouse.

```yaml
# profiles.yml
ecommerce_ch:
  target: dev
  outputs:
    dev:
      type: clickhouse
      schema: analytics
      host: clickhouse
      port: 8123
```

**Kelebihan:**
- Idempotensi built-in — `table` materialization = CREATE OR REPLACE
- dbt lineage untuk ClickHouse tables
- Satu tool (dbt) untuk semua transformasi

**Kekurangan:**
- dbt tidak support cross-database queries — model ClickHouse tidak bisa baca dari DuckDB
- Akibatnya, harus salah satu dari:
  - Rewrite semua model dari DuckDB SQL ke ClickHouse SQL → **logika transformasi dipecah jadi dua**
  - Load silver ke ClickHouse dulu → **nambah kompleksitas, dua sumber kebenaran untuk silver**
  - Rewrite silver di ClickHouse → **Spark jadi tidak terpakai untuk layer ini**
- Setup lebih kompleks (dbt profile, adapter, dialect differences)
- Tidak konsisten dengan Postgres mart yang pakai script

## Keputusan

**Pilih Opsi A — script DuckDB → ClickHouse via `clickhouse-connect`.**

Alasan:

1. **Single source of truth untuk transformasi** — dbt-duckdb adalah satu-satunya tempat logika star schema dihitung. ClickHouse (seperti Postgres) hanya menerima salinan.
2. **Konsisten dengan arsitektur existing** — `load_to_postgres.py` sudah jalan dengan pattern ini. Dua serving layer, satu pattern.
3. **Paling sedikit perubahan** — tambah 1 file (`load_to_clickhouse.py`), 1 dependency (`clickhouse-connect`), 1 task DAG.

## Strategi idempotensi

```
fct_product_snapshot → truncate partition (month) lalu INSERT
dim_product          → ReplacingMergeTree + INSERT; OPTIMIZE FINAL di DAG maintenance
dim_shop             → ReplacingMergeTree + INSERT; OPTIMIZE FINAL di DAG maintenance
```

### Kenapa bukan ReplacingMergeTree untuk fakta?

`fct_product_snapshot` grained per `(product_id, crawled_at)` — setiap crawl menghasilkan row baru dengan timestamp berbeda. ReplacingMergeTree hanya dedup berdasarkan ORDER BY key, jadi tidak bisa mendeteksi duplikat faktual (row yang sama di-insert dua kali). Truncate-partition-insert lebih sederhana dan deterministik.

### Kenapa truncate-partition, bukan truncate seluruh tabel?

- Partisi = `toYYYYMM(crawled_at)`. Setiap DAG run menghasilkan data untuk bulan berjalan.
- Hapus partisi bulan ini → INSERT data baru. Partisi bulan lain tidak tersentuh.
- Kalau suatu saat butuh backfill bulan lama, tinggal overwrite partisi bulan itu.

### Kenapa ReplacingMergeTree untuk dimensi?

- Dimensi adalah latest-state (`row_number() over partition by product_id order by crawled_at desc = 1`).
- Duplikat pada key yang sama akan otomatis di-merge oleh ReplacingMergeTree berdasarkan `last_seen_at` (yang paling baru yang dipertahankan).
- OPTIMIZE FINAL bisa dijadwalkan (DAG maintenance mingguan) atau query pakai `FINAL` di view.

## Konsekuensi

| | Positif | Negatif |
|---|---|---|
| **Arsitektur** | Transformasi tetap satu tempat (dbt-duckdb), serving layer bebas bertambah | |
| **Operasional** | Pattern konsisten dengan Postgres | Perlu jadwalkan OPTIMIZE FINAL untuk dim tables |
| **BI** | ClickHouse siap query dengan native driver Metabase/Superset | View BI mungkin perlu `FINAL` modifier untuk dim tables |
| **Maintenance** | 1 file Python, 1 dependency | Dependency `clickhouse-connect` harus ditambah ke `pipeline/requirements.txt` |
| **dbt lineage** | | ClickHouse tables tidak muncul di dbt docs — tapi untuk serving layer ini acceptable |

## Alternatif yang tidak dipilih

- **Materialized views di ClickHouse** — ClickHouse bisa query eksternal (DuckDB via ODBC/JDBC), tapi setup kompleks dan performa tidak terprediksi.
- **dbt-clickhouse + DuckDB source** — tidak mungkin karena dbt single-database per profile.
- **ClickHouse-only gold** — hapus DuckDB sepenuhnya. Ini berarti rewrite silver → gold di ClickHouse SQL. Mungkin di masa depan kalau DuckDB jadi bottleneck, tapi untuk skala portfolio ini over-engineered.

## Rencana implementasi

1. **Task 1.5:** Buat `pipeline/load/load_to_clickhouse.py` — mirror dari `load_to_postgres.py`, dengan:
   - Truncate partisi bulan ini di `fct_product_snapshot` sebelum INSERT
   - INSERT biasa ke `dim_product` & `dim_shop` (ReplacingMergeTree handle dedup)
2. **Task 1.6:** Tambah task `load_clickhouse` di DAG setelah `dbt_build`
3. **Task 1.7:** Test `pipeline/tests/test_clickhouse_load.py`
