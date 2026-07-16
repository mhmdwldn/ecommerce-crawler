# Metabase Dashboard Strategy & Architecture — Production Guide

**Version:** 2.0  
**Author:** Senior BI Analyst  
**Date:** 2026-07-16  
**Backend:** Postgres Mart (primary) + ClickHouse Analytics (Superset, optional)  
**Star Schema:** `dim_product` ⟵ `fct_product_snapshot` ⟶ `dim_shop` · `dim_category`  

---

## 🛠️ Analisis Arsitektur Proyek & Data Pipeline

### Star Schema Overview

```
                        ┌──────────────────────┐
                        │    dim_category       │
                        │  category_sk (PK)     │
                        │  asset_category       │
                        │  cat_l1/l2/l3_name    │
                        └─────────┬────────────┘
                                  │ 1:N
                                  ▼
┌──────────────────┐   ┌──────────────────────────┐   ┌──────────────────┐
│   dim_product    │   │  fct_product_snapshot     │   │    dim_shop      │
│  product_id (PK) │◄──│  snapshot_id (PK)         │──►│  shop_id (PK)    │
│  product_name    │   │  product_id (FK)          │   │  shop_name       │
│  product_url     │   │  shop_id (FK)             │   │  shop_city       │
│  shop_id (FK)    │   │  category_sk (FK)         │   │  shop_tier       │
│  last_seen_at    │   │  price_idr                │   │  last_seen_at    │
└──────────────────┘   │  discount_pct             │   └──────────────────┘
                       │  rating                   │
                       │  search_keyword           │
                       │  crawled_at               │
                       └──────────────────────────┘
```

**Grain faktual:** Satu baris = satu snapshot harga produk pada satu waktu crawl (`product_id + crawled_at`).  
**Dimensi terkini:** `dim_product`, `dim_shop`, dan `dim_category` menyimpan state terbaru (Type 1 SCD).  
**FK path yang didukung:**
- `fct → dim_product` via `product_id` — harga, diskon, rating per produk
- `fct → dim_shop` via `shop_id` — harga per kota/tier toko
- `fct → dim_category` via `category_sk` — harga per kategori/sub-kategori (Tokopedia breadcrumb 3-level + asset registry)

### Dual-Backend Architecture

| Layer | Postgres Mart | ClickHouse Analytics |
|-------|--------------|---------------------|
| **Engine** | Row-oriented (OLTP) | Column-oriented (OLAP) |
| **BI Tool** | Metabase (`:3000`) | Superset (`:8088`) |
| **Query speed** | <100ms untuk <10K rows | 3–5× lebih cepat untuk agregasi >100K rows |
| **Best for** | Operational dashboards, record-level detail | Time-series trends, heavy aggregations |
| **Category support** | ✅ `dim_category` (77 categories) | ✅ `dim_category` (identical data) |
| **Login** | `admin@tokocrawl.local` / `admin12345` | `admin` / `admin` |

**Kapan pakai yang mana:** Metabase cocok untuk eksplorasi harian dan report pricing summary. Superset lebih cocok untuk trend 30+ hari, window functions, dan visualisasi kompleks. Keduanya membaca dari sumber yang sama (gold DuckDB), jadi data selalu konsisten.

---

## 📊 Rekomendasi Arsitektur Dashboard Metabase

### Dashboard Group A — **Executive Pricing Summary** (Marketing / Product Manager)

**Tujuan:** Memantau harga rata-rata, diskon, dan distribusi harga per kategori dalam 7–30 hari terakhir.  
**Target user:** Product manager, pricing analyst.

| Card | Business Question | Chart Type |
|------|-------------------|------------|
| A1 | Bagaimana tren harga rata-rata 30 hari terakhir? | **Line** — multi-series (avg/min/max) |
| A2 | Kategori mana yang paling fluktuatif harganya? | **Bar** — avg price per `cat_l1_name` + stddev |
| A3 | Berapa % produk yang didiskon hari ini? | **Gauge** — `discount_pct > 0` / total products |
| A4 | Top 10 produk termurah hari ini | **Table** — product name, price, shop, category |

### Dashboard Group B — **Operational Competitor Insights** (Category Manager)

**Tujuan:** Menganalisis lanskap kompetitif per keyword, per toko, per kota.  
**Target user:** Category manager, merchandising.

| Card | Business Question | Chart Type |
|------|-------------------|------------|
| B1 | Toko mana yang paling dominan per keyword? | **Bar (stacked)** — product count per shop, colored by keyword |
| B2 | Bagaimana distribusi harga per kota? | **Map / Bar** — avg price per `shop_city` |
| B3 | Keyword mana yang paling kompetitif (harga terendah)? | **Table** — keyword, min price, shop count |
| B4 | Bagaimana distribusi rating per tier toko? | **Donut** — count by `shop_tier`, grouped by rating buckets |

### Dashboard Group C — **Data Quality & Pipeline Monitor** (Data Engineer)

**Tujuan:** Memonitor kesehatan pipeline, volume data, dan anomali.  
**Target user:** Data engineer, SRE.

| Card | Business Question | Chart Type |
|------|-------------------|------------|
| C1 | Berapa row yang masuk per hari? | **Line** — `count(*)` by `date(crawled_at)` |
| C2 | Apakah ada jam tanpa data (gap)? | **Table** — hours with zero rows |
| C3 | Berapa persen data yang punya kategori valid? | **Gauge** — `cat_l1_name != '(unknown)'` / total |
| C4 | Asset registry: berapa asset aktif vs nonaktif? | **Donut** — from `control.crawl_assets` |

---

## 📐 Pemetaan Metrik & Tipe Visualisasi (Charts Mapping)

| # | Card / Business Question | Source Table(s) | Join Path | Metric | Visual | Rationale |
|---|--------------------------|-----------------|-----------|--------|--------|-----------|
| A1 | Tren harga 30 hari | `fct_product_snapshot` | — | `avg(price_idr)`, `min()`, `max()` per `date(crawled_at)` | **Line (multi-series)** | Time-series = line. 3 series (avg/min/max) memberikan envelope harga. |
| A2 | Fluktuasi per kategori | `fct` + `dim_category` | `category_sk` | `stddev(price_idr)` per `cat_l1_name` | **Bar (horizontal)** | Kategori = categorical. Bar horizontal mudah dibaca untuk label panjang. |
| A3 | % produk didiskon hari ini | `fct` | — | `count(CASE WHEN discount_pct > 0)` / `count(*)` | **Gauge** | Single KPI dengan target 100%. Gauge menunjukkan progres menuju "semua produk terdiskon". |
| A4 | Top 10 termurah | `fct` + `dim_product` + `dim_category` | `product_id`, `category_sk` | `min(price_idr)` per product, sorted ASC, limit 10 | **Table** | Record-level detail butuh row visibility. Table dengan conditional formatting (merah=murah, hijau=mahal). |
| B1 | Dominasi toko per keyword | `fct` + `dim_shop` | `shop_id` | `count(product_id)` per `shop_name`, stacked by `search_keyword` | **Stacked Bar** | Stacking menunjukkan komposisi keyword per toko. |
| B2 | Harga per kota | `fct` + `dim_shop` | `shop_id` | `avg(price_idr)` per `shop_city` | **Bar / Map** | Geografis. Kalau ada lat/long di masa depan, upgrade ke map. |
| B3 | Keyword kompetitif | `fct` + `dim_product` | `product_id` | `min(price_idr)`, `count(DISTINCT shop_id)` per `search_keyword` | **Table (sorted)** | Sorting by min price ASC → keyword termurah di atas. |
| B4 | Rating per tier toko | `fct` + `dim_shop` | `shop_id` | `count(*)` per `shop_tier`, bucketed by `CASE WHEN rating >= 4.5 THEN '4.5+' ...` | **Donut** | Part-to-whole. Donut lebih compact dari pie, menampilkan distribusi. |
| C1 | Volume data harian | `fct` | — | `count(*)` per `date(crawled_at)` | **Line** | Tren volume → deteksi growth/anomali. |
| C2 | Gap detection | `fct` | — | Generate hour series + LEFT JOIN | **Table** | Record-level. Warna merah untuk jam kosong. |
| C3 | Kategori valid | `fct` + `dim_category` | `category_sk` | `count(CASE WHEN cat_l1_name != '(unknown)')` / `count(*)` | **Gauge** | Target 100%. Turun → API schema drift. |
| C4 | Asset health | `control.crawl_assets` (Postgres) | — | `count(*)` grouped by `is_active`, `category` | **Donut** | Part-to-whole. Hijau=aktif, merah=nonaktif (circuit breaker). |

---

## 🎛️ Rancangan Filter & Interaktivitas (Interactive Filters)

### Global Dashboard Filters

Setiap dashboard group dipasangi **Dashboard Filter** yang berlaku untuk semua card:

| Filter | Type | Field | Default | Applies To |
|--------|------|-------|---------|------------|
| **Date Range** | Date Range | `fct.crawled_at` | Last 7 days | Semua card (kecuali C4) |
| **Category (L1)** | Dropdown | `dim_category.cat_l1_name` | All | A2, A4, B3 |
| **Shop Tier** | Dropdown | `dim_shop.shop_tier` | All | B1, B4 |
| **Keyword** | Search | `fct.search_keyword` | None | B1, B3 |

**Setup di Metabase:**
1. Buka dashboard → klik **Pencil icon (Edit)** → **Filter** (tab kiri atas)
2. Tambah **Date Filter** → pilih field `crawled_at` dari `fct_product_snapshot`
3. Untuk setiap card yang ingin difilter, klik card → **Filter** tab → connect ke dashboard filter yang sesuai
4. **"Same filter for all cards"** rule: gunakan field yang sama (`crawled_at`) di semua card agar satu date filter berlaku global

### Cross-Filtering (Click Behavior)

**Setup click behavior antar card:**
1. Edit dashboard → klik card A4 (Top 10 termurah)
2. Klik **Settings gear → Click behavior**
3. Pilih **Update a dashboard filter** → pilih filter `Category (L1)`
4. Mapped column: pilih `dim_category.cat_l1_name`
5. **Result:** User klik produk di card A4 → filter `Category (L1)` ter-update → semua card lain (A2, B3) ikut terfilter ke kategori produk yang diklik

**Custom destination (cross-dashboard):**
1. Di click behavior, pilih **Go to a custom destination**
2. URL pattern: `/dashboard/2-operational?keyword={{search_keyword}}`
3. User klik card di Executive Dashboard → lompat ke Operational Dashboard dengan keyword pre-filtered

---

## 🛠️ Catatan Teknis Pembuatan Kueri (SQL & Metadata Tip)

### 1. Metadata Modeling di Admin Settings (Zero-Code Transform)

Daripada nulis `CASE WHEN shop_tier = 1 THEN 'Official Store' ...` berulang-ulang, manfaatkan **Metabase Data Model**:

**Path:** Admin (gear) → Databases → Postgres Mart → `dim_shop` table → **Column settings**

| Column | Setting | Value |
|--------|---------|-------|
| `shop_tier` | **Type** | `Category` |
| `shop_tier` | **Filter type** | `Dropdown` |
| `shop_tier` | **Remap values** | `1=Official Store, 2=Gold Merchant, 3=Silver, 4=Regular, 5=New` |
| `shop_tier` | **Display as** | `Select box` |

Keuntungan:
- Semua question otomatis menampilkan label "Official Store" tanpa SQL CASE WHEN
- Filter dropdown di dashboard langsung pakai label manusiawi
- Bisa di-override di SQL question tertentu

**Untuk `dim_category`:**
| Column | Setting | Value |
|--------|---------|-------|
| `cat_l1_name` | **Filter type** | `Dropdown` |
| `asset_category` | **Remap values** | `=Tidak ada kategori, elektronik=Elektronik, fashion=Fashion` |

### 2. Native SQL dengan `{{field_filter}}` (Metabase Parameterized Queries)

Metabase support **field filter** untuk bind variable ke dashboard filter tanpa SQL injection risk:

```sql
-- Card A1: Price Trend dengan date filter dari dashboard
SELECT
    date(crawled_at) AS tanggal,
    round(avg(price_idr)) AS avg_price,
    min(price_idr) AS min_price,
    max(price_idr) AS max_price
FROM fct_product_snapshot
WHERE {{crawled_at_filter}}
GROUP BY date(crawled_at)
ORDER BY tanggal
```

**Setup field filter:**
1. Tulis query di **Native SQL editor** (bukan Notebook)
2. Klik **Variables** (ikon `{x}` di sidebar kanan)
3. Pilih **Field Filter** → pilih field `crawled_at` dari `fct_product_snapshot`
4. Metabase akan mengenali sintaks `{{crawled_at_filter}}` sebagai variable
5. **Filter widget type:** `Date Range`

Contoh dengan category filter:
```sql
SELECT
    dc.cat_l1_name,
    dc.cat_l2_name,
    count(DISTINCT fct.product_id) AS products,
    round(avg(fct.price_idr)) AS avg_price
FROM fct_product_snapshot fct
JOIN dim_category dc ON fct.category_sk = dc.category_sk
WHERE {{crawled_at_filter}}
  AND {{category_filter}}
  AND dc.cat_l1_name != '(unknown)'
GROUP BY dc.cat_l1_name, dc.cat_l2_name
ORDER BY products DESC
```

### 3. Optimasi Performa untuk Data Engineer

| Tip | Detail |
|-----|--------|
| **Index kolom join** | `CREATE INDEX idx_fct_product ON fct_product_snapshot(product_id, crawled_at)` — mempercepat JOIN + filter tanggal |
| **Materialize common aggregation** | Kalau query A1 (tren 30 hari) diakses banyak user, buat **Metabase Model** dari SQL query dan schedule refresh |
| **Avoid `SELECT *` di notebook** | Metabase Notebook editor kadang fetch semua kolom untuk preview. Gunakan Native SQL untuk query yang hanya butuh 2-3 kolom |
| **Gunakan ClickHouse untuk heavy lift** | Kalau query >5 detik di Postgres, pindahkan ke Superset/ClickHouse. `fct_product_snapshot` di CH punya ORDER BY `(product_id, crawled_at)` — optimal untuk time-series |
| **Pre-aggregate untuk dashboard summary** | Buat tabel materialized `daily_price_summary` via dbt model untuk query <100ms |
| **Monitor slow queries** | Admin → Tools → **Query log** → filter by `avg_exec_time > 1000` (ms) |

### 4. Rekomendasi Indexing (untuk DBA / Data Engineer)

```sql
-- Postgres: index untuk query pattern umum di dashboard
CREATE INDEX IF NOT EXISTS idx_fct_crawled ON fct_product_snapshot(crawled_at);
CREATE INDEX IF NOT EXISTS idx_fct_product ON fct_product_snapshot(product_id);
CREATE INDEX IF NOT EXISTS idx_fct_shop ON fct_product_snapshot(shop_id);
CREATE INDEX IF NOT EXISTS idx_fct_category ON fct_product_snapshot(category_sk);

-- Composite untuk join + filter date (most common pattern)
CREATE INDEX IF NOT EXISTS idx_fct_product_date ON fct_product_snapshot(product_id, crawled_at);

-- ClickHouse: sudah di-cover oleh ORDER BY + PARTITION BY di DDL
-- dim_product: ReplacingMergeTree ORDER BY (product_id)
-- fct: MergeTree PARTITION BY toYYYYMM(crawled_at) ORDER BY (product_id, crawled_at)
```

### 5. Common Pitfalls & Solutions

| Pitfall | Why | Fix |
|---------|-----|-----|
| **Filter tidak jalan di JOIN card** | Metabase field filter hanya bekerja pada source table pertama | Gunakan Native SQL + `{{field_filter}}` untuk query multi-table |
| **"Question took too long"** | Query timeout default 60s | Native SQL + index + materialized view |
| **Label `shop_tier = 1` di chart** | Data integer yang belum di-remap | Admin → Data Model → `dim_shop.shop_tier` → Remap Values |
| **"Data tidak sinkron setelah DAG run"** | Metabase cache default 1 jam | Admin → Settings → Caching → **Minimum query duration** set ke 0 detik untuk development, atau trigger manual refresh di **Admin → Databases → Sync schema** |
| **`dim_category.cat_l1_name = "(unknown)"` muncul di chart** | Sentinel value dari silver untuk data tanpa breadcrumb | Filter di level dashboard: `cat_l1_name != '(unknown)'` |

---

## Appendix: Metabase vs Superset Quick Reference

| | Metabase | Superset |
|---|---|---|
| **Port** | `:3000` | `:8088` |
| **Backend** | Postgres Mart | ClickHouse Analytics |
| **Setup complexity** | ✅ 1 form, langsung pakai | Butuh script init, 3 CLI commands |
| **Best for** | Business users, quick exploration | Data teams, complex dashboards |
| **Chart types** | 16 (line, bar, pie, gauge, table, map, funnel, ...) | 40+ (all Metabase types + heatmap, treemap, deck.gl) |
| **SQL editor** | Notebook (GUI) + Native SQL | SQL Lab (powerful autocomplete) |
| **Filter interactivity** | ✅ Click behavior, cross-dashboard | ✅ Cross-filter + drill-down |
| **Alerting** | ✅ Dashboard subscription email | Via Celery + webhook |
| **Category dim** | ✅ Full support | ✅ Full support |
| **Login** | `admin@tokocrawl.local` / `admin12345` | `admin` / `admin` |
| **Verdict** | Daily driver, 90% use cases covered | Power tool, complex analytics |
