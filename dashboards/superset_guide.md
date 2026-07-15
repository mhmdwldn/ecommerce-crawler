# Superset Dashboard Guide — Step-by-Step

**Prasyarat:** Superset nyala di `http://localhost:8088`. Login: `admin` / `admin`. Database "ClickHouse Analytics" udah konek, dataset udah di-sync (kolom muncul di Edit Dataset).

---

## Persiapan: Cek Dataset

1. Buka `http://localhost:8088`
2. Hover ke **Data** (top menu) → klik **Datasets**
3. Pastiin 4 tabel muncul: `fct_product_snapshot`, `dim_product`, `dim_shop`, `pipeline_runs`
4. Klik salah satu dataset → pastiin kolom-kolomnya keliatan. Kalau kosong, klik **Sync columns from source**.

Kalau dataset belum ada:
- **Data → Databases → ClickHouse Analytics → ⋮ (three dots) → Sync all tables**

---

## Dashboard 1 — Rata-rata Harga per Kota (Bar Chart)

### 1a. Bikin Chart Baru

1. Klik **+** (tombol kanan atas) → **Chart**
2. Pilih dataset **fct_product_snapshot**
3. Di tab **Data** (kiri), atur:
   - **Metrics:** klik `price_idr` → pilih **AVG** (average)
   - Label metric: `Rata-rata Harga`
4. Di section **Filters**, klik **+ Add filter**:
   - Pilih kolom `crawled_at`
   - Operator: **≥**
   - Value: cek **Relative** → **30 days ago**

### 1b. Join ke dim_shop (biar dapet nama kota)

1. Di sebelah kiri, klik dataset **fct_product_snapshot** lagi
2. Scroll ke section **Advanced** → **Custom SQL** (atau kita pakai cara gampang:)
3. **Cara gampang:** di tab **Data**, kita group aja pake shop_id dulu, nanti di chart kasih label manual.

Atau pakai SQL Lab:

1. Klik **SQL** (top menu) → **SQL Lab**
2. Pilih database **ClickHouse Analytics**
3. Ketik query:
```sql
SELECT
    ds.shop_city AS kota,
    round(avg(f.price_idr)) AS avg_price
FROM analytics.fct_product_snapshot f
JOIN analytics.dim_shop ds ON f.shop_id = ds.shop_id
WHERE f.crawled_at >= now() - interval 30 day
GROUP BY ds.shop_city
ORDER BY avg_price DESC
```
4. Klik **Run** (Ctrl+Enter)
5. Di hasil query, klik tombol **Create Chart** → pilih dataset (bikin baru atau pakai existing)
6. Pilih chart type **Bar Chart**
7. Settings:
   - **X-Axis:** `kota`
   - **Y-Axis:** `avg_price`
   - **Sort by:** `avg_price` descending
8. Klik **Create** atau **Save as** → beri nama `Avg Price by City`

---

## Dashboard 2 — Tren Harga 30 Hari (Line Chart)

### 2a. SQL Lab Query

1. Klik **SQL → SQL Lab** → database **ClickHouse Analytics**
2. Query:
```sql
SELECT
    toDate(crawled_at) AS tanggal,
    round(avg(price_idr)) AS avg_price,
    min(price_idr) AS min_price,
    max(price_idr) AS max_price
FROM analytics.fct_product_snapshot
WHERE crawled_at >= now() - interval 30 day
GROUP BY tanggal
ORDER BY tanggal
```
3. **Run**

### 2b. Bikin Chart

1. Klik **Create Chart** dari hasil query
2. Pilih chart type **Time-series Line Chart**
3. Settings:
   - **Time Column:** `tanggal`
   - **Metrics:** `avg_price`, `min_price`, `max_price`
   - **X-Axis Label:** `Tanggal`
   - **Y-Axis Label:** `Harga (IDR)`
4. Di tab **Customize**:
   - **Y-Axis Format:** `,d` (pakai pemisah ribuan)
   - **Legend:** Aktifkan
5. Klik **Save** → `Price Trend (30 Days)`

---

## Dashboard 3 — Top Produk Termurah Hari Ini (Table)

### 3a. SQL Lab Query

1. **SQL Lab → ClickHouse Analytics**
2. Query:
```sql
SELECT
    dp.product_name,
    f.price_idr AS harga,
    f.discount_pct AS diskon_persen,
    f.rating,
    f.crawled_at
FROM analytics.fct_product_snapshot f
JOIN analytics.dim_product dp ON f.product_id = dp.product_id
WHERE toDate(f.crawled_at) = today()
ORDER BY f.price_idr ASC
LIMIT 20
```
3. **Run**

### 3b. Bikin Chart

1. Klik **Create Chart** → chart type **Table**
2. Settings:
   - **Columns:** pilih semua yang muncul
   - **Page length:** `20`
   - **Server pagination:** Aktifkan
3. Di tab **Customize**:
   - Kolom `harga` → number format `,d`
   - Kolom `diskon_persen` → akhiran `%`
4. **Save** → `Top Products Today`

---

## Dashboard 4 — Distribusi Rating (Histogram)

### 4a. SQL Lab Query

1. **SQL Lab → ClickHouse Analytics**
2. Query:
```sql
SELECT
    round(rating) AS rating_rounded,
    count() AS jumlah_produk
FROM analytics.fct_product_snapshot
WHERE crawled_at >= now() - interval 30 day
GROUP BY rating_rounded
ORDER BY rating_rounded
```
3. **Run**

### 4b. Bikin Chart

1. Klik **Create Chart** → chart type **Bar Chart**
2. Settings:
   - **X-Axis:** `rating_rounded`
   - **Y-Axis:** `jumlah_produk`
   - **X-Axis Label:** `Rating (1-5)`
   - **Y-Axis Label:** `Jumlah Produk`
3. Di tab **Customize**:
   - **Bar values:** Aktifkan (nunjukin angka di atas bar)
4. **Save** → `Rating Distribution`

---

## Dashboard 5 — Proporsi Shop Tier (Pie Chart)

### 5a. SQL Lab Query

1. **SQL Lab → ClickHouse Analytics**
2. Query:
```sql
SELECT
    CASE
        WHEN shop_tier = 1 THEN 'Official Store'
        WHEN shop_tier = 2 THEN 'Gold Merchant'
        WHEN shop_tier = 3 THEN 'Silver Merchant'
        ELSE 'Regular'
    END AS tier_label,
    count() AS jumlah
FROM analytics.dim_shop
GROUP BY shop_tier
ORDER BY shop_tier
```
3. **Run**

### 5b. Bikin Chart

1. Klik **Create Chart** → chart type **Pie Chart**
2. Settings:
   - **Category:** `tier_label`
   - **Metric:** `jumlah`
   - **Show legend:** Aktifkan
   - **Show labels:** Aktifkan → pilih **Category + Percent**
3. **Save** → `Shop Tier Distribution`

---

## Bonus 1 — Pipeline Health Dashboard (Time Series + Stat Tiles)

### 6a. Main Chart: Rows Over Time

1. **SQL Lab → ClickHouse Analytics**
2. Query:
```sql
SELECT
    execution_date,
    rows_silver,
    rows_gold,
    duration_sec
FROM analytics.pipeline_runs
ORDER BY execution_date DESC
LIMIT 50
```
3. **Create Chart → Time-series Line Chart**
4. Metrics: `rows_silver`, `rows_gold`
5. **Save** → `Pipeline Health - Rows`

### 6b. Stat Tiles (Big Numbers)

1. Buat chart baru → **Big Number**
2. Query:
```sql
SELECT count() FROM analytics.pipeline_runs WHERE status = 'failed'
```
3. Label: `Failed Runs`
4. **Save** → `Pipeline - Failed`

---

## Bonus 2 — Gabung Semua Jadi Dashboard

1. Klik **+** → **Dashboard**
2. Beri nama: `Tokopedia Analytics`
3. Di editor dashboard, drag-drop components:
   - **Row 1:** `Pipeline - Failed` (Big Number) + `Pipeline Health - Rows` (Line)
   - **Row 2:** `Price Trend (30 Days)` (Line) — full width
   - **Row 3:** `Avg Price by City` (Bar) + `Shop Tier Distribution` (Pie)
   - **Row 4:** `Top Products Today` (Table) — full width
   - **Row 5:** `Rating Distribution` (Bar)
4. Tambah **Divider** antar section (drag dari component panel)
5. Klik **Save**

---

## Tips Cepat Superset

| Masalah | Solusi |
|---|---|
| Dataset kosong / kolom gak muncul | Data → Datasets → klik dataset → **Sync columns from source** |
| SQL Lab error "no such table" | Pastiin pilih database **ClickHouse Analytics** di dropdown kiri atas |
| Chart gak muncul / error load | Cek filter — mungkin terlalu ketat. Hapus filter, lihat ada data gak |
| Join antar tabel gak bisa | Di Explore UI, join harus dari dataset yang sama. Pakai **SQL Lab** buat query join — lebih fleksibel |
| Mau export chart | Di dashboard → klik ⋮ di chart → **Download as...** (CSV, JSON, PNG, PDF) |
| Format angka (Rupiah) | Di Customize tab → **Number Format** = `,d` (pemisah ribuan) |
| Ganti warna chart | Di Customize tab → **Color Scheme** |

---

## Perbedaan Superset vs Metabase

| Aspek | Superset | Metabase |
|---|---|---|
| Bikin query | **SQL Lab** — query manual full control | **Notebook** — klik-klik builder |
| Chart types | 50+ (deck.gl, ECharts) | 20+ |
| Join | Harus lewat SQL Lab atau dataset pre-joined | Bisa join langsung di UI |
| Cocok buat | Power user, data analyst | Pemula, stakeholder |
| Learning curve | Lebih curam | Lebih landai |
| Dataset setup | Manual sync per table | Auto-detect dari database |
