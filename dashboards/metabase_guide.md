# Metabase Dashboard Guide — Step-by-Step

**Prasyarat:** Metabase sudah nyala di `http://localhost:3000`. Login: `admin@tokocrawl.local` / `admin12345`. Database "Postgres Mart" sudah terkoneksi dan sudah di-sync.

---

## Persiapan: Cek Tabel

1. Buka `http://localhost:3000`
2. Klik **Browse** (ikon kotak di sidebar)
3. Di dropdown "Database", pilih **Postgres Mart**
4. Pastikan 3 tabel muncul: `dim_product`, `dim_shop`, `fct_product_snapshot`

Kalau belum muncul, ke **Admin (gear icon) → Databases → Postgres Mart → Sync database schema now**.

---

## Dashboard 1 — Rata-rata Harga per Kota (Bar Chart)

### 1a. Bikin Question Baru

1. Klik **New** (tombol biru kanan atas) → **Question**
2. Pilih **Postgres Mart** sebagai database
3. Di tab **Notebook Editor**, klik **+ Join Data**
4. Pilih tabel **fct_product_snapshot**
5. Klik **Join** → pilih **dim_shop**
6. Di join condition: pilih `shop_id = shop_id` (Field: `Shop ID`, Foreign Field: `Shop ID`)
7. Klik **Join**

### 1b. Atur Metrik & Grouping

1. Di section **Summarize**:
   - Klik **Pick the metric** → pilih **Average of...** → pilih `fct_product_snapshot.price_idr`
   - Label: `Rata-rata Harga`
2. Klik **Group by** → pilih `dShop.City`
3. Hasil query langsung muncul di bawah

### 1c. Visualisasi

1. Klik tombol **Visualization** (ikon chart, kiri bawah)
2. Pilih **Bar** chart
3. Di settings sidebar:
   - **X-axis:** `City`
   - **Y-axis:** `Rata-rata Harga`
   - **Sort:** Y-axis descending
4. Klik **Save** → beri nama `Avg Price by City` → simpan

---

## Dashboard 2 — Tren Harga Harian (Line Chart)

### 2a. Bikin Question

1. Klik **New → Question**
2. Pilih **Postgres Mart** → tabel **fct_product_snapshot**
3. Di **Summarize**, tambahin 3 metrik:
   - Klik **Pick the metric** → **Average of...** → `price_idr` → label `Avg Price`
   - Klik **+** (tambah metrik) → **Minimum of...** → `price_idr` → label `Min Price`
   - Klik **+** (tambah metrik) → **Maximum of...** → `price_idr` → label `Max Price`
4. Klik **Group by** → pilih `crawled_at`

### 2b. Bikin jadi Harian (bukan per detik)

1. Klik pada field `crawled_at` di Group by
2. Di dropdown **Binning**, pilih **Day**
3. Data sekarang group per hari

### 2c. Filter 30 Hari Terakhir

1. Klik tombol **Filter** (ikon corong)
2. Pilih field `crawled_at`
3. Pilih filter type **Relative Dates** → **Past 30 Days**

### 2d. Visualisasi

1. Klik **Visualization** → pilih **Line** chart
2. Settings:
   - **X-axis:** `crawled_at: Day`
   - **Y-axis:** `Avg Price`, `Min Price`, `Max Price`
3. Klik **Save** → `Price Trend (30 Days)`

---

## Dashboard 3 — Top Produk dengan Diskon (Table)

### 3a. Bikin Question

1. Klik **New → Question** → **Postgres Mart**
2. Pilih tabel **fct_product_snapshot**
3. Klik **+ Join Data** → pilih **dim_product** → join `product_id = product_id`

### 3b. Atur Filter & Sort

1. Klik **Filter** → `fct_product_snapshot.discount_pct`
2. Pilih **Greater than** → isi `0`
3. Di section **Sort**, klik **Pick a column to sort by** → `fct_product_snapshot.price_idr`
4. Arah sort: **Ascending** (termurah dulu)

### 3c. Pilih Kolom Tampilan

1. Di section **View**, pilih kolom yang mau ditampilkan:
   - `Product Name`
   - `price_idr` (ganti label jadi "Harga")
   - `discount_pct` (ganti label jadi "Diskon %")
2. Klik **+** untuk nambah kolom lain kalau perlu

### 3d. Visualisasi

1. Klik **Visualization** → pilih **Table**
2. Settings: centang **Show row numbers**
3. Klik **Save** → `Top Discounted Products`

---

## Dashboard 4 — Distribusi Rating (Histogram / Bar)

### 4a. Bikin Question

1. Klik **New → Question** → **Postgres Mart** → **fct_product_snapshot**
2. Di **Summarize**, pilih **Count**
3. Klik **Group by** → pilih `rating`

### 4b. Binning Rating

1. Klik pada field `rating` di Group by
2. Di dropdown **Binning**, pilih **1 bin width**
3. Rating 4.5 dan 5.0 akan dikelompokkan ke 4 dan 5

### 4c. Visualisasi

1. Klik **Visualization** → pilih **Bar**
2. Settings:
   - **X-axis:** `rating` (1 sampai 5)
   - **Y-axis:** `Count`
3. Klik **Save** → `Rating Distribution`

---

## Dashboard 5 — Proporsi Shop Tier (Pie/Donut Chart)

### 5a. Bikin Question

1. Klik **New → Question** → **Postgres Mart** → **dim_shop**
2. Di **Summarize**, pilih **Count**
3. Klik **Group by** → pilih `shop_tier`

### 5b. Label Tier yang Kebaca

Di Metabase, nilai `shop_tier` angka (1, 2, 3, dst). Kita bisa kasih label custom:

1. Klik **Custom Column** (ikon sigma/Σ)
2. Custom expression:
```sql
CASE
  WHEN shop_tier = 1 THEN 'Official Store'
  WHEN shop_tier = 2 THEN 'Gold Merchant'
  WHEN shop_tier = 3 THEN 'Silver Merchant'
  ELSE 'Regular (' || shop_tier || ')'
END
```
3. Beri nama `Tier Label`
4. Di **Group by**, ganti `shop_tier` dengan `Tier Label`

### 5c. Visualisasi

1. Klik **Visualization** → pilih **Pie** (atau **Donut**)
2. Settings:
   - **Category:** `Tier Label`
   - **Measure:** `Count`
3. Centang **Show percentages**
4. Klik **Save** → `Shop Tier Distribution`

---

## Bonus — Gabung Semua Jadi Satu Dashboard

1. Klik **New** → **Dashboard**
2. Beri nama: `Tokopedia Analytics - Fase 3`
3. Klik **+** (add card) → pilih 5 question yang udah dibuat:
   - Price Trend (30 Days) — Line chart
   - Avg Price by City — Bar chart
   - Top Discounted Products — Table
   - Rating Distribution — Bar chart
   - Shop Tier Distribution — Pie chart
4. Drag & drop buat atur layout
5. Klik **Save**

---

## Tips Cepat

| Masalah | Solusi |
|---|---|
| Data gak muncul | Pastiin database udah di-sync: Admin → Databases → Postgres Mart → Sync now |
| Chart kosong | Cek filter — mungkin terlalu sempit. Hapus filter, lihat apakah ada data. |
| Tipe chart salah | Klik Visualization, pilih tipe lain — Metabase auto-detect yang kompatibel |
| Join error | Pastiin nama kolom join sama (`shop_id = shop_id`, `product_id = product_id`) |
| Mau ganti warna | Visualization settings → Colors |
