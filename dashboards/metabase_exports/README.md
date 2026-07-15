# Metabase Dashboard Exports

## Exporting (from UI)
- Download individual dashboard JSON from: Admin → Export → Dashboard
- Or use API: `GET /api/dashboard/{id}` with `X-Metabase-Session` header

## Dashboards

### 1. US-1: Price Trend 30 Hari
- Question: Line chart — avg_price_idr per day from fct_product_snapshot
- SQL: `dashboards/dashboards.sql` section "US-1"
- Chart type: Line chart, x=tanggal, y=avg_price_idr

### 2. US-2: Top Price Drops Today
- Question: Table — cheapest products crawled today
- SQL: `dashboards/dashboards.sql` section "US-2"
- Chart type: Table or Bar chart, sorted by price_idr ASC

### 3. US-3: Perbandingan Shop/Kota
- Question: Bar chart — product count per city, avg price
- SQL: `dashboards/dashboards.sql` section "US-3"
- Chart type: Bar chart, x=kota, y=jumlah_produk

### 4. Pipeline Health (US-6)
- Question: Time series — pipeline runs over time
- SQL: `dashboards/dashboards.sql` section "US-6"
- Source: ClickHouse analytics.pipeline_runs

### 5. Asset Health (FR-20)
- Question: Summary table — assets by category
- SQL: `dashboards/dashboards.sql` section "FR-20"
- Source: Postgres control.crawl_assets
