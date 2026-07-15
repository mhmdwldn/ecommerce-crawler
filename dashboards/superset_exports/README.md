# Superset Dashboard Exports

## Setup
```bash
# Superset is at http://localhost:8088 (admin / admin)
# Run setup script to create ClickHouse connection:
docker exec superset python /opt/airflow/repo/dashboards/setup_superset.py
```

## Exporting (from UI)
- Dashboard → Edit → ... → Download as ZIP
- Or use API: `GET /api/v1/dashboard/export/`

## Dashboards (same as Metabase, but all queries run on ClickHouse)

### 1. US-1: Price Trend 30 Hari
- Dataset: fct_product_snapshot + dim_product
- Visualization: Line chart
- SQL: `dashboards/dashboards.sql` section "US-1 (ClickHouse)"

### 2. US-2: Top Price Drops Today
- Dataset: fct_product_snapshot + dim_product
- Visualization: Table
- SQL: `dashboards/dashboards.sql` section "US-2 (ClickHouse)"

### 3. US-3: Perbandingan Shop/Kota
- Dataset: fct_product_snapshot + dim_shop
- Visualization: Bar chart
- SQL: `dashboards/dashboards.sql` section "US-3 (ClickHouse)"

### 4. Pipeline Health (US-6)
- Dataset: pipeline_runs
- Visualization: Time series + stat tiles
- SQL: `dashboards/dashboards.sql` section "US-6"

### 5. Asset Health (FR-20)
- Dataset: control.crawl_assets (Postgres)
- Visualization: Summary table
- SQL: `dashboards/dashboards.sql` section "FR-20"
