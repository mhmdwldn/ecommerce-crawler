-- ============================================================================
-- Fase 3 — Dashboard SQL Queries (Metabase + Superset)
-- ============================================================================
-- Copy-paste ke SQL query editor di Metabase atau Superset.
-- Metabase: connect to Postgres (host=postgres, port=5432, db=mart, user=mart)
-- Superset: connect to ClickHouse (host=clickhouse, port=8123, db=analytics)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- US-1: Price Trend 30 Hari — rata-rata harga per hari
-- ----------------------------------------------------------------------------
-- Postgres:
SELECT
    date(crawled_at) AS tanggal,
    round(avg(price_idr)) AS avg_price_idr,
    min(price_idr) AS min_price_idr,
    max(price_idr) AS max_price_idr
FROM fct_product_snapshot
WHERE crawled_at >= now() - interval '30 days'
GROUP BY date(crawled_at)
ORDER BY tanggal;

-- ClickHouse:
SELECT
    toDate(crawled_at) AS tanggal,
    round(avg(price_idr)) AS avg_price_idr,
    min(price_idr) AS min_price_idr,
    max(price_idr) AS max_price_idr
FROM analytics.fct_product_snapshot
WHERE crawled_at >= now() - interval 30 day
GROUP BY tanggal
ORDER BY tanggal;

-- ----------------------------------------------------------------------------
-- US-2: Top Price Drops Today — diskon terbesar hari ini
-- ----------------------------------------------------------------------------
-- Postgres:
WITH today AS (
    SELECT
        product_id,
        price_idr,
        crawled_at,
        row_number() OVER (PARTITION BY product_id ORDER BY crawled_at DESC) AS rn
    FROM fct_product_snapshot
    WHERE date(crawled_at) = current_date
),
latest AS (
    SELECT dp.product_name, dp.product_url, ts.price_idr, ts.crawled_at
    FROM today ts
    JOIN dim_product dp ON ts.product_id = dp.product_id
    WHERE ts.rn = 1
)
SELECT product_name, price_idr, crawled_at
FROM latest
ORDER BY price_idr ASC
LIMIT 20;

-- ClickHouse:
SELECT
    dp.product_name,
    f.price_idr,
    f.crawled_at
FROM analytics.fct_product_snapshot f
JOIN analytics.dim_product dp ON f.product_id = dp.product_id
WHERE toDate(f.crawled_at) = today()
ORDER BY f.price_idr ASC
LIMIT 20;

-- ----------------------------------------------------------------------------
-- US-3: Perbandingan Shop/Kota — jumlah produk + rata-rata harga per kota
-- ----------------------------------------------------------------------------
-- Postgres:
SELECT
    ds.shop_city AS kota,
    count(DISTINCT f.product_id) AS jumlah_produk,
    count(*) AS jumlah_snapshot,
    round(avg(f.price_idr)) AS avg_price_idr
FROM fct_product_snapshot f
JOIN dim_shop ds ON f.shop_id = ds.shop_id
WHERE f.crawled_at >= now() - interval '7 days'
GROUP BY ds.shop_city
ORDER BY jumlah_produk DESC;

-- ClickHouse:
SELECT
    ds.shop_city AS kota,
    uniqExact(f.product_id) AS jumlah_produk,
    count() AS jumlah_snapshot,
    round(avg(f.price_idr)) AS avg_price_idr
FROM analytics.fct_product_snapshot f
JOIN analytics.dim_shop ds ON f.shop_id = ds.shop_id
WHERE f.crawled_at >= now() - interval 7 day
GROUP BY kota
ORDER BY jumlah_produk DESC;

-- ----------------------------------------------------------------------------
-- US-6: Pipeline Health — rows per run, rejects, durasi, status (pipeline_runs)
-- ----------------------------------------------------------------------------
-- Postgres via DuckDB or ClickHouse:
SELECT
    execution_date,
    status,
    rows_silver,
    rows_rejects,
    rows_gold,
    round(duration_sec, 1) AS duration_sec
FROM analytics.pipeline_runs
ORDER BY execution_date DESC
LIMIT 50;

-- ----------------------------------------------------------------------------
-- FR-20: Asset Health — active vs nonaktif, last_crawled_at, failure rate
-- ----------------------------------------------------------------------------
-- Postgres (langsung ke control.crawl_assets):
SELECT
    category,
    count(*) AS total,
    count(*) FILTER (WHERE is_active) AS active,
    count(*) FILTER (WHERE NOT is_active) AS nonaktif,
    count(*) FILTER (WHERE last_status = 'success') AS success_count,
    count(*) FILTER (WHERE last_status = 'failed') AS failed_count,
    round(avg(consecutive_failures), 1) AS avg_failures,
    max(last_crawled_at) AS last_crawl
FROM control.crawl_assets
GROUP BY category
ORDER BY total DESC;

-- ----------------------------------------------------------------------------
-- Bonus: Product count by category (from asset registry payloads)
-- ----------------------------------------------------------------------------
SELECT
    payload->>'keyword' AS keyword,
    label,
    category,
    priority,
    last_crawled_at,
    last_status,
    consecutive_failures
FROM control.crawl_assets
WHERE is_active
ORDER BY priority ASC, last_crawled_at ASC NULLS FIRST;
