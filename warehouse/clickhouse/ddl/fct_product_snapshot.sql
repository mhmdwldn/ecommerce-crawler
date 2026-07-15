-- fct_product_snapshot — one row per product per crawl
-- ponytail: MergeTree, partition by month, ordered by product+time for BI queries.
-- Idempotency: INSERT-only; ADR-001 may switch to truncate-partition-insert.

CREATE TABLE IF NOT EXISTS analytics.fct_product_snapshot (
    snapshot_id     String,
    product_id      String,
    shop_id         String,
    price_idr       Int64,
    discount_pct    Int32,
    rating          Float64,
    crawled_at      DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(crawled_at)
ORDER BY (product_id, crawled_at)
SETTINGS index_granularity = 8192;
