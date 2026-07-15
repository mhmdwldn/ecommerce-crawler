-- dim_product — latest product attributes per product_id
-- ponytail: ReplacingMergeTree so rerun doesn't duplicate;
-- OPTIMIZE FINAL deduplicates, or use FINAL in queries.
-- ADR-001 may switch to truncate-partition-insert instead.

CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_id      String,
    product_name    String,
    product_url     String,
    shop_id         String,
    last_seen_at    DateTime
)
ENGINE = ReplacingMergeTree(last_seen_at)
PARTITION BY toYYYYMM(last_seen_at)
ORDER BY (product_id)
SETTINGS index_granularity = 8192;
