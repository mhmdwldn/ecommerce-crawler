-- dim_shop — latest shop attributes per shop_id
-- ponytail: ReplacingMergeTree for idempotent re-runs.

CREATE TABLE IF NOT EXISTS analytics.dim_shop (
    shop_id         String,
    shop_name       String,
    shop_city       String,
    shop_tier       Int32,
    last_seen_at    DateTime
)
ENGINE = ReplacingMergeTree(last_seen_at)
PARTITION BY toYYYYMM(last_seen_at)
ORDER BY (shop_id)
SETTINGS index_granularity = 8192;
