-- dim_category — composite category dimension (Tokopedia breadcrumb + asset registry category)
-- Surrogate key: category_sk = md5(l1_id|l2_id|l3_id|asset_category)
-- ReplacingMergeTree: rerun-safe — OPTIMIZE FINAL deduplicates.

CREATE TABLE IF NOT EXISTS analytics.dim_category (
    category_sk    String,
    asset_category String,
    cat_l1_name    String,
    l1_id          String,
    cat_l2_name    String,
    l2_id          String,
    cat_l3_name    String,
    l3_id          String
)
ENGINE = ReplacingMergeTree
ORDER BY (category_sk)
SETTINGS index_granularity = 8192;
