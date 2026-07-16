select
    md5(product_id || '|' || cast(crawled_at as varchar)) as snapshot_id,
    product_id,
    shop_id,
    category_sk,
    price_idr,
    discount_pct,
    rating,
    search_keyword,
    crawled_at
from {{ ref('stg_product_snapshot') }}
