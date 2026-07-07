select
    md5(product_id || '|' || cast(crawled_at as varchar)) as snapshot_id,
    product_id,
    shop_id,
    price_idr,
    discount_pct,
    rating,
    crawled_at
from {{ ref('stg_product_snapshot') }}
