select
    product_id,
    product_name,
    product_url,
    rating,
    price_idr,
    discount_pct,
    shop_id,
    shop_name,
    shop_city,
    shop_tier,
    crawled_at
from delta_scan('s3://lakehouse/silver/products')
