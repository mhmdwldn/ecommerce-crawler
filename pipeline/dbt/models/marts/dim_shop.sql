select
    shop_id,
    shop_name,
    shop_city,
    shop_tier,
    crawled_at as last_seen_at
from {{ ref('stg_product_snapshot') }}
where shop_id is not null and shop_id <> ''
qualify row_number() over (partition by shop_id order by crawled_at desc) = 1
