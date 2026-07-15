select
    product_id,
    product_name,
    product_url,
    shop_id,
    crawled_at as last_seen_at
from {{ ref('stg_product_snapshot') }}
where product_id is not null
qualify row_number() over (partition by product_id order by crawled_at desc) = 1
