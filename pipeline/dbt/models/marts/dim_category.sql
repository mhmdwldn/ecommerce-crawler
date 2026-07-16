select distinct
    category_sk,
    coalesce(asset_category, '') as asset_category,
    coalesce(cat_l1_name, '') as cat_l1_name,
    coalesce(l1_id, '') as l1_id,
    coalesce(cat_l2_name, '') as cat_l2_name,
    coalesce(l2_id, '') as l2_id,
    coalesce(cat_l3_name, '') as cat_l3_name,
    coalesce(l3_id, '') as l3_id
from {{ ref('stg_product_snapshot') }}
where category_sk is not null and category_sk != ''
