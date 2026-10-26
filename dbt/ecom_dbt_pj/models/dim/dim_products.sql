
with raw_prod as (
    select * from {{ ref("src_products") }}
)

select
    product_sk,
    product_id,
    CONCAT(fruit, ' ', category) as product,
    category as prod_category,
    fruit as prod_fruit,
    cast(price as numeric) as unit_price

from raw_prod