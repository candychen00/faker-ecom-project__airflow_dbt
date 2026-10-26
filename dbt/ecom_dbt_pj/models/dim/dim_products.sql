
with raw_prod as (
    select * from {{ ref("src_products") }}
)

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as customer_key,
    product_id,
    CONCAT(fruit, ' ', category) as product,
    category as prod_category,
    fruit as prod_fruit,
    price as unit_price

from raw_prod