
{{ 
    config(
        tags= ["prod"]
    ) 
}}


with source as (
    select * from {{ ref("int_products") }}
)

select
    product_sk,
    product_id,
    CONCAT(fruit, ' ', category) as product,
    category as prod_category,
    fruit as prod_fruit,
    price as unit_price,
    updated_at,
    created_at

from source