
{{ 
    config(
        tags= ["prod"]
    ) 
}}

with source as (
    select * from {{ source('ecom_raw', 'products') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['fruit', 'category', 'product_id', 'price']) }} as product_sk,
    *
from source
order by product_sk
