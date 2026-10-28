
{{ 
    config(
        tags= ["ord"]
    ) 
}}

with source as (
    select * from {{ source('ecom_raw', 'orders' ) }}
)

select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'order_at', 'customer_id', 'product_id', 'quantity', 'created_at']) }} as order_sk,
    *

from source
order by order_sk
