
{{ 
    config(
        tags= ["cust"]
    ) 
}}

with source as (
    select * from {{ source('ecom_raw', 'customers' ) }}
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'name', 'email', 'state', 'signup_date']) }} as customer_sk,
    *
from source
order by customer_sk

