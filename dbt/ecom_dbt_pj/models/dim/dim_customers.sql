
{{ 
    config(
        tags= ["cust"]
    ) 
}}

with source as (
    select * from {{ ref('int_customers') }}
)

select
    customer_sk,
    customer_id,
    name as full_name,
    SPLIT(name, ' ')[SAFE_OFFSET(0)] AS first_name,
    SPLIT(name, ' ')[SAFE_OFFSET(1)] AS last_name,
    email,
    state,
    signup_date,
    updated_at,
    created_at
    
from source