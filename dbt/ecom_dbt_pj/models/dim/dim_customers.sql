with src_cust as (
    select * from {{ ref('src_customers') }}
)

select
    customer_sk,
    customer_id,
    name as full_name,
    SPLIT(name, ' ')[SAFE_OFFSET(0)] AS first_name,
    SPLIT(name, ' ')[SAFE_OFFSET(1)] AS last_name,
    email,
    state,
    signup_date
    
from src_cust