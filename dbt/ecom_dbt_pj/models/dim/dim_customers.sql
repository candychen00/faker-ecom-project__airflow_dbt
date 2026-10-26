with src_cust as (
    select * from {{ ref('src_customers') }}
)

select
    /* row_number() over (order by customer_id) as customer_unique_id, */
    /*{{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,*/
    customer_id,
    name as full_name,
    SPLIT(name, ' ')[SAFE_OFFSET(0)] AS first_name,
    SPLIT(name, ' ')[SAFE_OFFSET(1)] AS last_name,
    email,
    state,
    signup_date
    
from src_cust