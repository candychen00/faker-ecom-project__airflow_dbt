with src_cust as (
    select * from {{ ref('src_customers') }}
)

select * 
from src_cust