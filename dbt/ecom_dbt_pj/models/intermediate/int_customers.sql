
{{ 
    config(
        tags= ["cust"]
    ) 
}}

with source as (
    select * from {{ ref('src_customers') }}
),

partition_sk as (
    select
        *,
        row_number() over (
            partition by customer_sk
            order by customer_sk desc
        ) as rn
    from source
),

deduped as (

    select * except (rn)
    from partition_sk
    where rn = 1
)

select *
from deduped
