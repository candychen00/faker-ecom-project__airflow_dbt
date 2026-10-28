
{{ 
    config(
        tags= ["ord"]
    ) 
}}

with source as (
    select * from {{ ref('src_orders') }}
),

partition_sk as (
    select
        *,
        row_number() over (
            partition by order_sk
            order by order_sk desc
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
