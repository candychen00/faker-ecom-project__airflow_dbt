
{{ 
    config(
        tags= ["prod"]
    ) 
}}

with source as (
    select * from {{ ref('src_products') }}
),

partition_sk as (
    select
        *,
        row_number() over (
            partition by product_sk
            order by product_sk desc
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
