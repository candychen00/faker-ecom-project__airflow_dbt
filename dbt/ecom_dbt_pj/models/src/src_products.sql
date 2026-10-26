
WITH
dedup as (
    select distinct *
    from {{ source('ecom_raw', 'products') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
    *
FROM dedup
