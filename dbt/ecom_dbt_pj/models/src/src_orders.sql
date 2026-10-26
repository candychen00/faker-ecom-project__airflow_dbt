
WITH
dedup as (
    select distinct *
    from {{ source('ecom_raw', 'orders') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_sk,
    *
FROM dedup