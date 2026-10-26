
WITH
dedup as (
    select distinct * 
    from {{ source('ecom_raw', 'customers') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
    *
FROM dedup

