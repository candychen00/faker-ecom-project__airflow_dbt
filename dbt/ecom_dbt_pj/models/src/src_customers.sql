
WITH
dedup as (
    select distinct * 
    from {{ source('ecom_raw', 'customers') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'name', 'email', 'state', 'signup_date']) }} as customer_sk,
    *
FROM dedup

