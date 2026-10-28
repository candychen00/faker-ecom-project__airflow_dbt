
{{ 
    config(
        tags= ["data_quality"]
    ) 
}}

{% set checks = [
    {'name': 'raw_orders',    'rel': source('ecom_raw', 'orders'),    'key': 'order_id'},
    {'name': 'int_orders',         'rel': ref('int_orders'),               'key': 'order_id'},
    {'name': 'fct_orders',         'rel': ref('fct_orders'),               'key': 'order_id'},
    {'name': 'raw_products',  'rel': source('ecom_raw', 'products'),  'key': 'product_id'},
    {'name': 'int_products',       'rel': ref('int_products'),             'key': 'product_id'},
    {'name': 'dim_products',       'rel': ref('dim_products'),             'key': 'product_id'},
    {'name': 'raw_customers', 'rel': source('ecom_raw', 'customers'), 'key': 'customer_id'},
    {'name': 'int_customers',      'rel': ref('int_customers'),            'key': 'customer_id'},
    {'name': 'dim_customers',      'rel': ref('dim_customers'),            'key': 'customer_id'},
] %}

WITH summary AS (
    {% for c in checks %}
    SELECT
        '{{ c.name }}' AS table_name,
        COUNT(*) - COUNT(DISTINCT {{ c.key }}) AS dup_count
    FROM {{ c.rel }}
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

SELECT
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', CURRENT_TIMESTAMP(), 'Asia/Taipei') AS run_time,
    table_name,
    dup_count
FROM summary
ORDER BY table_name