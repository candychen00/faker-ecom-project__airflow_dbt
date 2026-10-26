
{% test concat_two_strings(model, column_name, col_a, seperator, col_b) %}

WITH
compared as (
    SELECT
        {{ column_name }} as actual,
        CONCAT( {{col_a}}, '{{seperator}}', {{col_b}}) as expected

    FROM {{ model }}
)

SELECT *
FROM compared
WHERE actual IS DISTINCT FROM expected

/*
is distinct from:
-> This operator never returns NULL so NULL values are considered to be distinct from non-NULL values, not other NULL values.
*/

{% endtest %}