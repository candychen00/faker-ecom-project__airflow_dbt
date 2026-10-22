
select * 
from {{ source('ecom', 'customers') }}
