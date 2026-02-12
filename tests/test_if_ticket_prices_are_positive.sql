select ticket_price
from {{ ref('fct_all_ticket_sales') }}
where ticket_price < 0

-- If it returns any rows, the test will fail
-- If it returns no rows, the test will pass