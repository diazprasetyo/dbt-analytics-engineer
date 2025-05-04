WITH 
fact_orders AS (
    SELECT *
    FROM {{ ref("fact_orders")}}    
)
, dim_customers AS (
    SELECT *
    FROM {{ ref("dim_customers")}}
)

SELECT
        cust.customer_id
        , cust.first_name
        , SUM(total_amount) AS global_paid_amount
FROM fact_orders AS ord
LEFT JOIN dim_customers AS cust
    ON ord.customer_id = cust.customer_id
WHERE ord.is_order_completed = 1
GROUP BY cust.customer_id, cust.first_name
ORDER BY 3 DESC
-- LIMIT 10