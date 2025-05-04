SELECT
        orderid AS order_id
        , SUM(amount) AS total_amount
FROM {{source('stripe', 'payment')}}
GROUP BY 1
HAVING total_amount < 0