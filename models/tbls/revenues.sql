-- Purchases and revenue per product and week (assuming all currencies are USD)
SELECT
    week_start_date,
    product,
    COUNT(product) AS purchases,
    sum(amount)/100 AS revenue
FROM
    {{ref('funnel_metrics')}}
WHERE
    step='purchase'
GROUP BY
    week_start_date,
    product