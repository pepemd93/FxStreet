-- -- CTE to calculate purchases and revenue per product and week
-- WITH purchase_revenue AS (
--   SELECT
--     DATE_TRUNC('week', TIMESTAMP 'epoch' + event_timestamp * INTERVAL '1 millisecond') AS week_start_date,
--     event_params[2].value.string_value AS product,
--     COUNT(*) AS purchases,
--     SUM(event_params[3].value.int_value)::DECIMAL / 100 AS revenue
--   FROM
--     'funnel_data'
--   where event_params[1].value.string_value='purchase'
--   GROUP BY
--     week_start_date,
--     product
-- ),

-- -- CTE to calculate number of users per week at each step of the funnel
-- users_per_step AS (
--   SELECT
--     DATE_TRUNC('week', TIMESTAMP 'epoch' + event_timestamp * INTERVAL '1 millisecond') AS week_start_date,
--     event_params[1].value.string_value AS step,
--     COUNT(DISTINCT user_pseudo_id) AS num_users
--   FROM
--     'funnel_data'
--   GROUP BY
--     week_start_date,
--     step
-- ),

-- -- CTE to calculate rate of users going from each step to the next per week
-- conversion_rate AS (
--   SELECT
--     DATE_TRUNC('week', TIMESTAMP 'epoch' + event_timestamp * INTERVAL '1 millisecond') AS week_start_date,
--     event_params[1].value.string_value AS step,
--     COUNT(*) FILTER (WHERE event_params[1].value.string_value = 'landing')::DECIMAL
--       / COUNT(*)::DECIMAL * 100 AS conversion_rate
--   FROM
--     'funnel_data'
--   GROUP BY
--     week_start_date,
--     step
-- ),

WITH funnel_next_steps AS (
  SELECT 
    week_start_date,
    step as step_from,
    user_pseudo_id,
    LEAD(step) OVER (
      PARTITION BY user_pseudo_id 
      ORDER BY week_start_date, 
        CASE step
          WHEN 'landing' THEN 1 
          WHEN 'login-options' THEN 2 
          WHEN 'sign-up' THEN 3
          WHEN 'checkout' THEN 4
          WHEN 'purchase' THEN 5 
        END
    ) AS step_to 
  FROM 
    {{ref('funnel_metrics')}}
),

funnel_steps_gr AS (
  SELECT
    week_start_date, 
    step_from,
    step_to,
    COUNT(DISTINCT user_pseudo_id) as nr_users
  FROM
    funnel_next_steps
  GROUP BY
    week_start_date, 
    step_from,
    step_to
),

funnel_steps_total AS (
  SELECT 
    week_start_date, 
    COUNT(DISTINCT user_pseudo_id) as total_users
  FROM
    {{ref('funnel_metrics')}}
  GROUP BY week_start_date
)

SELECT 
  fng.week_start_date, 
  fng.step_from,
  fng.step_to,
  fng.nr_users / fs.total_users as conversion_rate
FROM
  funnel_steps_gr fng
JOIN
  funnel_steps_total fs ON fng.week_start_date = fs.week_start_date
ORDER BY fng.week_start_date