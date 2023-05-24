-- Number of users per week at each step of the funnel
SELECT 
    week_start_date, 
    step,
    COUNT(DISTINCT user_pseudo_id) as total_users
  FROM
    funnel_metrics
  GROUP BY 
    week_start_date,
    step
  ORDER BY
    week_start_date ASC