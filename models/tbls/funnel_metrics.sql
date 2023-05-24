-- SELECT
--     DATE_TRUNC('week', TIMESTAMP 'epoch' + event_timestamp * INTERVAL '1 millisecond') AS week_start_date,
--     unnest(event_params, recursive :=True),
--     user_pseudo_id
--   FROM
--     funnel_data

SELECT
  DATE_TRUNC('week', TIMESTAMP 'epoch' + event_timestamp * INTERVAL '1 millisecond') AS week_start_date,
  user_pseudo_id,
  (SELECT op.event_params.value.string_value FROM UNNEST(event_params) AS op WHERE op.event_params.key='step') AS step,
  (SELECT op.event_params.value.string_value FROM UNNEST(event_params) AS op WHERE op.event_params.key='product') AS product,
  (SELECT op.event_params.value.int_value FROM UNNEST(event_params) AS op WHERE op.event_params.key='amount') AS amount
FROM
  funnel_data