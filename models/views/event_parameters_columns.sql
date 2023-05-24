-- Create a view to extract event parameters into new columns
SELECT
  event_timestamp,
  event_name,
  user_id,
  user_pseudo_id,
  session_id,
  (SELECT op.event_params.value.string_value FROM UNNEST(event_params) AS op WHERE op.event_params.key='step') AS step,
  (SELECT op.event_params.value.string_value FROM UNNEST(event_params) AS op WHERE op.event_params.key='product') AS product,
  (SELECT op.event_params.value.string_value FROM UNNEST(event_params) AS op WHERE op.event_params.key='currency') AS currency,
  (SELECT op.event_params.value.int_value FROM UNNEST(event_params) AS op WHERE op.event_params.key='amount') AS amount,
  (SELECT op.event_params.value.int_value FROM UNNEST(event_params) AS op WHERE op.event_params.key='has_discount') AS has_discount
FROM
  'funnel_data'