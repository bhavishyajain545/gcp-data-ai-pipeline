-- Batch ELT: one row per service per day. Runs as a BigQuery scheduled query
-- (terraform/main.tf) and can be run ad hoc with the same text.
-- Template variables: ${project}, ${dataset}
MERGE `${project}.${dataset}.daily_service_summary` T
USING (
  WITH ev AS (
    SELECT
      DATE(ts) AS day,
      service,
      severity,
      latency_ms,
      error_msg,
      region
    FROM `${project}.${dataset}.raw_events`
    WHERE DATE(ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  ),
  agg AS (
    SELECT
      day,
      service,
      COUNT(*) AS events,
      COUNTIF(severity IN ('ERROR', 'CRITICAL')) AS errors,
      SAFE_DIVIDE(COUNTIF(severity IN ('ERROR', 'CRITICAL')), COUNT(*)) AS error_rate,
      APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_latency_ms,
      COUNT(DISTINCT region) AS regions
    FROM ev
    GROUP BY day, service
  ),
  top_err AS (
    SELECT day, service, error_msg AS top_error
    FROM (
      SELECT day, service, error_msg,
             ROW_NUMBER() OVER (PARTITION BY day, service ORDER BY COUNT(*) DESC) AS rn
      FROM ev
      WHERE error_msg IS NOT NULL
      GROUP BY day, service, error_msg
    )
    WHERE rn = 1
  )
  SELECT a.day, a.service, a.events, a.errors, a.error_rate, a.p95_latency_ms, t.top_error, a.regions
  FROM agg a LEFT JOIN top_err t USING (day, service)
) S
ON T.day = S.day AND T.service = S.service
WHEN MATCHED THEN UPDATE SET
  events = S.events, errors = S.errors, error_rate = S.error_rate,
  p95_latency_ms = S.p95_latency_ms, top_error = S.top_error, regions = S.regions
WHEN NOT MATCHED THEN INSERT (day, service, events, errors, error_rate, p95_latency_ms, top_error, regions)
  VALUES (S.day, S.service, S.events, S.errors, S.error_rate, S.p95_latency_ms, S.top_error, S.regions);
