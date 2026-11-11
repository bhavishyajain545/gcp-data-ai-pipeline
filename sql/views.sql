-- Views the Looker Studio dashboard reads (kept thin so the dashboard never scans raw_events directly).
CREATE OR REPLACE VIEW `${project}.${dataset}.v_error_rate_1m` AS
SELECT window_start, service, events, errors, error_rate, p50_latency_ms, p95_latency_ms
FROM `${project}.${dataset}.service_metrics_1m`
WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY);

CREATE OR REPLACE VIEW `${project}.${dataset}.v_top_root_causes` AS
SELECT root_cause, COUNT(*) AS distinct_errors, SUM(occurrences) AS occurrences,
       ARRAY_TO_STRING(ARRAY_AGG(error_msg ORDER BY occurrences DESC LIMIT 3), ' | ') AS examples
FROM `${project}.${dataset}.error_root_cause`
GROUP BY root_cause;

CREATE OR REPLACE VIEW `${project}.${dataset}.v_daily_summary` AS
SELECT s.day, s.service, s.events, s.errors, s.error_rate, s.p95_latency_ms, s.top_error, g.summary
FROM `${project}.${dataset}.daily_service_summary` s
LEFT JOIN `${project}.${dataset}.daily_incident_summary` g USING (day, service);

CREATE OR REPLACE VIEW `${project}.${dataset}.v_dead_letter_recent` AS
SELECT received_at, reason, SUBSTR(payload, 1, 200) AS payload
FROM `${project}.${dataset}.dead_letter`
WHERE received_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
ORDER BY received_at DESC;
