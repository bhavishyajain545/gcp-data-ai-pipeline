-- Gemini in BigQuery: a remote model over the Vertex AI connection, then
-- ML.GENERATE_TEXT to classify distinct error messages and write a daily summary.
-- Run with:  ./deploy.sh gemini   (substitutes ${project}/${dataset}/${connection}/${model})

-- 1. Remote model (idempotent).
CREATE MODEL IF NOT EXISTS `${project}.${dataset}.gemini`
  REMOTE WITH CONNECTION `${connection}`
  OPTIONS (ENDPOINT = '${model}');

-- 2. Root-cause classification of distinct error messages (LIMIT keeps the LLM bill bounded).
CREATE OR REPLACE TABLE `${project}.${dataset}.error_root_cause` AS
WITH distinct_errors AS (
  SELECT error_msg, COUNT(*) AS occurrences, ARRAY_AGG(DISTINCT service LIMIT 5) AS services
  FROM `${project}.${dataset}.raw_events`
  WHERE error_msg IS NOT NULL AND DATE(ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  GROUP BY error_msg
  ORDER BY occurrences DESC
  LIMIT 200
)
SELECT
  error_msg,
  occurrences,
  services,
  COALESCE(NULLIF(LOWER(TRIM(REGEXP_REPLACE(ml_generate_text_llm_result, r'[^A-Za-z]', ''))), ''), 'unknown') AS root_cause
FROM ML.GENERATE_TEXT(
  MODEL `${project}.${dataset}.gemini`,
  (
    SELECT
      error_msg, occurrences, services,
      CONCAT(
        'Classify this production error message into exactly one root-cause category from this list: ',
        'DB, network, deploy, config, code, capacity, external. Reply with the category word only.\n\nError: ',
        error_msg
      ) AS prompt
    FROM distinct_errors
  ),
  -- Gemini 3.x reasons before it answers and the thinking tokens count against this cap,
  -- so a tiny cap (16) returns NULL for some rows. 256 is plenty for a one-word answer.
  STRUCT(0.0 AS temperature, 256 AS max_output_tokens, TRUE AS flatten_json_output)
);

-- 3. One-paragraph incident summary per service for yesterday and today.
CREATE OR REPLACE TABLE `${project}.${dataset}.daily_incident_summary` AS
WITH per_service AS (
  SELECT
    DATE(ts) AS day,
    service,
    COUNT(*) AS events,
    COUNTIF(severity IN ('ERROR', 'CRITICAL')) AS errors,
    APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_latency_ms,
    STRING_AGG(DISTINCT error_msg, ' | ' LIMIT 8) AS sample_errors,
    STRING_AGG(DISTINCT region, ', ' LIMIT 6) AS regions
  FROM `${project}.${dataset}.raw_events`
  WHERE DATE(ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY day, service
)
SELECT
  day, service, events, errors, p95_latency_ms,
  TRIM(ml_generate_text_llm_result) AS summary
FROM ML.GENERATE_TEXT(
  MODEL `${project}.${dataset}.gemini`,
  (
    SELECT
      day, service, events, errors, p95_latency_ms,
      CONCAT(
        'You are an SRE writing a daily incident note. In at most 3 sentences, summarise the day for service "', service,
        '" on ', CAST(day AS STRING), ': ', CAST(events AS STRING), ' events, ', CAST(errors AS STRING),
        ' errors, p95 latency ', CAST(p95_latency_ms AS STRING), ' ms, regions ', IFNULL(regions, 'n/a'),
        '. Sample errors: ', IFNULL(sample_errors, 'none'), '. Name the most likely root cause and one action.'
      ) AS prompt
    FROM per_service
  ),
  STRUCT(0.2 AS temperature, 1024 AS max_output_tokens, TRUE AS flatten_json_output)
);
