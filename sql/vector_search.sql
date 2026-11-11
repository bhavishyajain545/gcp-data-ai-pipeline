-- Stretch: embeddings + vector search over error messages to find similar past incidents.
-- Requires the remote embedding model over the same connection.
CREATE MODEL IF NOT EXISTS `${project}.${dataset}.embedding`
  REMOTE WITH CONNECTION `${connection}`
  OPTIONS (ENDPOINT = 'gemini-embedding-001');

CREATE OR REPLACE TABLE `${project}.${dataset}.error_embeddings` AS
SELECT error_msg, occurrences, ml_generate_embedding_result AS embedding
FROM ML.GENERATE_EMBEDDING(
  MODEL `${project}.${dataset}.embedding`,
  (SELECT error_msg AS content, error_msg, occurrences FROM `${project}.${dataset}.error_root_cause`),
  STRUCT(TRUE AS flatten_json_output, 'SEMANTIC_SIMILARITY' AS task_type)
);

-- Find the 5 most similar known errors to a new message.
SELECT base.error_msg, base.occurrences, distance
FROM VECTOR_SEARCH(
  TABLE `${project}.${dataset}.error_embeddings`, 'embedding',
  (
    SELECT ml_generate_embedding_result AS embedding
    FROM ML.GENERATE_EMBEDDING(
      MODEL `${project}.${dataset}.embedding`,
      (SELECT 'connection pool exhausted while talking to postgres' AS content),
      STRUCT(TRUE AS flatten_json_output, 'SEMANTIC_SIMILARITY' AS task_type)
    )
  ),
  top_k => 5, distance_type => 'COSINE'
);
