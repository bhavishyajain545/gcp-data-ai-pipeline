locals {
  services = [
    "pubsub.googleapis.com",
    "dataflow.googleapis.com",
    "bigquery.googleapis.com",
    "bigqueryconnection.googleapis.com",
    "bigquerydatatransfer.googleapis.com",
    "aiplatform.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "compute.googleapis.com",
  ]
}

resource "google_project_service" "apis" {
  for_each           = toset(local.services)
  service            = each.value
  disable_on_destroy = false
}

data "google_project" "this" {}

# ---------------------------------------------------------------------------
# Pub/Sub: events topic, Dataflow subscription, dead-letter topic.
# ---------------------------------------------------------------------------
resource "google_pubsub_topic" "events" {
  name       = "events"
  depends_on = [google_project_service.apis]
}

resource "google_pubsub_topic" "events_dlq" {
  name       = "events-dlq"
  depends_on = [google_project_service.apis]
}

resource "google_pubsub_subscription" "events_dataflow" {
  name                       = "events-dataflow"
  topic                      = google_pubsub_topic.events.id
  ack_deadline_seconds       = 60
  message_retention_duration = "3600s"

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.events_dlq.id
    max_delivery_attempts = 5
  }
}

resource "google_pubsub_subscription" "events_dlq_sub" {
  name  = "events-dlq-inspect"
  topic = google_pubsub_topic.events_dlq.id
}

# Pub/Sub service agent must be able to publish to the DLQ and ack the source.
resource "google_pubsub_topic_iam_member" "dlq_publisher" {
  topic  = google_pubsub_topic.events_dlq.id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription_iam_member" "dlq_subscriber" {
  subscription = google_pubsub_subscription.events_dataflow.id
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# ---------------------------------------------------------------------------
# GCS: Dataflow temp/staging + historical CSV drop zone.
# ---------------------------------------------------------------------------
resource "google_storage_bucket" "pipeline" {
  name                        = "${var.project_id}-pipeline"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true
  depends_on                  = [google_project_service.apis]

  lifecycle_rule {
    condition {
      age            = 3
      matches_prefix = ["temp/", "staging/"]
    }
    action {
      type = "Delete"
    }
  }
}

# ---------------------------------------------------------------------------
# BigQuery: dataset + tables (partitioned / clustered) + Vertex AI connection.
# ---------------------------------------------------------------------------
resource "google_bigquery_dataset" "incidents" {
  dataset_id                 = var.dataset_id
  location                   = var.bq_location
  delete_contents_on_destroy = true
  description                = "Incident and log analytics: raw events, 1-minute metrics, daily summaries, Gemini enrichments"
  depends_on                 = [google_project_service.apis]

  # Explicit ACL: the provider's default adds the creating user by email, which the org's
  # domain-restricted-sharing policy rejects for consumer accounts. Project roles still apply.
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
  access {
    role          = "WRITER"
    user_by_email = google_service_account.dataflow.email
  }
}

resource "google_bigquery_table" "raw_events" {
  dataset_id          = google_bigquery_dataset.incidents.dataset_id
  table_id            = "raw_events"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ts"
  }
  clustering = ["service", "severity"]

  schema = jsonencode([
    { name = "event_id", type = "STRING", mode = "REQUIRED" },
    { name = "ts", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "service", type = "STRING", mode = "REQUIRED" },
    { name = "severity", type = "STRING", mode = "REQUIRED" },
    { name = "latency_ms", type = "INT64", mode = "NULLABLE" },
    { name = "status_code", type = "INT64", mode = "NULLABLE" },
    { name = "error_msg", type = "STRING", mode = "NULLABLE" },
    { name = "region", type = "STRING", mode = "NULLABLE" },
    { name = "host", type = "STRING", mode = "NULLABLE" },
    { name = "ingested_at", type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "service_metrics_1m" {
  dataset_id          = google_bigquery_dataset.incidents.dataset_id
  table_id            = "service_metrics_1m"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "window_start"
  }
  clustering = ["service"]

  schema = jsonencode([
    { name = "window_start", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "window_end", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "service", type = "STRING", mode = "REQUIRED" },
    { name = "events", type = "INT64", mode = "REQUIRED" },
    { name = "errors", type = "INT64", mode = "REQUIRED" },
    { name = "error_rate", type = "FLOAT64", mode = "REQUIRED" },
    { name = "p50_latency_ms", type = "FLOAT64", mode = "NULLABLE" },
    { name = "p95_latency_ms", type = "FLOAT64", mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "dead_letter" {
  dataset_id          = google_bigquery_dataset.incidents.dataset_id
  table_id            = "dead_letter"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "received_at"
  }

  schema = jsonencode([
    { name = "received_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "reason", type = "STRING", mode = "REQUIRED" },
    { name = "payload", type = "STRING", mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "daily_service_summary" {
  dataset_id          = google_bigquery_dataset.incidents.dataset_id
  table_id            = "daily_service_summary"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "day"
  }

  schema = jsonencode([
    { name = "day", type = "DATE", mode = "REQUIRED" },
    { name = "service", type = "STRING", mode = "REQUIRED" },
    { name = "events", type = "INT64", mode = "REQUIRED" },
    { name = "errors", type = "INT64", mode = "REQUIRED" },
    { name = "error_rate", type = "FLOAT64", mode = "REQUIRED" },
    { name = "p95_latency_ms", type = "FLOAT64", mode = "NULLABLE" },
    { name = "top_error", type = "STRING", mode = "NULLABLE" },
    { name = "regions", type = "INT64", mode = "NULLABLE" },
  ])
}

# Cloud resource connection: lets BigQuery call Vertex AI (remote Gemini model).
resource "google_bigquery_connection" "vertex" {
  connection_id = "vertex-ai"
  location      = var.bq_location
  friendly_name = "Vertex AI for BigQuery ML"
  cloud_resource {}
  depends_on = [google_project_service.apis]
}

resource "google_project_iam_member" "bq_connection_vertex" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_bigquery_connection.vertex.cloud_resource[0].service_account_id}"
}

# ---------------------------------------------------------------------------
# Dataflow worker identity (not the Compute default SA).
# ---------------------------------------------------------------------------
resource "google_service_account" "dataflow" {
  account_id   = "sa-dataflow-worker"
  display_name = "Dataflow worker (streaming + batch pipelines)"
  depends_on   = [google_project_service.apis]
}

resource "google_project_iam_member" "dataflow_roles" {
  for_each = toset([
    "roles/dataflow.worker",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/pubsub.subscriber",
    "roles/pubsub.viewer",
  ])
  project = var.project_id
  role    = each.value
  member  = google_service_account.dataflow.member
}

resource "google_storage_bucket_iam_member" "dataflow_bucket" {
  bucket = google_storage_bucket.pipeline.name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.dataflow.member
}

# The person launching jobs needs roles/iam.serviceAccountUser on the worker SA.
# Project Owner already includes iam.serviceAccounts.actAs; a per-SA binding for a
# consumer (gmail) account is rejected by the org's domain-restricted-sharing policy,
# so it is not managed here.

# ---------------------------------------------------------------------------
# Batch ELT: daily summary as a BigQuery scheduled query (03:00 UTC).
# ---------------------------------------------------------------------------
resource "google_bigquery_data_transfer_config" "daily_summary" {
  display_name           = "daily_service_summary ELT"
  location               = var.bq_location
  data_source_id         = "scheduled_query"
  schedule               = "every day 03:00"
  destination_dataset_id = google_bigquery_dataset.incidents.dataset_id
  service_account_name   = google_service_account.dataflow.email

  params = {
    query = templatefile("${path.module}/../sql/elt_daily_summary.sql", { project = var.project_id, dataset = var.dataset_id })
  }
  depends_on = [google_project_iam_member.dataflow_roles]
}
