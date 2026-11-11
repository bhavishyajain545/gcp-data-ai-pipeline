output "topic" {
  value = google_pubsub_topic.events.id
}

output "subscription" {
  value = google_pubsub_subscription.events_dataflow.id
}

output "bucket" {
  value = google_storage_bucket.pipeline.name
}

output "dataset" {
  value = "${var.project_id}.${google_bigquery_dataset.incidents.dataset_id}"
}

output "dataflow_service_account" {
  value = google_service_account.dataflow.email
}

output "dataflow_subnetwork" {
  value = "regions/${var.region}/subnetworks/${google_compute_subnetwork.dataflow.name}"
}

output "bq_connection" {
  value = "${var.bq_location}.${google_bigquery_connection.vertex.connection_id}"
}

output "bq_connection_service_account" {
  value = google_bigquery_connection.vertex.cloud_resource[0].service_account_id
}
