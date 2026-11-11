variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "bq_location" {
  description = "BigQuery dataset location (multi-region US keeps the Vertex AI connection simple)."
  type        = string
  default     = "US"
}

variable "dataset_id" {
  type    = string
  default = "incidents"
}

variable "gemini_model" {
  description = "Vertex AI endpoint for the BigQuery remote model."
  type        = string
  default     = "gemini-3.8-flash"
}
