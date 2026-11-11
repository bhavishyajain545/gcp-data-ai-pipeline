#!/usr/bin/env bash
# List everything that can still bill in a project (or several). Usage: ./audit-billing.sh [project ...]
# Default: the project in terraform/terraform.tfvars. Empty sections = nothing billable of that kind.
cd "$(dirname "$0")"
PROJECTS=("$@")
[ ${#PROJECTS[@]} -eq 0 ] && PROJECTS=("$(grep -E '^project_id' terraform/terraform.tfvars | cut -d'"' -f2)")
for P in "${PROJECTS[@]}"; do
  echo "################ $P"
  echo "-- compute instances:";   gcloud compute instances list --project "$P" --format='value(name,zone,status)' 2>/dev/null
  echo "-- GKE clusters:";        gcloud container clusters list --project "$P" --format='value(name,status)' 2>/dev/null
  echo "-- forwarding rules:";    gcloud compute forwarding-rules list --project "$P" --format='value(name)' 2>/dev/null
  echo "-- static addresses:";    gcloud compute addresses list --project "$P" --format='value(name,status)' 2>/dev/null
  echo "-- routers/NAT:";         gcloud compute routers list --project "$P" --format='value(name)' 2>/dev/null
  echo "-- networks (custom):";   gcloud compute networks list --project "$P" --format='value(name)' 2>/dev/null | grep -v '^default$'
  echo "-- Cloud Run services:";  gcloud run services list --project "$P" --format='value(metadata.name)' 2>/dev/null
  echo "-- Dataflow active:";     gcloud dataflow jobs list --project "$P" --region us-central1 --status active --format='value(name,state)' 2>/dev/null
  echo "-- Artifact Registry:";   gcloud artifacts repositories list --project "$P" --format='value(name)' 2>/dev/null
  echo "-- GCS buckets:";         gcloud storage ls --project "$P" 2>/dev/null
  echo "-- BigQuery datasets:";   bq ls --project_id="$P" 2>/dev/null | tail -n +3
  echo "-- Firestore:";           gcloud firestore databases list --project "$P" --format='value(name)' 2>/dev/null
  echo "-- Pub/Sub topics:";      gcloud pubsub topics list --project "$P" --format='value(name)' 2>/dev/null
  echo "-- Functions:";           gcloud functions list --project "$P" --format='value(name,state)' 2>/dev/null
done
