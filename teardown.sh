#!/usr/bin/env bash
# Teardown so nothing bills afterwards: drain Dataflow jobs, terraform destroy (bucket force_destroy, dataset
# delete_contents_on_destroy, connection, SA, VPC, scheduled query), then a report of what is left. Safe to re-run.
set -uo pipefail
cd "$(dirname "$0")"
PROJECT_ID="$(grep -E '^project_id' terraform/terraform.tfvars | cut -d'"' -f2)"
REGION="$(grep -E '^region' terraform/terraform.tfvars | cut -d'"' -f2)"
g() { gcloud "$@" --project "$PROJECT_ID" --quiet; }

echo "== drain active Dataflow jobs"
for j in $(g dataflow jobs list --region "$REGION" --status active --format='value(id)'); do g dataflow jobs drain "$j" --region "$REGION" || true; done
for i in $(seq 1 20); do
  n=$(g dataflow jobs list --region "$REGION" --status active --format='value(id)' | wc -l); [ "$n" = "0" ] && break
  echo "  waiting for $n job(s) to drain"; sleep 15
done

echo "== terraform destroy"
terraform -chdir=terraform init -input=false >/dev/null 2>&1 || true
terraform -chdir=terraform destroy -input=false -auto-approve 2>&1 | grep -v 'Still destroying' | tail -8

echo "== leftovers in $PROJECT_ID (expect nothing below)"
echo "Dataflow active:";  g dataflow jobs list --region "$REGION" --status active --format='value(name,state)'
echo "BigQuery datasets:"; bq ls --project_id="$PROJECT_ID" 2>/dev/null | tail -n +3
echo "buckets:";          gcloud storage ls --project "$PROJECT_ID" 2>/dev/null | grep -v 'dataflow-staging\|cloudbuild' || true
echo "Pub/Sub topics:";   g pubsub topics list --format='value(name)'
echo "networks:";         g compute networks list --format='value(name)' | grep -v '^default$' || true
echo "done. Looker Studio reports are free; delete the report in lookerstudio.google.com if you want it gone too."
