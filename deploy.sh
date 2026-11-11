#!/usr/bin/env bash
# Bring-up for the incident analytics pipeline.
#   ./deploy.sh infra      terraform apply (Pub/Sub, GCS, BigQuery tables + Vertex connection, Dataflow SA, scheduled query)
#   ./deploy.sh stream     launch the streaming Dataflow job (1 worker, Streaming Engine)
#   ./deploy.sh generate   publish events for 4 minutes with a 90 s incident burst after 60 s
#   ./deploy.sh backfill   create a historical CSV, upload it and run the batch Dataflow job
#   ./deploy.sh elt        run the daily summary MERGE now (same SQL as the scheduled query)
#   ./deploy.sh gemini     create the remote Gemini model and run root-cause + daily summary
#   ./deploy.sh views      create the dashboard views
#   ./deploy.sh stop       drain the streaming job
#   ./deploy.sh destroy    terraform destroy
set -euo pipefail
cd "$(dirname "$0")"
PROJECT_ID="$(grep -E '^project_id' terraform/terraform.tfvars | cut -d'"' -f2)"
REGION="$(grep -E '^region' terraform/terraform.tfvars | cut -d'"' -f2)"
BQ_LOCATION="$(grep -E '^bq_location' terraform/terraform.tfvars | cut -d'"' -f2)"
MODEL="$(grep -E '^gemini_model' terraform/terraform.tfvars | cut -d'"' -f2)"
DATASET=incidents
BUCKET="${PROJECT_ID}-pipeline"
SA="sa-dataflow-worker@${PROJECT_ID}.iam.gserviceaccount.com"
PY="${PYTHON:-python}"
# Workers have no external IPs (org policy); the Terraform subnet has Private Google Access.
NET="--no_use_public_ips --subnetwork=regions/${REGION}/subnetworks/dataflow"

render_sql() {  # substitute ${project} ${dataset} ${connection} ${model} in a sql file
  sed -e "s|\${project}|${PROJECT_ID}|g" -e "s|\${dataset}|${DATASET}|g" \
      -e "s|\${connection}|${PROJECT_ID}.${BQ_LOCATION}.vertex-ai|g" -e "s|\${model}|${MODEL}|g" "$1"
}

case "${1:-}" in
  infra)
    terraform -chdir=terraform init -input=false >/dev/null
    terraform -chdir=terraform apply -input=false -auto-approve ;;
  stream)
    $PY pipeline/streaming_pipeline.py --project "$PROJECT_ID" --runner DataflowRunner --streaming \
      --region "$REGION" --temp_location "gs://$BUCKET/temp" --staging_location "gs://$BUCKET/staging" \
      --service_account_email "$SA" --num_workers 1 --max_num_workers 2 --enable_streaming_engine $NET \
      --no_wait_until_finish --setup_file pipeline/setup.py --job_name "incident-stream-$(date +%Y%m%d-%H%M)" ;;
  generate)
    $PY generator/publish_events.py --project "$PROJECT_ID" --rate 20 --burst-after 60 --burst-seconds 90 --duration 240 ;;
  backfill)
    $PY generator/make_historical_csv.py --days 3 --per-day 20000 --out data/historical.csv
    gcloud storage cp data/historical.csv "gs://$BUCKET/historical/historical.csv" --project "$PROJECT_ID"
    $PY pipeline/batch_backfill.py --project "$PROJECT_ID" --input "gs://$BUCKET/historical/*.csv" --runner DataflowRunner \
      --region "$REGION" --temp_location "gs://$BUCKET/temp" --staging_location "gs://$BUCKET/staging" \
      --service_account_email "$SA" --max_num_workers 2 $NET --setup_file pipeline/setup.py --job_name "incident-backfill-$(date +%Y%m%d-%H%M)" ;;
  elt)
    render_sql sql/elt_daily_summary.sql | bq query --project_id="$PROJECT_ID" --location="$BQ_LOCATION" --use_legacy_sql=false ;;
  views)
    render_sql sql/views.sql | bq query --project_id="$PROJECT_ID" --location="$BQ_LOCATION" --use_legacy_sql=false ;;
  gemini)
    render_sql sql/gemini_root_cause.sql | bq query --project_id="$PROJECT_ID" --location="$BQ_LOCATION" --use_legacy_sql=false
    bq query --project_id="$PROJECT_ID" --location="$BQ_LOCATION" --use_legacy_sql=false --format=prettyjson \
      "SELECT root_cause, COUNT(*) AS errors, SUM(occurrences) AS occurrences FROM \`${PROJECT_ID}.${DATASET}.error_root_cause\` GROUP BY 1 ORDER BY 3 DESC" ;;
  stop)
    for j in $(gcloud dataflow jobs list --region "$REGION" --project "$PROJECT_ID" --status active --format='value(id)'); do
      gcloud dataflow jobs drain "$j" --region "$REGION" --project "$PROJECT_ID"; done ;;
  destroy)
    "$0" stop || true
    terraform -chdir=terraform destroy -input=false -auto-approve ;;
  *) sed -n '2,12p' "$0"; exit 1 ;;
esac
