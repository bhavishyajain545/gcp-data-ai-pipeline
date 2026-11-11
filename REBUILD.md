# Rebuild from zero

Terraform owns Pub/Sub, GCS, the BigQuery dataset/tables/connection, the Dataflow worker identity, the
network and the scheduled query; `deploy.sh` sequences the jobs and SQL; `demo/` re-records the video.
This page is the exact order of operations plus the gotchas.

Time: ~20 minutes to data in BigQuery (streaming job takes ~3 min to start). Cost while up: the streaming
Dataflow job (~₹25/hour, 1 small worker + Streaming Engine) is the only meaningful cost — drain it when done.

## 0. Prerequisites

| Need | Notes |
|---|---|
| Billing-linked GCP account | Budget + kill switch recommended. |
| `gcloud` (with `bq`), `terraform` >= 1.6, Python 3.12 | Built from WSL Ubuntu 24.04. |
| **Java 17+ (Temurin 21 used)** on the machine that launches the jobs | Beam's Storage Write API sink is cross-language; the launcher starts a Java expansion service. `JAVA_HOME` must be set. |
| Python venv: `pip install -r pipeline/requirements.txt -r generator/requirements.txt setuptools build pytest` | `setuptools`/`build` are needed for `--setup_file` (ships `streaming_pipeline.py` to the workers). |
| `gcloud auth application-default login` + `set-quota-project $PROJECT_ID` | Dataflow API calls are billed/quota'd against the ADC quota project; a different one returns 403. |

Organisation constraints (one-person orgs included):

```bash
export PROJECT_ID=bj-data-ai-pipeline
# Domain-restricted sharing: BigQuery cannot create the anonymous result datasets for a gmail user and `bq query`
# fails with "users named in the policy do not belong to a permitted customer". Override at project level:
gcloud services enable orgpolicy.googleapis.com --project "$PROJECT_ID"
cat > /tmp/drs.yaml <<EOF
name: projects/$PROJECT_ID/policies/iam.allowedPolicyMemberDomains
spec:
  rules:
  - allowAll: true
EOF
gcloud org-policies set-policy /tmp/drs.yaml --project "$PROJECT_ID"
# compute.vmExternalIpAccess=DENY: handled — workers run with --no_use_public_ips in the Terraform subnet with Private Google Access.
# The dataset uses explicit access blocks (no per-user bindings) for the same reason.
```

## 1. Project

```bash
gcloud projects create "$PROJECT_ID" --name "Data + AI pipeline"
gcloud billing projects link "$PROJECT_ID" --billing-account <BILLING_ACCOUNT_ID>
gcloud config set project "$PROJECT_ID"
gcloud auth application-default set-quota-project "$PROJECT_ID"
```

## 2. Configure and build the infrastructure

```bash
git clone https://github.com/bhavishyajain545/gcp-data-ai-pipeline && cd gcp-data-ai-pipeline
cp terraform/terraform.tfvars.example terraform/terraform.tfvars    # project_id, region, bq_location, gemini_model
./deploy.sh infra          # APIs, VPC+subnet (PGA) + firewall, Pub/Sub topic/sub/DLQ, bucket, dataset + 4 tables,
                           # BigQuery→Vertex connection + aiplatform.user, sa-dataflow-worker + roles, scheduled query
```

## 3. Run the pipeline

```bash
./deploy.sh stream         # Dataflow streaming job (Storage Write API, Streaming Engine, 1 worker, no public IPs)
./deploy.sh generate       # 4 min of events, incident burst on checkout after 60 s
./deploy.sh backfill       # 3 days × 20k rows CSV → GCS → Dataflow batch through the same ParseEvent
./deploy.sh elt            # daily_service_summary MERGE now (the scheduled query does it at 03:00 UTC)
./deploy.sh gemini         # CREATE MODEL (remote, gemini-3.8-flash) + ML.GENERATE_TEXT root causes + daily notes
./deploy.sh views          # v_error_rate_1m, v_top_root_causes, v_daily_summary, v_dead_letter_recent
```

Verify: `bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM incidents.raw_events'`, the Dataflow job graph in the console,
`SELECT root_cause, COUNT(*) FROM incidents.error_root_cause GROUP BY 1`.

Unit tests: `cd pipeline && python -m pytest -q` (DirectRunner, no cloud).

## 4. Looker Studio dashboard (manual, ~10 minutes)

The Linking-API "create report" URL currently spins forever (any account); build it in the UI:

1. lookerstudio.google.com → first-time setup (country, company, terms) → Blank report.
2. Add data → BigQuery → Authorize → My Projects → project → `incidents` → `v_error_rate_1m` → Add; repeat for `v_top_root_causes` and `v_daily_summary`.
3. Charts: time series (source v_error_rate_1m, X `window_start`, breakdown `service`, metric `error_rate`); bar (v_top_root_causes, `root_cause` × `occurrences`); table (v_daily_summary: service, day, summary, events, errors).
4. Share → link settings → **Unlisted** → Save. Put the URL in `DEMO_LOOKER_URL` for the recorder and in README/docs.

## 5. Re-record the demo (optional)

DemoProfile Chrome with `--remote-debugging-port=9222` signed in to the console; the streaming job Running;
`DEMO_LOOKER_URL=<report url>`; `DEMO_EXTRA_PATH` with gcloud + ffmpeg. `python -u demo\scenes.py` (~10 min: it runs a
real 150 s burst, waits for windows to close, runs the BigQuery/Gemini queries live, and picks a fully rendered dashboard tab).

## 6. Tear down (stop all cost)

```bash
./deploy.sh stop           # drain the streaming job (check: gcloud dataflow jobs list --region us-central1 --status active)
./deploy.sh destroy        # terraform destroy: bucket force_destroy, dataset delete_contents_on_destroy, tables, connection, SA, VPC
# Looker Studio reports are free to keep; delete the report from lookerstudio.google.com if you like.
```

Keeping the empty project costs nothing.

## Gotchas met while building

- `TIMESTAMP` columns through the Storage Write API need `apache_beam.utils.timestamp.Timestamp` values (timezone-aware); the file-loads path (batch) wants strings → `schema=None` there, otherwise REQUIRED/NULLABLE mode mismatch.
- `with Pipeline(...)` blocks forever on a streaming job → `--no_wait_until_finish`.
- `ModuleNotFoundError: streaming_pipeline` on workers → `--setup_file pipeline/setup.py` (and `pip install setuptools build`).
- Worker pool failed to start → external IPs denied by org policy → `--no_use_public_ips` + subnet with Private Google Access + firewall 12345-12346 for shuffle.
- Gemini 3.x thinking tokens count toward `max_output_tokens`: 16 returned NULL for some rows → 256 (classifier) / 1024 (notes).
- `AI.GENERATE` TVF and `gemini-2.5-flash` were not available on the connection; `ML.GENERATE_TEXT` + `gemini-3.8-flash` work.
- `at` is a reserved word in BigQuery SQL (alias it as `received`).
- Four `bq` processes started at the same instant race on gcloud's credential cache (`config-helper … adc.json`); stagger them by ~2 s.
