# Demo transcript: demo

## 00:00  Architecture

This project is an incident analytics pipeline on Google Cloud. Application events stream through Pub/Sub into a Dataflow job written with Apache Beam in Python. The job validates every record, writes clean rows to BigQuery with the Storage Write API, sends malformed records to a dead-letter table, and computes one-minute error rates and latency percentiles per service. A batch job backfills historical CSV through the same code, a scheduled query builds daily summaries, and Gemini, called from SQL through a BigQuery remote model, classifies root causes and writes incident notes. Terraform provisions all of it.

## 00:38  Infrastructure

The infrastructure first. Terraform created the Pub/Sub topic with a dead-letter policy, the pipeline bucket, the BigQuery dataset with four partitioned tables, the cloud resource connection that lets BigQuery call Vertex AI, a dedicated Dataflow worker service account with just the roles it needs, and the scheduled query. No service account keys anywhere; the pipeline authenticates with attached identities.

## 01:11  Incident burst

Now the incident. The generator publishes about twenty events per second with a realistic mix, and after a minute it degrades one service, checkout: a third of its requests fail, mostly with database connection errors, and latency jumps five to ten times. One percent of messages are deliberately malformed to exercise the dead-letter path.

## 01:37  Dataflow

In the Dataflow console the streaming job shows the Beam graph: read from Pub/Sub, parse and validate with two outputs, the BigQuery writers, and the windowed aggregation branch. It runs on one worker with Streaming Engine, so the state lives in the service and the worker stays small.

## 02:01  BigQuery

The data lands within seconds. Raw events are partitioned by day and clustered by service and severity, so queries scan only what they need. The one-minute metrics table makes the incident obvious: checkout's error rate climbs from two percent to over thirty while the other services stay flat. And the dead-letter table holds the malformed messages with the exact validation error, so bad data is visible instead of silently dropped.

## 02:27  Gemini

Then Gemini, inside BigQuery. A remote model points at a Gemini Flash endpoint through the connection. One query deduplicates the error messages and asks the model for a single root-cause category each; the result is a table we can group by, and the incident's database errors dominate. A second query writes a three-sentence incident note per service and day, with the most likely cause and one action. Costs stay bounded because we classify distinct messages, not rows, and cap the row count.

## 03:00  Dashboard

The Looker Studio dashboard reads three thin views: error rate and p95 latency over time, where the spike is, the top root causes from Gemini, and the daily notes.

## 03:14  Wrap-up

The streaming job is the only meaningful cost, so it runs for the demo and is drained afterwards; BigQuery and Pub/Sub stay in the free tier, and the Gemini calls are limited by design. Code, SQL, Terraform and the Beam tests are in the repository. Thanks for watching.
