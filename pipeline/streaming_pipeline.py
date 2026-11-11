"""Streaming pipeline: Pub/Sub -> parse/validate -> BigQuery raw_events + dead_letter,
plus 1-minute windowed aggregates per service -> service_metrics_1m.

Local test (DirectRunner, reads the real subscription):
    python pipeline/streaming_pipeline.py --project bj-data-ai-pipeline --runner DirectRunner --streaming

Dataflow (1 worker, Streaming Engine, dedicated worker SA):
    python pipeline/streaming_pipeline.py --project bj-data-ai-pipeline --runner DataflowRunner --streaming \
        --region us-central1 --temp_location gs://bj-data-ai-pipeline-pipeline/temp \
        --staging_location gs://bj-data-ai-pipeline-pipeline/staging \
        --service_account_email sa-dataflow-worker@bj-data-ai-pipeline.iam.gserviceaccount.com \
        --num_workers 1 --max_num_workers 2 --enable_streaming_engine --job_name incident-stream
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
from apache_beam.utils.timestamp import Timestamp

SEVERITIES = {"DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"}


def ts_of(dt: datetime) -> Timestamp:
    """BigQuery TIMESTAMP columns written through the Storage Write API must be Beam Timestamps, not strings."""
    aware = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return Timestamp.from_utc_datetime(aware.astimezone(timezone.utc))


def stringify_timestamps(row: dict) -> dict:
    """For the file-loads path (batch) JSON rows need ISO-8601 strings instead of Timestamp objects."""
    return {k: (v.to_utc_datetime().replace(tzinfo=timezone.utc).isoformat() if isinstance(v, Timestamp) else v) for k, v in row.items()}

RAW_SCHEMA = (
    "event_id:STRING,ts:TIMESTAMP,service:STRING,severity:STRING,latency_ms:INT64,"
    "status_code:INT64,error_msg:STRING,region:STRING,host:STRING,ingested_at:TIMESTAMP"
)
DLQ_SCHEMA = "received_at:TIMESTAMP,reason:STRING,payload:STRING"
METRICS_SCHEMA = (
    "window_start:TIMESTAMP,window_end:TIMESTAMP,service:STRING,events:INT64,errors:INT64,"
    "error_rate:FLOAT64,p50_latency_ms:FLOAT64,p95_latency_ms:FLOAT64"
)


class ParseEvent(beam.DoFn):
    """Validate one Pub/Sub message. Good rows go to the main output, bad ones to 'dead'."""

    DEAD = "dead"

    def process(self, raw: bytes):
        now = ts_of(datetime.now(timezone.utc))
        try:
            e = json.loads(raw.decode("utf-8"))
            if not isinstance(e, dict):
                raise ValueError("payload is not an object")
            event_id = str(e["event_id"])
            ts = datetime.fromisoformat(str(e["ts"]).replace("Z", "+00:00"))
            service = e["service"]
            severity = str(e["severity"]).upper()
            if not isinstance(service, str) or not service:
                raise ValueError("service must be a non-empty string")
            if severity not in SEVERITIES:
                raise ValueError(f"unknown severity {severity}")
            latency = int(e["latency_ms"]) if e.get("latency_ms") is not None else None
            if latency is not None and not (0 <= latency < 600_000):
                raise ValueError("latency_ms out of range")
            yield {
                "event_id": event_id,
                "ts": ts_of(ts),
                "service": service,
                "severity": severity,
                "latency_ms": latency,
                "status_code": int(e["status_code"]) if e.get("status_code") is not None else None,
                "error_msg": e.get("error_msg"),
                "region": e.get("region"),
                "host": e.get("host"),
                "ingested_at": now,
            }
        except Exception as exc:  # malformed -> dead-letter table with the reason
            yield beam.pvalue.TaggedOutput(
                self.DEAD,
                {"received_at": now, "reason": f"{type(exc).__name__}: {exc}"[:500], "payload": raw.decode("utf-8", "replace")[:4000]},
            )


class AggregateWindow(beam.DoFn):
    """Turn (service, [events]) inside a fixed window into one metrics row."""

    def process(self, element, win=beam.DoFn.WindowParam):
        service, events = element
        events = list(events)
        latencies = sorted(e["latency_ms"] for e in events if e["latency_ms"] is not None)
        errors = sum(1 for e in events if e["severity"] in ("ERROR", "CRITICAL"))

        def pct(p: float):
            if not latencies:
                return None
            idx = min(int(round(p * (len(latencies) - 1))), len(latencies) - 1)
            return float(latencies[idx])

        yield {
            "window_start": Timestamp.of(win.start),
            "window_end": Timestamp.of(win.end),
            "service": service,
            "events": len(events),
            "errors": errors,
            "error_rate": errors / len(events) if events else 0.0,
            "p50_latency_ms": pct(0.50),
            "p95_latency_ms": pct(0.95),
        }


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--subscription", default="events-dataflow")
    parser.add_argument("--dataset", default="incidents")
    parser.add_argument("--window_seconds", type=int, default=60)
    args, beam_args = parser.parse_known_args(argv)

    options = PipelineOptions(beam_args, project=args.project, save_main_session=True, streaming=True)
    sub = f"projects/{args.project}/subscriptions/{args.subscription}"
    table = lambda name: f"{args.project}:{args.dataset}.{name}"  # noqa: E731

    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=sub)
            | "ParseAndValidate" >> beam.ParDo(ParseEvent()).with_outputs(ParseEvent.DEAD, main="ok")
        )
        ok, dead = parsed.ok, parsed[ParseEvent.DEAD]

        ok | "WriteRawEvents" >> WriteToBigQuery(
            table("raw_events"), schema=RAW_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            method=WriteToBigQuery.Method.STORAGE_WRITE_API,
        )

        dead | "WriteDeadLetter" >> WriteToBigQuery(
            table("dead_letter"), schema=DLQ_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            method=WriteToBigQuery.Method.STORAGE_WRITE_API,
        )

        (
            ok
            | "KeyByService" >> beam.Map(lambda e: (e["service"], e))
            | "FixedWindows" >> beam.WindowInto(
                window.FixedWindows(args.window_seconds),
                allowed_lateness=120,
                accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING,
            )
            | "GroupByService" >> beam.GroupByKey()
            | "Aggregate" >> beam.ParDo(AggregateWindow())
            | "WriteMetrics1m" >> WriteToBigQuery(
                table("service_metrics_1m"), schema=METRICS_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                method=WriteToBigQuery.Method.STORAGE_WRITE_API,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
