"""Batch pipeline: load historical CSV exports from GCS into raw_events.

    python pipeline/batch_backfill.py --project bj-data-ai-pipeline \
        --input gs://bj-data-ai-pipeline-pipeline/historical/*.csv --runner DataflowRunner --region us-central1 \
        --temp_location gs://bj-data-ai-pipeline-pipeline/temp --service_account_email sa-dataflow-worker@...

Same validation as the streaming job (ParseEvent), so one code path decides what a valid event is.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

from streaming_pipeline import ParseEvent, stringify_timestamps

COLUMNS = ["event_id", "ts", "service", "severity", "latency_ms", "status_code", "error_msg", "region", "host"]


def csv_line_to_json_bytes(line: str) -> bytes:
    row = next(csv.reader(io.StringIO(line)))
    rec = dict(zip(COLUMNS, row))
    for k in ("latency_ms", "status_code"):
        rec[k] = int(rec[k]) if rec.get(k) not in (None, "") else None
    if rec.get("error_msg") == "":
        rec["error_msg"] = None
    return json.dumps(rec).encode()


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--input", required=True, help="gs://bucket/historical/*.csv (header row expected)")
    parser.add_argument("--dataset", default="incidents")
    args, beam_args = parser.parse_known_args(argv)
    options = PipelineOptions(beam_args, project=args.project, save_main_session=True)
    table = lambda name: f"{args.project}:{args.dataset}.{name}"  # noqa: E731

    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | "ReadCSV" >> beam.io.ReadFromText(args.input, skip_header_lines=1)
            | "CsvToJson" >> beam.Map(csv_line_to_json_bytes)
            | "ParseAndValidate" >> beam.ParDo(ParseEvent()).with_outputs(ParseEvent.DEAD, main="ok")
        )
        # File loads: no schema passed, so the load job uses the existing table's schema
        # (the string schemas imply NULLABLE fields, which BigQuery rejects against REQUIRED columns).
        parsed.ok | "TimestampsToJson" >> beam.Map(stringify_timestamps) | "WriteRawEvents" >> WriteToBigQuery(
            table("raw_events"),
            write_disposition=BigQueryDisposition.WRITE_APPEND, create_disposition=BigQueryDisposition.CREATE_NEVER,
        )
        parsed[ParseEvent.DEAD] | "DeadTimestampsToJson" >> beam.Map(stringify_timestamps) | "WriteDeadLetter" >> WriteToBigQuery(
            table("dead_letter"),
            write_disposition=BigQueryDisposition.WRITE_APPEND, create_disposition=BigQueryDisposition.CREATE_NEVER,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
