"""Unit tests for the parse/validate and windowed aggregation steps (no GCP needed).

    cd pipeline && python -m pytest -q
"""

import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms import window

from streaming_pipeline import AggregateWindow, ParseEvent

GOOD = {"event_id": "1", "ts": "2026-11-10T10:00:00+00:00", "service": "checkout", "severity": "error", "latency_ms": 900, "status_code": 503, "error_msg": "x", "region": "us-central1", "host": "h"}


def test_parse_splits_good_and_bad():
    with beam.Pipeline("DirectRunner", options=PipelineOptions([])) as p:
        out = (
            p
            | beam.Create([json.dumps(GOOD).encode(), b"{not json", b'{"event_id":"2","ts":"nope","service":"a","severity":"INFO"}'])
            | beam.ParDo(ParseEvent()).with_outputs(ParseEvent.DEAD, main="ok")
        )
        assert_that(out.ok | beam.Map(lambda e: (e["event_id"], e["severity"], e["latency_ms"], e["ts"].micros)), equal_to([("1", "ERROR", 900, 1794304800000000)]), label="ok")  # ts = 2026-11-10T10:00:00Z
        assert_that(out[ParseEvent.DEAD] | beam.Map(lambda d: d["reason"].split(":")[0]), equal_to(["JSONDecodeError", "ValueError"]), label="dead")


def test_window_aggregate():
    events = [
        {"service": "checkout", "severity": "INFO", "latency_ms": 100},
        {"service": "checkout", "severity": "ERROR", "latency_ms": 900},
        {"service": "checkout", "severity": "INFO", "latency_ms": 120},
        {"service": "checkout", "severity": "INFO", "latency_ms": 110},
    ]
    with beam.Pipeline("DirectRunner", options=PipelineOptions([])) as p:
        out = (
            p
            | beam.Create([("checkout", e) for e in events])
            | beam.Map(lambda kv: window.TimestampedValue(kv, 1_700_000_000))
            | beam.WindowInto(window.FixedWindows(60))
            | beam.GroupByKey()
            | beam.ParDo(AggregateWindow())
            | beam.Map(lambda r: (r["service"], r["events"], r["errors"], r["error_rate"], r["p50_latency_ms"], r["p95_latency_ms"]))
        )
        # nearest-rank percentiles over [100,110,120,900]: p50 -> index round(1.5)=2 -> 120, p95 -> 900
        assert_that(out, equal_to([("checkout", 4, 1, 0.25, 120.0, 900.0)]))
