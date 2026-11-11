"""Publish realistic application log / incident events to Pub/Sub.

    python generator/publish_events.py --project bj-data-ai-pipeline --rate 20
    python generator/publish_events.py --project bj-data-ai-pipeline --rate 20 --burst-after 60 --burst-seconds 120

Normal mode: ~2% errors, latency ~ lognormal around 120 ms.
Burst mode: one service (checkout by default) degrades: 35% errors, latencies 5-10x, a dominant
error message, which is what the dashboard and the Gemini root-cause step should surface.
A small share of messages is deliberately malformed to exercise the dead-letter path.
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from google.cloud import pubsub_v1

fake = Faker()
SERVICES = ["checkout", "payments", "catalog", "auth", "search", "notifications"]
REGIONS = ["us-central1", "us-east1", "europe-west1", "asia-south1"]
ERRORS = {
    "DB": ["connection pool exhausted (max 50) while querying orders", "deadlock detected on table payments_ledger", "pg: canceling statement due to statement timeout"],
    "network": ["upstream connect error or disconnect/reset before headers", "dial tcp 10.8.3.21:6379: i/o timeout", "TLS handshake timeout to catalog-svc"],
    "deploy": ["readiness probe failed: HTTP 503 after rollout v2.14.0", "ImagePullBackOff for payments:2.14.0", "schema migration 0042 pending; refusing to start"],
    "config": ["missing env var STRIPE_WEBHOOK_SECRET", "invalid feature flag value for checkout.new_flow", "KeyVault secret rotation not propagated"],
    "code": ["NullPointerException at CartService.merge(CartService.java:214)", "KeyError: 'sku' in price_lookup", "panic: runtime error: index out of range [3] with length 3"],
    "capacity": ["429 Too Many Requests from search index", "CPU throttled 92% for 5m; HPA at max replicas", "queue depth 48k, consumers lagging"],
}


def make_event(burst_service: str | None) -> dict:
    service = random.choice(SERVICES)
    region = random.choice(REGIONS)
    in_burst = burst_service is not None and service == burst_service
    err_p = 0.35 if in_burst else 0.02
    is_error = random.random() < err_p
    base = random.lognormvariate(4.8, 0.45)  # ~120 ms median
    latency = base * (random.uniform(5, 10) if in_burst else 1.0)
    if is_error:
        category = "DB" if in_burst and random.random() < 0.7 else random.choice(list(ERRORS))
        msg = random.choice(ERRORS[category])
        severity = "CRITICAL" if in_burst and random.random() < 0.3 else "ERROR"
        status = random.choice([500, 502, 503, 504])
    else:
        msg, severity, status = None, random.choice(["INFO", "INFO", "INFO", "WARN"]), random.choice([200, 200, 200, 201, 204, 304])
    return {
        "event_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "severity": severity,
        "latency_ms": int(latency),
        "status_code": status,
        "error_msg": msg,
        "region": region,
        "host": f"{service}-{fake.pystr(min_chars=5, max_chars=5).lower()}",
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--topic", default="events")
    ap.add_argument("--rate", type=float, default=20.0, help="events per second")
    ap.add_argument("--burst-after", type=int, default=0, help="seconds before the incident burst starts (0 = never)")
    ap.add_argument("--burst-seconds", type=int, default=120)
    ap.add_argument("--burst-service", default="checkout")
    ap.add_argument("--bad-ratio", type=float, default=0.01, help="share of malformed messages (dead-letter test)")
    ap.add_argument("--duration", type=int, default=0, help="stop after N seconds (0 = run until Ctrl-C)")
    args = ap.parse_args()

    publisher = pubsub_v1.PublisherClient()
    topic = publisher.topic_path(args.project, args.topic)
    stop = False

    def _sig(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    start = time.time()
    sent = bad = 0
    interval = 1.0 / args.rate
    print(f"publishing to {topic} at {args.rate}/s (burst after {args.burst_after}s for {args.burst_seconds}s on {args.burst_service})", flush=True)
    while not stop:
        elapsed = time.time() - start
        if args.duration and elapsed >= args.duration:
            break
        bursting = args.burst_after and args.burst_after <= elapsed < args.burst_after + args.burst_seconds
        if random.random() < args.bad_ratio:
            payload = b'{"event_id": "broken", "ts": "not-a-timestamp", "service": 42}'
            bad += 1
        else:
            payload = json.dumps(make_event(args.burst_service if bursting else None)).encode()
        publisher.publish(topic, payload, mode="burst" if bursting else "normal")
        sent += 1
        if sent % (int(args.rate) * 10 or 10) == 0:
            print(f"t+{int(elapsed):4d}s sent={sent} malformed={bad} {'BURST' if bursting else ''}", flush=True)
        time.sleep(interval)
    print(f"done: sent={sent} malformed={bad}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
