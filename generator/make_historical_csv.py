"""Write a synthetic historical export (CSV) for the batch backfill job.

    python generator/make_historical_csv.py --days 3 --per-day 20000 --out data/historical.csv
    gcloud storage cp data/historical.csv gs://<bucket>/historical/

Columns match pipeline.batch_backfill.COLUMNS; a few rows are malformed on purpose.
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from publish_events import make_event

COLUMNS = ["event_id", "ts", "service", "severity", "latency_ms", "status_code", "error_msg", "region", "host"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--per-day", type=int, default=20000)
    ap.add_argument("--out", default="data/historical.csv")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    random.seed(args.seed)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=args.days)
    n = 0
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLUMNS)
        for d in range(args.days):
            day = start + timedelta(days=d)
            incident_hour = random.randint(9, 18)  # one incident window per day on one service
            incident_service = random.choice(["checkout", "payments", "search"])
            for i in range(args.per_day):
                ts = day + timedelta(seconds=random.uniform(0, 86400))
                bursting = ts.hour == incident_hour
                e = make_event(incident_service if bursting else None)
                e["ts"] = ts.isoformat()
                if random.random() < 0.002:
                    e["ts"] = "yesterday"  # malformed -> dead_letter
                w.writerow([e[c] if e[c] is not None else "" for c in COLUMNS])
                n += 1
    print(f"wrote {n} rows to {out}")


if __name__ == "__main__":
    main()
