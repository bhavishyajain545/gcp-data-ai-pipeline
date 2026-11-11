"""Demo video for Project 3: streaming + batch incident analytics with Gemini in BigQuery.

Prerequisites:
  * ./deploy.sh infra done; the streaming Dataflow job is Running (./deploy.sh stream)
  * ./deploy.sh backfill, elt, gemini, views have run at least once (tables populated)
  * Chrome with --remote-debugging-port=9222 signed in to the Cloud console (console scenes)
  * optional: DEMO_LOOKER_URL = Looker Studio report URL for the dashboard scene

  python demo/scenes.py
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
from pathlib import Path

from recorder import Demo

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
PROJECT = "bj-data-ai-pipeline"
REGION = "us-central1"
DATASET = "incidents"
CDP_URL = os.environ.get("DEMO_CDP_URL", "http://127.0.0.1:9222")
LOOKER_URL = os.environ.get("DEMO_LOOKER_URL", "")

for p in os.environ.get("DEMO_EXTRA_PATH", "").split(os.pathsep):
    if p:
        os.environ["PATH"] = p + os.pathsep + os.environ["PATH"]
os.chdir(ROOT)


def wsl(cmd: str) -> str:
    """Run in WSL where gcloud/bq/terraform and the Beam venv live (Windows host)."""
    root = "/mnt/" + ROOT.drive[0].lower() + ROOT.as_posix()[2:] if os.name == "nt" else str(ROOT)
    full = f"source ~/.gcp-env.sh; source ~/venvs/p3/bin/activate 2>/dev/null; cd {root}; {cmd}"
    if os.name == "nt":
        r = subprocess.run(["wsl", "-e", "bash", "-c", full], capture_output=True, text=True)
    else:
        r = subprocess.run(["bash", "-c", full], capture_output=True, text=True)
    return (r.stdout + r.stderr).strip()


def bq(sql: str, fmt: str = "pretty") -> str:
    # The grep also drops gcloud's credential-cache chatter: concurrent bq processes race on legacy_credentials/adc.json
    # and one of them prints a harmless 'config-helper ... Unable to write file' error (seen in take 5).
    return wsl(f"bq query --project_id={PROJECT} --location=US --use_legacy_sql=false --format={fmt} --max_rows=12 \"{sql}\" 2>&1 | grep -v '^$' | grep -v -e 'config-helper' -e 'legacy_credentials' -e 'adc.json'")


async def live_terminal(demo: Demo, page, title: str, steps, type_delay: int = 5, pause: float = 0.4) -> None:
    """Like demo.terminal, but each output is produced *while the command is on screen*.

    `steps` is a list of (command, producer) where producer is a str or a zero-arg callable run in a
    thread (bq/gcloud take 5-8 s each; the viewer sees the command, a short wait, then real output
    instead of a blank terminal while everything is pre-computed).
    """
    await demo.terminal(title, [])
    # Start every producer now, in parallel: bq's CLI start-up plus the query is 6-15 s each, and run
    # serially the terminal sat blank for most of the narration (take 2).
    tasks = []
    for _, p in steps:
        if isinstance(p, asyncio.Future):
            tasks.append(p)
        elif callable(p):
            tasks.append(asyncio.create_task(asyncio.to_thread(p)))
            await asyncio.sleep(2)  # stagger: simultaneous bq start-ups race on gcloud's credential cache
        else:
            tasks.append(None)
    for (cmd, producer), task in zip(steps, tasks):
        await page.evaluate("(c) => window.__prompt(c)", "")
        for ch in cmd:
            await page.evaluate("(c) => window.__type(c)", ch)
            await asyncio.sleep(type_delay / 1000)
        out = await task if task is not None else producer
        await page.evaluate("(o) => window.__output(o)", out or "(no rows)")
        await asyncio.sleep(pause)
    # Keep the final output on screen: when the action already outlasts the narration the scene ends at once,
    # and the last DOM change can miss the screencast entirely (take 6 lost the dead-letter rows).
    await asyncio.sleep(2.5)


def cdp_available() -> bool:
    import urllib.request
    try:
        with urllib.request.urlopen(CDP_URL + "/json/version", timeout=2):
            return True
    except Exception:
        return False


NARRATION = {
    "arch": (
        "This project is an incident analytics pipeline on Google Cloud. Application events stream through Pub/Sub into a Dataflow job written with Apache Beam in Python. "
        "The job validates every record, writes clean rows to BigQuery with the Storage Write API, sends malformed records to a dead-letter table, "
        "and computes one-minute error rates and latency percentiles per service. A batch job backfills historical CSV through the same code, "
        "a scheduled query builds daily summaries, and Gemini, called from SQL through a BigQuery remote model, classifies root causes and writes incident notes. "
        "Terraform provisions all of it."
    ),
    "infra": (
        "The infrastructure first. Terraform created the Pub/Sub topic with a dead-letter policy, the pipeline bucket, the BigQuery dataset with four partitioned tables, "
        "the cloud resource connection that lets BigQuery call Vertex AI, a dedicated Dataflow worker service account with just the roles it needs, and the scheduled query. "
        "No service account keys anywhere; the pipeline authenticates with attached identities."
    ),
    "burst": (
        "Now the incident. The generator publishes about twenty events per second with a realistic mix, and after a minute it degrades one service, checkout: "
        "a third of its requests fail, mostly with database connection errors, and latency jumps five to ten times. One percent of messages are deliberately malformed "
        "to exercise the dead-letter path."
    ),
    "dataflow": (
        "In the Dataflow console the streaming job shows the Beam graph: read from Pub/Sub, parse and validate with two outputs, the BigQuery writers, and the windowed aggregation branch. "
        "It runs on one worker with Streaming Engine, so the state lives in the service and the worker stays small."
    ),
    "bigquery": (
        "The data lands within seconds. Raw events are partitioned by day and clustered by service and severity, so queries scan only what they need. "
        "The one-minute metrics table makes the incident obvious: checkout's error rate climbs from two percent to over thirty while the other services stay flat. "
        "And the dead-letter table holds the malformed messages with the exact validation error, so bad data is visible instead of silently dropped."
    ),
    "gemini": (
        "Then Gemini, inside BigQuery. A remote model points at a Gemini Flash endpoint through the connection. One query deduplicates the error messages and asks the model for a single root-cause category each; "
        "the result is a table we can group by, and the incident's database errors dominate. A second query writes a three-sentence incident note per service and day, "
        "with the most likely cause and one action. Costs stay bounded because we classify distinct messages, not rows, and cap the row count."
    ),
    "dashboard": (
        "The Looker Studio dashboard reads three thin views: error rate and p95 latency over time, where the spike is, the top root causes from Gemini, and the daily notes."
    ),
    "wrap": (
        "The streaming job is the only meaningful cost, so it runs for the demo and is drained afterwards; BigQuery and Pub/Sub stay in the free tier, and the Gemini calls are limited by design. "
        "Code, SQL, Terraform and the Beam tests are in the repository. Thanks for watching."
    ),
}


async def main() -> None:
    use_cdp = cdp_available()
    demo = Demo(out_dir=HERE, cdp_url=CDP_URL if use_cdp else None)
    await demo.prepare(*NARRATION.values())

    # ---- real data for the terminal scenes ----
    t0 = time.time()
    tf_out = wsl("terraform -chdir=terraform output -no-color")
    topics = wsl(f"gcloud pubsub subscriptions describe events-dataflow --project {PROJECT} --format='value(topic,deadLetterPolicy.deadLetterTopic,deadLetterPolicy.maxDeliveryAttempts)'")
    tables = wsl(f"bq ls --project_id={PROJECT} --format=pretty {DATASET} 2>&1 | grep -E 'raw_events|service_metrics|dead_letter|daily_|error_root|incident_summary|TABLE|MODEL' | head -12")
    sa_roles = wsl(f"gcloud projects get-iam-policy {PROJECT} --flatten='bindings[].members' --filter='bindings.members:sa-dataflow-worker@' --format='value(bindings.role)' | tr '\\n' ' '")
    conn_sa = wsl("terraform -chdir=terraform output -raw bq_connection_service_account")
    conn_roles = wsl(f"gcloud projects get-iam-policy {PROJECT} --flatten='bindings[].members' --filter='bindings.members:{conn_sa}' --format='value(bindings.role)'")
    job = wsl(f"gcloud dataflow jobs list --region {REGION} --project {PROJECT} --status active --filter='name~incident-stream' --format='value(id,name,state,type)' --limit=1")
    job_id = job.split()[0] if job else ""
    print(f"pre-computed in {time.time() - t0:.0f}s; job={job}")

    page = await demo.start()
    df_tab = bq_tab = looker_tab = looker_tab2 = None
    if use_cdp and job_id:
        df_tab = await demo.new_tab(f"https://console.cloud.google.com/dataflow/jobs/{REGION}/{job_id}?project={PROJECT}")
        if LOOKER_URL:
            looker_tab = await demo.new_tab(LOOKER_URL)
            looker_tab2 = await demo.new_tab(LOOKER_URL)  # second copy, reloaded during the burst scene (see s3/s7)
        await demo.capture(page)

    # ---- 1 architecture ----
    async def s1():
        await page.goto((ROOT / "docs" / "architecture.html").as_uri())
        await page.evaluate("() => { document.body.style.zoom = '1.33'; document.body.style.margin = '0'; }")
    await demo.scene("Architecture", NARRATION["arch"], s1)

    # ---- 2 infra ----
    async def s2():
        await demo.terminal("Infrastructure (Terraform)", [
            ("terraform -chdir=terraform output", tf_out),
            (f"gcloud pubsub subscriptions describe events-dataflow --format='value(topic,deadLetterPolicy.deadLetterTopic,deadLetterPolicy.maxDeliveryAttempts)'", topics),
            (f"bq ls {DATASET}", tables),
            (f"gcloud projects get-iam-policy {PROJECT} --filter='bindings.members:sa-dataflow-worker@' --format='value(bindings.role)'", sa_roles),
            (f"gcloud projects get-iam-policy {PROJECT} --filter='bindings.members:{conn_sa}' --format='value(bindings.role)'", conn_roles),
        ], type_delay=6, pause=0.4)
    await demo.scene("Infrastructure", NARRATION["infra"], s2, hold=1.0)

    # ---- 3 generator burst (live, ~150 s real time; dead air is cut) ----
    async def s3():
        if looker_tab2 is not None:  # background reload: ~4 min before the dashboard scene
            asyncio.ensure_future(looker_tab2.reload(wait_until="commit", timeout=120000))
        await demo.terminal("Event generator: incident burst", [])
        cmd = f"python generator/publish_events.py --project {PROJECT} --rate 20 --burst-after 30 --burst-seconds 90 --duration 150"
        await page.evaluate("() => window.__prompt('')")
        for ch in cmd:
            await page.evaluate("(c) => window.__type(c)", ch)
            await asyncio.sleep(0.006)
        root = "/mnt/" + ROOT.drive[0].lower() + ROOT.as_posix()[2:] if os.name == "nt" else str(ROOT)
        proc = subprocess.Popen(["wsl", "-e", "bash", "-c", f"source ~/.gcp-env.sh; source ~/venvs/p3/bin/activate; cd {root}; {cmd}"] if os.name == "nt" else ["bash", "-c", cmd],
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        first = True
        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            if first:
                await page.evaluate("(o) => window.__output(o)", line)
                first = False
            else:
                await page.evaluate("(o) => { const t=document.getElementById('t'); const d=document.createElement('div'); d.className='o'; d.textContent=o; t.appendChild(d); }", line)
        proc.wait()
    await demo.scene("Incident burst", NARRATION["burst"], s3, hold=1.0)

    # ---- scene 5 queries (started during scene 4, see below) ----
    bq_jobs: dict = {}

    def s5_queries():
        counts = lambda: bq(f"SELECT 'raw_events' AS t, COUNT(*) n FROM \\`{PROJECT}.{DATASET}.raw_events\\` UNION ALL SELECT 'service_metrics_1m', COUNT(*) FROM \\`{PROJECT}.{DATASET}.service_metrics_1m\\` UNION ALL SELECT 'dead_letter', COUNT(*) FROM \\`{PROJECT}.{DATASET}.dead_letter\\`")
        spike = lambda: bq(f"SELECT FORMAT_TIMESTAMP('%H:%M', window_start) AS minute, service, events, ROUND(error_rate*100,1) AS err_pct, p95_latency_ms FROM \\`{PROJECT}.{DATASET}.service_metrics_1m\\` WHERE service='checkout' AND window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE) ORDER BY window_start DESC LIMIT 10")
        others = lambda: bq(f"SELECT service, ROUND(AVG(error_rate)*100,1) AS avg_err_pct, MAX(p95_latency_ms) AS max_p95 FROM \\`{PROJECT}.{DATASET}.service_metrics_1m\\` WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE) GROUP BY service ORDER BY avg_err_pct DESC")
        dead = lambda: bq(f"SELECT FORMAT_TIMESTAMP('%H:%M:%S', received_at) AS received, reason, SUBSTR(payload,1,60) AS payload FROM \\`{PROJECT}.{DATASET}.dead_letter\\` ORDER BY received_at DESC LIMIT 3")  # 'at' is a reserved word
        return [counts, spike, others, dead]

    # ---- 4 dataflow console ----
    if df_tab is not None:
        async def s4():
            await demo.capture(df_tab)
            try:
                await df_tab.wait_for_selector("text=ParseAndValidate", timeout=60000)
            except Exception:
                pass
            # Linger on the job graph so the burst's 1-minute windows close (watermark lag ~1-2 min) before the
            # BigQuery scene queries them; the dead-air cut drops this wait from the final video.
            await asyncio.sleep(40)
            # Kick off scene 5's queries now (bq takes 6-15 s each): their results are ready when the commands are typed.
            bq_jobs["s5"] = []
            for fn in s5_queries():
                bq_jobs["s5"].append(asyncio.create_task(asyncio.to_thread(fn)))
                await asyncio.sleep(2)  # stagger (credential-cache race between concurrent bq processes)
            await asyncio.sleep(14)
        await demo.scene("Dataflow", NARRATION["dataflow"], s4, hold=1.5)
        await demo.capture(page)
    else:
        async def s4b():
            await demo.terminal("Dataflow streaming job", [(f"gcloud dataflow jobs list --region {REGION} --status active", job)])
        await demo.scene("Dataflow", NARRATION["dataflow"], s4b)

    # ---- 5 bigquery results ----
    async def s5():
        # Results were pre-fetched 20 s ago during the Dataflow scene (bq_jobs["s5"]); fall back to live queries.
        q = bq_jobs.get("s5") or s5_queries()
        await live_terminal(demo, page, "BigQuery: rows arriving, the spike, the dead letters", [
            ("bq query 'SELECT table, COUNT(*) ...'", q[0]),
            ("bq query 'SELECT minute, service, events, err_pct, p95 FROM service_metrics_1m WHERE service=\"checkout\" ...'", q[1]),
            ("bq query 'SELECT service, AVG(error_rate), MAX(p95) FROM service_metrics_1m ... GROUP BY service'", q[2]),
            ("bq query 'SELECT received_at, reason, payload FROM dead_letter ORDER BY received_at DESC LIMIT 3'", q[3]),
        ], type_delay=5, pause=0.5)
    await demo.scene("BigQuery", NARRATION["bigquery"], s5, hold=1.5)

    # ---- 6 gemini ----
    async def looker_ready(tab) -> bool:
        """True when both chart tiles have rendered (each chart lives in a blob: iframe) and no tile errored."""
        try:
            if "System Error" in await tab.evaluate("() => document.body.innerText"):
                return False
            line = bar = False
            for fr in tab.frames:
                if not fr.url.startswith("blob:"):
                    continue
                try:
                    t = await fr.evaluate("() => document.body.innerText")
                except Exception:
                    continue
                line = line or ("checkout" in t and "payments" in t)
                bar = bar or ("occurrences" in t)
            return line and bar
        except Exception:
            return False

    async def s6():
        model = lambda: wsl(f"bq show --project_id={PROJECT} --model --format=prettyjson {DATASET}.gemini | grep -E '\"(endpoint|connection)\"' | tr -d ' ,'")
        root_causes = lambda: bq(f"SELECT root_cause, COUNT(*) AS distinct_errors, SUM(occurrences) AS occurrences FROM \\`{PROJECT}.{DATASET}.error_root_cause\\` GROUP BY 1 ORDER BY 3 DESC")
        sample = lambda: bq(f"SELECT root_cause, SUBSTR(error_msg,1,70) AS error_msg, occurrences FROM \\`{PROJECT}.{DATASET}.error_root_cause\\` ORDER BY occurrences DESC LIMIT 6")
        note = lambda: bq(f"SELECT service, events, errors, SUBSTR(summary,1,150) AS summary FROM \\`{PROJECT}.{DATASET}.daily_incident_summary\\` WHERE day = CURRENT_DATE() ORDER BY errors DESC LIMIT 2")
        await live_terminal(demo, page, "Gemini in BigQuery: ML.GENERATE_TEXT over a remote model", [
            (f"bq show --model {DATASET}.gemini   # from: CREATE MODEL ... REMOTE WITH CONNECTION \\`US.vertex-ai\\` OPTIONS (ENDPOINT='gemini-3.8-flash')", model),
            ("bq query 'SELECT root_cause, COUNT(*), SUM(occurrences) FROM error_root_cause GROUP BY 1 ORDER BY 3 DESC'", root_causes),
            ("bq query 'SELECT root_cause, error_msg, occurrences FROM error_root_cause ORDER BY occurrences DESC LIMIT 6'", sample),
            ("bq query 'SELECT service, events, errors, summary FROM daily_incident_summary WHERE day = CURRENT_DATE() ORDER BY errors DESC LIMIT 2'", note),
        ], type_delay=5, pause=0.5)
    await demo.scene("Gemini", NARRATION["gemini"], s6, hold=2.0)

    # ---- 7 dashboard (optional) ----
    if looker_tab is not None:
        async def s7():
            # Two copies of the dashboard were loaded (start-up, and a reload ~4 min ago during the burst scene):
            # Looker takes 2+ minutes to render the tiles, and a tile left idle for long can show "System Error".
            # Show whichever copy has fully rendered; wait up to 30 s for one.
            chosen = None
            for _ in range(15):
                for tab in (looker_tab, looker_tab2):
                    if tab is not None and await looker_ready(tab):
                        chosen = tab
                        break
                if chosen is not None:
                    break
                await asyncio.sleep(2)
            print("  looker tab:", "ready" if chosen is not None else "NOT ready (showing primary)")
            chosen = chosen or looker_tab
            try:
                await chosen.get_by_text("Dismiss", exact=True).first.click(timeout=1500)
            except Exception:
                pass
            await demo.capture(chosen)
            await asyncio.sleep(3)
        await demo.scene("Dashboard", NARRATION["dashboard"], s7, hold=2.0)
        await demo.capture(page)

    # ---- 8 wrap ----
    async def s8():
        await live_terminal(demo, page, "Cost control + cleanup", [
            (f"gcloud dataflow jobs list --region {REGION} --status active --format='table(name,state,type)'", lambda: wsl(f"gcloud dataflow jobs list --region {REGION} --project {PROJECT} --status active --format='table(name,state,type)'")),
            ("./deploy.sh stop        # drain the streaming job after the demo", "Draining incident-stream-... (Streaming Engine, 1 worker)"),
            ("./deploy.sh destroy     # terraform destroy", "removes topics, bucket, dataset, connection, SA, scheduled query"),
        ], type_delay=8, pause=0.5)
    await demo.scene("Wrap-up", NARRATION["wrap"], s8, hold=1.5)

    await demo.finish("demo")


if __name__ == "__main__":
    asyncio.run(main())
