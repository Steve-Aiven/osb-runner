#!/usr/bin/env python3
"""
Aiven Application OSB runner.

On start:
  1. Parses OPENSEARCH_URL (injected by Aiven service integration).
  2. Runs opensearch-benchmark against that cluster.
  3. Writes /app/app_results.json with the summary metrics.
  4. Serves /app/ on port 8080 so the orchestrator (and Aiven) can reach it.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
from datetime import datetime, timezone
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse

RESULTS_FILE = Path("/app/app_results.json")
PORT = int(os.environ.get("PORT", "8080"))
WORKLOAD_ID = os.environ.get("WORKLOAD_ID", "geonames")
TEST_PROCEDURE = os.environ.get("TEST_PROCEDURE", "")
TELEMETRY = os.environ.get("TELEMETRY", "node-stats")


def _parse_opensearch_url(url: str) -> tuple[str, str, str]:
    """Return (host:port, user, password) from an https://user:pass@host:port URL."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    port = parsed.port or 9200
    user = parsed.username or ""
    password = parsed.password or ""
    return f"{host}:{port}", user, password


def _first_float(text: str) -> float | None:
    m = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _parse_metrics(stdout: str) -> dict:
    throughput: float | None = None
    p90: float | None = None
    p99: float | None = None
    for line in stdout.splitlines():
        low = line.strip().lower()
        if low.startswith("mean throughput,") and "index-append" in low:
            parts = [p.strip() for p in low.split(",")]
            if len(parts) >= 3:
                throughput = _first_float(parts[2])
        elif low.startswith("90th percentile latency,") and p90 is None:
            parts = [p.strip() for p in low.split(",")]
            if len(parts) >= 3:
                p90 = _first_float(parts[2])
        elif low.startswith("99th percentile latency,") and p99 is None:
            parts = [p.strip() for p in low.split(",")]
            if len(parts) >= 3:
                p99 = _first_float(parts[2])
    return {
        "indexing_throughput_docs_per_sec": throughput,
        "query_latency_p90_ms": p90,
        "query_latency_p99_ms": p99,
    }


def run_benchmark() -> None:
    os_url = os.environ.get("OPENSEARCH_URL", "").strip()
    if not os_url:
        result = {
            "ok": False,
            "error": "OPENSEARCH_URL not set",
            "summary": {"indexing_throughput_docs_per_sec": None, "query_latency_p90_ms": None, "query_latency_p99_ms": None},
            "checked_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        RESULTS_FILE.write_text(json.dumps(result, indent=2))
        return

    target_hosts, user, password = _parse_opensearch_url(os_url)

    argv = [
        "opensearch-benchmark",
        "execute-test",
        "--pipeline=benchmark-only",
        f"--workload={WORKLOAD_ID}",
        f"--target-hosts={target_hosts}",
        f"--telemetry={TELEMETRY}",
        "--kill-running-processes",
    ]
    if TEST_PROCEDURE:
        argv.append(f"--test-procedure={TEST_PROCEDURE}")
    if user and password:
        argv += ["--client-options", f"verify_certs:true,basic_auth_user:{user},basic_auth_password:{password}"]

    print(f"[runner] starting OSB: workload={WORKLOAD_ID} target={target_hosts}", flush=True)
    proc = subprocess.run(argv, capture_output=False, text=True, check=False)

    summary = _parse_metrics(proc.stdout or "")
    result = {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "summary": summary,
        "workload_id": WORKLOAD_ID,
        "test_procedure": TEST_PROCEDURE,
        "checked_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    RESULTS_FILE.write_text(json.dumps(result, indent=2))
    print(f"[runner] OSB finished rc={proc.returncode} summary={summary}", flush=True)


def serve() -> None:
    os.chdir("/app")
    server = HTTPServer(("0.0.0.0", PORT), SimpleHTTPRequestHandler)
    print(f"[runner] serving on :{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    # Write a placeholder so Aiven health check has a response immediately.
    RESULTS_FILE.write_text(json.dumps({"status": "starting", "checked_at": datetime.now(tz=timezone.utc).isoformat()}))

    # Start HTTP server in background thread so Aiven can reach the port.
    t = threading.Thread(target=serve, daemon=True)
    t.start()

    # Run benchmark synchronously; results file is updated when done.
    run_benchmark()

    # Keep serving indefinitely.
    t.join()
