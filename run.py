#!/usr/bin/env python3
"""
Aiven Application OSB runner — HTTP job server.

Deploy once. The orchestrator submits benchmark jobs via HTTP; no redeploy is
needed to target a different OpenSearch cluster.

Endpoints:
  POST /run   {"opensearch_url": "https://user:pass@host:port",
               "workload_id": "geonames",        # optional, default: WORKLOAD_ID env
               "test_procedure": "",              # optional
               "telemetry": "node-stats"}         # optional
              → 202 {"status": "started"}  or  409 {"status": "busy"}

  GET  /status → {"status": "idle"|"running"|"done"|"error",
                  "result": {...} | null}

  GET  /       → same as /status (Aiven health check + orchestrator poll)
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse

PORT = int(os.environ.get("PORT", "8080"))
DEFAULT_WORKLOAD_ID = os.environ.get("WORKLOAD_ID", "geonames")
DEFAULT_TEST_PROCEDURE = os.environ.get("TEST_PROCEDURE", "")
DEFAULT_TELEMETRY = os.environ.get("TELEMETRY", "node-stats")

_lock = threading.Lock()
_state: dict = {"status": "idle", "result": None}
_log_lines: list[str] = []
_LOG_TAIL = 50


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


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


def _run_benchmark(opensearch_url: str, workload_id: str, test_procedure: str, telemetry: str) -> None:
    parsed = urlparse(opensearch_url)
    host = parsed.hostname or ""
    port = parsed.port or 9200
    user = parsed.username or ""
    password = parsed.password or ""
    target_hosts = f"{host}:{port}"

    argv = [
        "opensearch-benchmark",
        "run",
        "--pipeline=benchmark-only",
        f"--workload={workload_id}",
        f"--target-hosts={target_hosts}",
        "--kill-running-processes",
    ]
    if telemetry:
        argv.append(f"--telemetry={telemetry}")
    if test_procedure:
        argv.append(f"--test-procedure={test_procedure}")
    client_opts = "use_ssl:true,verify_certs:true"
    if user and password:
        client_opts += f",basic_auth_user:{user},basic_auth_password:{password}"
    argv += ["--client-options", client_opts]

    print(f"[runner] starting OSB: workload={workload_id} target={target_hosts}", flush=True)

    collected: list[str] = []
    with subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as proc:
        for line in proc.stdout:  # type: ignore[union-attr]
            print(line, end="", flush=True)
            collected.append(line)
            with _lock:
                _log_lines.append(line.rstrip())
                if len(_log_lines) > _LOG_TAIL:
                    del _log_lines[0]
        proc.wait()
    returncode = proc.returncode

    summary = _parse_metrics("".join(collected))
    result = {
        "ok": returncode == 0,
        "returncode": returncode,
        "summary": summary,
        "workload_id": workload_id,
        "test_procedure": test_procedure,
        "checked_at": _now(),
    }
    print(f"[runner] OSB finished rc={returncode} summary={summary}", flush=True)

    with _lock:
        _state["result"] = result
        _state["status"] = "done" if returncode == 0 else "error"


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: object) -> None:
        print(f"[http] {fmt % args}", flush=True)

    def _send_json(self, code: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        with _lock:
            snapshot = dict(_state)
            snapshot["log_tail"] = list(_log_lines)
        self._send_json(200, snapshot)

    def do_POST(self) -> None:
        if self.path != "/run":
            self._send_json(404, {"error": "not found"})
            return

        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            self._send_json(400, {"error": "invalid JSON"})
            return

        opensearch_url = (body.get("opensearch_url") or "").strip()
        if not opensearch_url:
            self._send_json(400, {"error": "opensearch_url is required"})
            return

        with _lock:
            if _state["status"] == "running":
                self._send_json(409, {"status": "busy", "error": "a benchmark is already running"})
                return
            _state["status"] = "running"
            _state["result"] = None
            _log_lines.clear()

        workload_id = (body.get("workload_id") or DEFAULT_WORKLOAD_ID).strip()
        test_procedure = (body.get("test_procedure") or DEFAULT_TEST_PROCEDURE).strip()
        telemetry = (body.get("telemetry") or DEFAULT_TELEMETRY).strip()

        t = threading.Thread(
            target=_run_benchmark,
            args=(opensearch_url, workload_id, test_procedure, telemetry),
            daemon=True,
        )
        t.start()
        self._send_json(202, {"status": "started", "workload_id": workload_id, "target": opensearch_url.split("@")[-1]})


if __name__ == "__main__":
    with _lock:
        _state["status"] = "idle"
        _state["started_at"] = _now()

    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[runner] serving on :{PORT} (POST /run to start, GET /status to poll)", flush=True)
    server.serve_forever()
