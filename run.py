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
from typing import Any
from urllib.parse import urlparse

PORT = int(os.environ.get("PORT", "8080"))
DEFAULT_WORKLOAD_ID = os.environ.get("WORKLOAD_ID", "geonames")
DEFAULT_TEST_PROCEDURE = os.environ.get("TEST_PROCEDURE", "")
DEFAULT_TELEMETRY = os.environ.get("TELEMETRY", "node-stats")
# Exclude three node-stats fields that are None or absent on Aiven managed OpenSearch
# (containerised, no process cpu/cgroup; indexing_pressure missing on older minors).
# See docs/aiven-application-benchmark.md and the OSB Telemetry Research plan.
DEFAULT_TELEMETRY_PARAMS = os.environ.get(
    "TELEMETRY_PARAMS",
    "node-stats-include-process:false,"
    "node-stats-include-cgroup:false,"
    "node-stats-include-indexing-pressure:false",
)

_lock = threading.Lock()
_state: dict = {"status": "idle", "result": None}
_log_lines: list[str] = []
# Size of the in-memory ring buffer of OSB stdout/stderr lines exposed via
# GET /status as "log_tail". The orchestrator drains this on every poll, so
# 200 lines is plenty to cover one ~10-15s poll interval at heavy log rates
# without spilling. Override via LOG_TAIL_LINES env var on the container.
_LOG_TAIL = max(10, int(os.environ.get("LOG_TAIL_LINES", "200")))


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


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text)


def _table_cells(line: str) -> list[str]:
    if "|" not in line:
        return []
    return [cell.strip() for cell in _strip_ansi(line).split("|") if cell.strip()]


def _parse_metrics(stdout: str) -> dict:
    throughput: float | None = None
    p90: float | None = None
    p99: float | None = None
    for line in stdout.splitlines():
        low = line.strip().lower()
        cells = _table_cells(line)
        if len(cells) >= 3:
            metric = cells[0].lower()
            task = cells[1].lower()
            value = _first_float(cells[2])
            if value is not None:
                if metric == "mean throughput" and "index-append" in task:
                    throughput = value
                elif metric == "90th percentile latency" and p90 is None:
                    p90 = value
                elif metric == "99th percentile latency" and p99 is None:
                    p99 = value
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


def _csv_or_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


def _workload_params(value: Any) -> dict[str, Any]:
    if value is None or value == "":
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("workload_params must be a JSON object")


def _merge_high_level_workload_params(params: dict[str, Any], body: dict[str, Any]) -> dict[str, Any]:
    # Preserve knobs that are not direct OSB flags in the runner's OSB version
    # as workload params for BMC/custom workloads that template them.
    merged = dict(params)
    if body.get("warmup_iterations") is not None:
        merged["warmup_iterations"] = body["warmup_iterations"]
    if body.get("target_throughput"):
        merged["target_throughput"] = body["target_throughput"]
    if body.get("search_clients") is not None:
        merged["clients"] = body["search_clients"]
    return merged


def _run_benchmark(
    opensearch_url: str,
    workload_id: str,
    test_procedure: str,
    telemetry: str,
    telemetry_params: str,
    workload_path: str,
    workload_params: dict[str, Any],
    include_tasks: list[str],
    exclude_tasks: list[str],
    test_iterations: int | None,
    latency_percentiles: list[float],
) -> None:
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
        if telemetry_params:
            argv.append(f"--telemetry-params={telemetry_params}")
    if test_procedure:
        argv.append(f"--test-procedure={test_procedure}")
    if workload_path:
        argv.append(f"--workload-path={workload_path}")
    if workload_params:
        argv.append(f"--workload-params={json.dumps(workload_params, separators=(',', ':'))}")
    if include_tasks:
        argv.append(f"--include-tasks={','.join(include_tasks)}")
    if exclude_tasks:
        argv.append(f"--exclude-tasks={','.join(exclude_tasks)}")
    if test_iterations is not None:
        argv.append(f"--test-iterations={test_iterations}")
    if latency_percentiles:
        argv.append("--latency-percentiles=" + ",".join(f"{value:g}" for value in latency_percentiles))
    client_opts = "use_ssl:true,verify_certs:true"
    if user and password:
        client_opts += f",basic_auth_user:{user},basic_auth_password:{password}"
    argv += ["--client-options", client_opts]

    print(
        f"[runner] starting OSB: workload={workload_id} target={target_hosts} "
        f"telemetry={telemetry or '<none>'} telemetry_params={telemetry_params or '<defaults>'} "
        f"workload_path={'set' if workload_path else '<none>'} "
        f"workload_params_keys={sorted(workload_params.keys())}",
        flush=True,
    )

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
        "workload_params_keys": sorted(workload_params.keys()),
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
        # Empty string in the request explicitly disables telemetry-params (use OSB defaults).
        # Missing key falls back to the runner's DEFAULT_TELEMETRY_PARAMS env var.
        raw_params = body.get("telemetry_params")
        telemetry_params = (
            (raw_params or "").strip() if raw_params is not None else DEFAULT_TELEMETRY_PARAMS
        )
        workload_path = (body.get("workload_path") or "").strip()
        try:
            workload_params = _merge_high_level_workload_params(
                _workload_params(body.get("workload_params")), body
            )
        except (json.JSONDecodeError, ValueError) as exc:
            with _lock:
                _state["status"] = "error"
                _state["result"] = {"ok": False, "error": str(exc), "checked_at": _now()}
            self._send_json(400, {"error": str(exc)})
            return
        include_tasks = _csv_or_list(body.get("include_tasks"))
        exclude_tasks = _csv_or_list(body.get("exclude_tasks"))
        test_iterations = body.get("test_iterations")
        if test_iterations is not None:
            test_iterations = int(test_iterations)
        latency_percentiles = [float(value) for value in _csv_or_list(body.get("latency_percentiles"))]

        t = threading.Thread(
            target=_run_benchmark,
            args=(
                opensearch_url,
                workload_id,
                test_procedure,
                telemetry,
                telemetry_params,
                workload_path,
                workload_params,
                include_tasks,
                exclude_tasks,
                test_iterations,
                latency_percentiles,
            ),
            daemon=True,
        )
        t.start()
        self._send_json(
            202,
            {
                "status": "started",
                "workload_id": workload_id,
                "telemetry": telemetry,
                "telemetry_params": telemetry_params,
                "workload_params_keys": sorted(workload_params.keys()),
                "target": opensearch_url.split("@")[-1],
            },
        )


if __name__ == "__main__":
    with _lock:
        _state["status"] = "idle"
        _state["started_at"] = _now()

    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[runner] serving on :{PORT} (POST /run to start, GET /status to poll)", flush=True)
    server.serve_forever()
