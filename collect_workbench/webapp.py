"""采集工作台 Web 服务。

对外 API：
- GET  /api/state                总览：run 列表、当前来源版本、证明、预算
- POST /api/runs                 发起 run（mode/sample_rate/budgets）
- POST /api/runs/{id}/resume     恢复中断 run（崩溃恢复入口）
- POST /api/runs/{id}/discard    放弃中断 run
- GET  /api/runs/{id}            run 详情（区间状态机 + 预算账本）
- POST /api/baseline/{run_id}    独立基准对照
- GET  /api/proofs               证明列表（含 STALE）
- GET  /api/source/counters      来源侧请求计数
- POST /api/source/version       来源换版
- POST /api/source/faults        注入/清除故障（缺页、旧缓存）
- POST /api/source/drift         设置采集途中自动换版
- POST /api/source/reset_counters
- GET  /                         静态页面 web/index.html
"""

from __future__ import annotations

import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional

from . import httpjson
from .baseline import Baseline
from .client import SourceClient
from .engine import (BudgetExhausted, DriftDetected, Engine, RunStatus,
                     BreakerTripped)
from .storage import Storage

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "web")


class Workbench:
    def __init__(self, db_path: str, source_url: str,
                 ground_truth_dir: str):
        self.db = Storage(db_path)
        self.client = SourceClient(source_url)
        self.engine = Engine(self.db, self.client)
        self.baseline = Baseline(ground_truth_dir)
        self._run_lock = threading.Lock()  # 同一时刻只推进一个 run

    def source_overview(self) -> Dict[str, Any]:
        return {
            "version": self.client.get_version(),
            "counters": self.client.get_counters()["counters"],
        }

    def start_run(self, payload: dict) -> Dict[str, Any]:
        if not self._run_lock.acquire(blocking=False):
            raise RuntimeError("已有 run 正在执行，请等待其结束")
        try:
            mode = payload.get("mode", "full")
            sample_rate = float(payload.get("sample_rate", 0.5))
            budgets = payload.get("budgets") or {}
            return self.engine.start_run(mode, sample_rate, budgets)
        finally:
            self._run_lock.release()

    def resume_run(self, run_id: int) -> Dict[str, Any]:
        if not self._run_lock.acquire(blocking=False):
            raise RuntimeError("已有 run 正在执行，请等待其结束")
        try:
            return self.engine.resume_run(run_id)
        finally:
            self._run_lock.release()


def make_handler(bench: Workbench):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def _ok(self, obj: Any, status: int = 200) -> None:
            httpjson.send_json(self, obj, status)

        def _err(self, exc: Exception, status: int = 400) -> None:
            self._ok({"error": str(exc),
                      "type": type(exc).__name__}, status)

        def do_GET(self):
            from urllib.parse import urlparse
            path = urlparse(self.path).path
            try:
                if path == "/api/state":
                    recoverable = bench.db.recoverable_run()
                    return self._ok({
                        "runs": bench.db.list_runs(),
                        "recoverable_run_id": recoverable["run_id"]
                        if recoverable else None,
                        "proofs": bench.db.list_proofs(),
                        "source": bench.source_overview(),
                    })
                if path.startswith("/api/runs/"):
                    run_id = int(path.rsplit("/", 1)[-1])
                    return self._ok(bench.engine.run_summary(run_id))
                if path == "/api/proofs":
                    return self._ok(
                        {"proofs": bench.db.list_proofs()})
                if path == "/api/source/counters":
                    return self._ok(bench.client.get_counters())
                if path == "/" or path == "/index.html":
                    return self._serve_index()
                self._err(Exception("not found"), 404)
            except Exception as exc:  # noqa: BLE001
                self._err(exc, 500)

        def do_POST(self):
            from urllib.parse import urlparse
            path = urlparse(self.path).path
            try:
                body = httpjson.read_json_body(self)
                if path == "/api/runs":
                    return self._ok(bench.start_run(body))
                if path.endswith("/resume"):
                    run_id = int(path.split("/")[-2])
                    return self._ok(bench.resume_run(run_id))
                if path.endswith("/discard"):
                    run_id = int(path.split("/")[-2])
                    bench.engine.discard_run(run_id)
                    return self._ok({"ok": True})
                if path.startswith("/api/baseline/"):
                    run_id = int(path.rsplit("/", 1)[-1])
                    return self._ok(bench.baseline.compare_run(
                        bench.db, run_id))
                if path == "/api/source/version":
                    return self._ok(bench.client.set_version(
                        body["version"]))
                if path == "/api/source/faults":
                    return self._ok(bench.client.set_faults(
                        body.get("faults", [])))
                if path == "/api/source/drift":
                    return self._ok(bench.client.set_drift(
                        int(body["after_rows"]), body["version"]))
                if path == "/api/source/reset_counters":
                    return self._ok(bench.client.reset_counters())
                self._err(Exception("not found"), 404)
            except (DriftDetected, BudgetExhausted, BreakerTripped,
                    RuntimeError) as exc:
                self._err(exc, 409)
            except Exception as exc:  # noqa: BLE001
                self._err(exc, 500)

        def _serve_index(self):
            index = os.path.normpath(os.path.join(STATIC_DIR, "index.html"))
            if not index.startswith(os.path.abspath(STATIC_DIR)):
                return self._err(Exception("forbidden"), 403)
            with open(index, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type",
                             "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

    return Handler


def build_server(db_path: str, source_url: str, ground_truth_dir: str,
                 host: str = "127.0.0.1", port: int = 8080
                 ) -> ThreadingHTTPServer:
    bench = Workbench(db_path, source_url, ground_truth_dir)
    httpd = ThreadingHTTPServer((host, port), make_handler(bench))
    httpd.workbench = bench  # type: ignore[attr-defined]
    return httpd


def main(argv: Optional[list] = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="采集工作台 Web 服务")
    parser.add_argument("--db", required=True)
    parser.add_argument("--source", default="http://127.0.0.1:8081")
    parser.add_argument("--ground-truth", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)
    httpd = build_server(args.db, args.source, args.ground_truth,
                         args.host, args.port)
    print(f"workbench on http://{args.host}:{args.port}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
