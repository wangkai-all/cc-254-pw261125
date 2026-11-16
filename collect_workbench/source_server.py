"""本地来源服务。

提供两类共三个读取接口：
- 分页读取（采集口）：GET /rows?offset=&limit=
    可能发生故障：缺页（返回被篡改后的 200 响应）、旧缓存（返回上一版本的 200 响应）。
    故障响应的 HTTP 状态码仍然是 200，模拟"状态码成功但内容不可信"。
- 核验口（始终权威，不受缺页/旧缓存故障影响）：
    GET /count?lo=&hi=   区间数量
    GET /digest?lo=&hi=  区间内容摘要（与 dataset.range_digest 同一协议）

管理口（故障注入 / 版本切换 / 计数）：
    GET  /version                 当前版本
    POST /admin/version          切换版本 {"version": "v2"}
    POST /admin/faults           覆盖式设置故障列表
    GET  /admin/faults           查看故障
    POST /admin/drift            设置"第 N 次 /rows 后自动换版"
    GET  /admin/counters         各接口请求计数（独立成本口径）
    POST /admin/reset_counters
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional

from . import dataset, httpjson


class SourceState:
    """来源的内存状态：当前版本、故障配置、请求计数。线程安全。"""

    def __init__(self, data_dir: str):
        self._lock = threading.Lock()
        self._data_dir = data_dir
        self._versions = {
            name: dataset.load_version(data_dir, name)
            for name in (dataset.V1_NAME, dataset.V2_NAME)
        }
        self.current_version = dataset.V1_NAME
        # 故障项：{"type": "drop"|"stale", "offset": int, "limit": int,
        #          "ids": [int] 可选, "once": bool}
        self.faults: List[dict] = []
        # 漂移计划：第 drift_after 次 /rows 响应后切换到 drift_version
        self.drift_after: Optional[int] = None
        self.drift_version: Optional[str] = None
        self.counters = {"rows": 0, "count": 0, "digest": 0,
                         "version": 0, "admin": 0}

    # ---- 数据访问 -------------------------------------------------------
    def doc(self, version: Optional[str] = None) -> dict:
        return self._versions[version or self.current_version]

    def rows_in(self, lo: int, hi: int, version: Optional[str] = None
                ) -> List[dict]:
        return dataset.slice_rows(self.doc(version)["rows"], lo, hi)

    # ---- 漂移与计数 -----------------------------------------------------
    def tick_rows(self) -> None:
        self.counters["rows"] += 1
        n = self.counters["rows"]
        if (self.drift_after is not None and self.drift_version is not None
                and n >= self.drift_after):
            self.current_version = self.drift_version
            self.drift_after = None
            self.drift_version = None

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "current_version": self.current_version,
                "faults": [dict(f) for f in self.faults],
                "drift_after": self.drift_after,
                "drift_version": self.drift_version,
                "counters": dict(self.counters),
            }


def make_handler(state: SourceState):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):  # 安静运行，演示脚本另做输出
            pass

        # ---------- 读取接口 ----------
        def do_GET(self):
            from urllib.parse import urlparse, parse_qs
            u = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            try:
                if u.path == "/version":
                    with state._lock:
                        state.counters["version"] += 1
                        ver = state.current_version
                    return httpjson.send_json(self, {"version": ver})
                if u.path == "/rows":
                    return self._handle_rows(q)
                if u.path == "/count":
                    return self._handle_count(q)
                if u.path == "/digest":
                    return self._handle_digest(q)
                if u.path == "/admin/faults":
                    return httpjson.send_json(self, state.snapshot())
                if u.path == "/admin/counters":
                    with state._lock:
                        return httpjson.send_json(
                            self, {"counters": dict(state.counters)})
                httpjson.send_json(self, {"error": "not found"}, 404)
            except Exception as exc:  # noqa: BLE001 - 服务端兜底，返回 500
                httpjson.send_json(self, {"error": str(exc)}, 500)

        def do_POST(self):
            from urllib.parse import urlparse
            u = urlparse(self.path)
            try:
                body = httpjson.read_json_body(self)
                with state._lock:
                    state.counters["admin"] += 1
                    if u.path == "/admin/version":
                        ver = body["version"]
                        if ver not in state._versions:
                            return httpjson.send_json(
                                self, {"error": f"unknown version {ver}"}, 400)
                        state.current_version = ver
                        state.drift_after = None
                        state.drift_version = None
                        return httpjson.send_json(
                            self, {"ok": True, "current_version": ver})
                    if u.path == "/admin/faults":
                        state.faults = list(body.get("faults", []))
                        return httpjson.send_json(
                            self, {"ok": True, "faults": state.faults})
                    if u.path == "/admin/drift":
                        state.drift_after = int(body["after_rows"])
                        state.drift_version = body["version"]
                        return httpjson.send_json(self, {"ok": True})
                    if u.path == "/admin/reset_counters":
                        for k in state.counters:
                            state.counters[k] = 0
                        return httpjson.send_json(self, {"ok": True})
                httpjson.send_json(self, {"error": "not found"}, 404)
            except Exception as exc:  # noqa: BLE001
                httpjson.send_json(self, {"error": str(exc)}, 500)

        # ---------- /rows：唯一会被故障污染的接口 ----------
        def _handle_rows(self, q):
            offset = int(q["offset"])
            limit = int(q.get("limit", dataset.PAGE_SIZE))
            with state._lock:
                state.tick_rows()
                version = state.current_version
                page = dataset.slice_rows(
                    state.doc(version)["rows"], offset, offset + limit)
                fault = self._match_fault(offset, limit)
                served_version = version
                if fault is not None:
                    ftype = fault["type"]
                    if ftype == "drop":
                        ids = fault.get("ids")
                        if ids:
                            idset = set(ids)
                            page = [r for r in page if r["id"] not in idset]
                        else:
                            # 默认丢该页第一条，模拟"少行"
                            page = page[1:]
                    elif ftype == "stale":
                        old = dataset.V1_NAME if version == dataset.V2_NAME \
                            else dataset.V2_NAME
                        page = dataset.slice_rows(
                            state.doc(old)["rows"], offset, offset + limit)
                        served_version = old
                    if fault.get("once"):
                        state.faults = [
                            f for f in state.faults if f is not fault]
                payload = {
                    "offset": offset,
                    "limit": limit,
                    "version": served_version,
                    "rows": page,
                }
            httpjson.send_json(self, payload)

        def _match_fault(self, offset: int, limit: int) -> Optional[dict]:
            for fault in state.faults:
                if fault.get("offset", -1) == offset and \
                        fault.get("limit", dataset.PAGE_SIZE) == limit:
                    return fault
            return None

        # ---------- 权威核验接口：不受任何故障影响 ----------
        def _handle_count(self, q):
            lo, hi = int(q["lo"]), int(q["hi"])
            with state._lock:
                state.counters["count"] += 1
                ver = state.current_version
                n = len(state.rows_in(lo, hi))
            httpjson.send_json(self, {"lo": lo, "hi": hi, "version": ver,
                                      "count": n})

        def _handle_digest(self, q):
            lo, hi = int(q["lo"]), int(q["hi"])
            with state._lock:
                state.counters["digest"] += 1
                ver = state.current_version
                rows = state.rows_in(lo, hi)
                digest = dataset.range_digest(rows)
            httpjson.send_json(self, {"lo": lo, "hi": hi, "version": ver,
                                      "digest": digest,
                                      "count": len(rows)})

    return Handler


def build_server(data_dir: str, host: str = "127.0.0.1",
                 port: int = 8081) -> ThreadingHTTPServer:
    state = SourceState(data_dir)
    httpd = ThreadingHTTPServer((host, port), make_handler(state))
    httpd.source_state = state  # type: ignore[attr-defined]
    return httpd


def main(argv: Optional[List[str]] = None) -> int:
    import argparse
    import os
    parser = argparse.ArgumentParser(description="本地来源服务")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8081)
    args = parser.parse_args(argv)
    if not os.path.isdir(args.data_dir):
        raise SystemExit(f"data dir not found: {args.data_dir}")
    httpd = build_server(args.data_dir, args.host, args.port)
    print(f"source server on http://{args.host}:{args.port} "
          f"(data={args.data_dir})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
