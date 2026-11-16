"""HTTP JSON 小工具：服务端收发与客户端请求，全部基于标准库。"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from http import HTTPStatus
from typing import Any, Dict, Optional


def send_json(handler, obj: Any, status: int = HTTPStatus.OK) -> None:
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def read_json_body(handler) -> Dict[str, Any]:
    length = int(handler.headers.get("Content-Length", "0") or 0)
    raw = handler.rfile.read(length) if length else b"{}"
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def _request(method: str, url: str, payload: Optional[dict],
             timeout: float) -> dict:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:  # 把 4xx/5xx 的 JSON 错误体透传
        detail = e.read().decode("utf-8", "replace")
        try:
            parsed = json.loads(detail)
        except json.JSONDecodeError:
            parsed = {"error": detail}
        raise HttpError(e.code, parsed) from None


class HttpError(RuntimeError):
    def __init__(self, status: int, body: Any):
        super().__init__(f"HTTP {status}: {body}")
        self.status = status
        self.body = body


def get_json(url: str, params: Optional[dict] = None,
             timeout: float = 10.0) -> dict:
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    return _request("GET", url, None, timeout)


def post_json(url: str, payload: Optional[dict] = None,
              timeout: float = 10.0) -> dict:
    return _request("POST", url, payload or {}, timeout)
