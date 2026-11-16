"""来源 HTTP 客户端：只做参数封装，不做任何判断逻辑。"""

from __future__ import annotations

from typing import Optional

from . import httpjson


class SourceClient:
    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get_version(self) -> dict:
        return httpjson.get_json(f"{self.base_url}/version",
                                 timeout=self.timeout)

    def get_rows(self, offset: int, limit: int) -> dict:
        return httpjson.get_json(
            f"{self.base_url}/rows",
            params={"offset": offset, "limit": limit},
            timeout=self.timeout)

    def get_count(self, lo: int, hi: int) -> dict:
        return httpjson.get_json(
            f"{self.base_url}/count",
            params={"lo": lo, "hi": hi}, timeout=self.timeout)

    def get_digest(self, lo: int, hi: int) -> dict:
        return httpjson.get_json(
            f"{self.base_url}/digest",
            params={"lo": lo, "hi": hi}, timeout=self.timeout)

    # ---- 管理口（故障注入/换版，仅演示与工作台代理使用）----
    def set_version(self, version: str) -> dict:
        return httpjson.post_json(f"{self.base_url}/admin/version",
                                  {"version": version}, timeout=self.timeout)

    def set_faults(self, faults: list) -> dict:
        return httpjson.post_json(f"{self.base_url}/admin/faults",
                                  {"faults": faults}, timeout=self.timeout)

    def set_drift(self, after_rows: int, version: str) -> dict:
        return httpjson.post_json(
            f"{self.base_url}/admin/drift",
            {"after_rows": after_rows, "version": version},
            timeout=self.timeout)

    def get_counters(self) -> dict:
        return httpjson.get_json(f"{self.base_url}/admin/counters",
                                 timeout=self.timeout)

    def reset_counters(self) -> dict:
        return httpjson.post_json(
            f"{self.base_url}/admin/reset_counters", {}, timeout=self.timeout)
