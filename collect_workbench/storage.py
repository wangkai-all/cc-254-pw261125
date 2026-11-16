"""SQLite 持久化层。

表结构：
- runs:           一次采集+核验运行（钉选一个来源版本，带预算账本）
- ranges:         区间状态机（每个 run 一份区间行）
- collected_rows: 采集到的数据行（按 run 隔离，重抓只覆盖同一 run 的内容）
- proofs:         证明记录：完整 / 抽检 / 缺口三类，换版后旧证明标记 STALE
- request_log:    每次对来源的请求流水（成本审计）
- kv:             崩溃恢复点等杂项键值
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    mode                TEXT NOT NULL,              -- full | sample
    pinned_version      TEXT,
    status              TEXT NOT NULL,              -- 见 engine.RunStatus
    sample_rate         REAL NOT NULL,
    budget_rows         INTEGER NOT NULL,
    budget_verify       INTEGER NOT NULL,
    budget_refetch      INTEGER NOT NULL,
    used_rows           INTEGER NOT NULL DEFAULT 0,
    used_verify         INTEGER NOT NULL DEFAULT 0,
    used_refetch        INTEGER NOT NULL DEFAULT 0,
    total               INTEGER,
    note                TEXT,
    created_at          REAL NOT NULL,
    updated_at          REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS ranges (
    run_id              INTEGER NOT NULL,
    lo                  INTEGER NOT NULL,
    hi                  INTEGER NOT NULL,
    state               TEXT NOT NULL,              -- 见 engine.RangeState
    sampled             INTEGER NOT NULL DEFAULT 0,
    expected_count      INTEGER,
    actual_count        INTEGER,
    expected_digest     TEXT,
    actual_digest       TEXT,
    refetch_rounds      INTEGER NOT NULL DEFAULT 0,
    detail              TEXT,                       -- JSON: 缺口/疑点说明
    updated_at          REAL NOT NULL,
    PRIMARY KEY (run_id, lo, hi)
);
CREATE TABLE IF NOT EXISTS collected_rows (
    run_id              INTEGER NOT NULL,
    row_id              INTEGER NOT NULL,
    served_version      TEXT,
    row_json            TEXT NOT NULL,
    updated_at          REAL NOT NULL,
    PRIMARY KEY (run_id, row_id)
);
CREATE TABLE IF NOT EXISTS proofs (
    proof_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id              INTEGER NOT NULL,
    kind                TEXT NOT NULL,              -- COMPLETE | SAMPLED | GAP
    version             TEXT NOT NULL,
    status              TEXT NOT NULL,              -- ACTIVE | STALE
    lo                  INTEGER,
    hi                  INTEGER,
    detail_json         TEXT NOT NULL,
    created_at          REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS request_log (
    seq                 INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id              INTEGER,
    kind                TEXT NOT NULL,              -- rows | count | digest | version
    detail              TEXT,
    created_at          REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS kv (
    k                   TEXT PRIMARY KEY,
    v                   TEXT NOT NULL
);
"""


class Storage:
    """每个操作开一个连接（check_same_thread=False + 进程内写锁）。"""

    def __init__(self, path: str):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self._wlock = threading.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        with self._wlock, self._connect() as conn:
            conn.executescript(SCHEMA)

    # ---------------- kv ----------------
    def kv_get(self, key: str) -> Optional[Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
        return json.loads(row["v"]) if row else None

    def kv_put(self, key: str, value: Any) -> None:
        with self._wlock, self._connect() as conn:
            conn.execute(
                "INSERT INTO kv(k,v) VALUES(?,?) "
                "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (key, json.dumps(value, ensure_ascii=False)))

    # ---------------- runs ----------------
    def create_run(self, mode: str, sample_rate: float,
                   budgets: Dict[str, int]) -> int:
        now = time.time()
        with self._wlock, self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO runs(mode,pinned_version,status,sample_rate,"
                "budget_rows,budget_verify,budget_refetch,created_at,updated_at)"
                " VALUES(?,NULL,?,?,?,?,?,?,?)",
                (mode, "RUNNING", sample_rate, budgets["rows"],
                 budgets["verify"], budgets["refetch"], now, now))
            return int(cur.lastrowid)

    def update_run(self, run_id: int, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = time.time()
        cols = ", ".join(f"{k}=?" for k in fields)
        with self._wlock, self._connect() as conn:
            conn.execute(f"UPDATE runs SET {cols} WHERE run_id=?",
                         (*fields.values(), run_id))

    def get_run(self, run_id: int) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id=?",
                               (run_id,)).fetchone()
        return dict(row) if row else None

    def list_runs(self) -> List[dict]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM runs ORDER BY run_id DESC"
                                ).fetchall()
        return [dict(r) for r in rows]

    def recoverable_run(self) -> Optional[dict]:
        """找最近一个处于中断态的 run（崩溃后恢复用）。"""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE status IN ('RUNNING','CRASHED') "
                "ORDER BY run_id DESC LIMIT 1").fetchone()
        return dict(row) if row else None

    # ---------------- ranges ----------------
    def upsert_range(self, run_id: int, lo: int, hi: int, state: str,
                     **fields: Any) -> None:
        now = time.time()
        if "detail" in fields and not isinstance(fields["detail"], str):
            fields["detail"] = json.dumps(fields["detail"],
                                          ensure_ascii=False)
        known = {"state", "sampled", "expected_count", "actual_count",
                 "expected_digest", "actual_digest", "refetch_rounds",
                 "detail"}
        fields = {k: v for k, v in fields.items() if k in known}
        fields["state"] = state
        fields["updated_at"] = now
        cols = ["run_id", "lo", "hi", *fields.keys()]
        placeholders = ", ".join("?" for _ in cols)
        updates = ", ".join(f"{k}=excluded.{k}" for k in fields)
        with self._wlock, self._connect() as conn:
            conn.execute(
                f"INSERT INTO ranges({', '.join(cols)}) VALUES({placeholders}) "
                f"ON CONFLICT(run_id,lo,hi) DO UPDATE SET {updates}",
                (run_id, lo, hi, *fields.values()))

    def list_ranges(self, run_id: int) -> List[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM ranges WHERE run_id=? ORDER BY lo",
                (run_id,)).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["detail"] = json.loads(d["detail"]) if d["detail"] else None
            out.append(d)
        return out

    def get_range(self, run_id: int, lo: int, hi: int) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM ranges WHERE run_id=? AND lo=? AND hi=?",
                (run_id, lo, hi)).fetchone()
        if not row:
            return None
        d = dict(row)
        d["detail"] = json.loads(d["detail"]) if d["detail"] else None
        return d

    # ---------------- collected rows ----------------
    def upsert_rows(self, run_id: int, rows: List[dict],
                    served_version: Optional[str]) -> None:
        now = time.time()
        with self._wlock, self._connect() as conn:
            conn.executemany(
                "INSERT INTO collected_rows(run_id,row_id,served_version,"
                "row_json,updated_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(run_id,row_id) DO UPDATE SET "
                "served_version=excluded.served_version,"
                "row_json=excluded.row_json,updated_at=excluded.updated_at",
                [(run_id, r["id"], served_version,
                  json.dumps(r, ensure_ascii=False), now) for r in rows])

    def get_rows(self, run_id: int, lo: int, hi: int) -> List[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT row_json FROM collected_rows WHERE run_id=? "
                "AND row_id>=? AND row_id<? ORDER BY row_id",
                (run_id, lo, hi)).fetchall()
        return [json.loads(r["row_json"]) for r in rows]

    def count_rows(self, run_id: int) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) c FROM collected_rows WHERE run_id=?",
                (run_id,)).fetchone()
        return int(row["c"])

    def missing_ids(self, run_id: int, lo: int, hi: int) -> List[int]:
        present = {r["id"] for r in self.get_rows(run_id, lo, hi)}
        return [i for i in range(lo, hi) if i not in present]

    # ---------------- proofs ----------------
    def add_proof(self, run_id: int, kind: str, version: str,
                  detail: Any, lo: Optional[int] = None,
                  hi: Optional[int] = None) -> int:
        now = time.time()
        with self._wlock, self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO proofs(run_id,kind,version,status,lo,hi,"
                "detail_json,created_at) VALUES(?,?,?, 'ACTIVE',?,?,?,?)",
                (run_id, kind, version, lo, hi,
                 json.dumps(detail, ensure_ascii=False), now))
            return int(cur.lastrowid)

    def mark_proofs_stale(self, except_run: Optional[int] = None) -> int:
        """来源换版后，旧版本的全部 ACTIVE 证明失效。"""
        with self._wlock, self._connect() as conn:
            if except_run is None:
                cur = conn.execute(
                    "UPDATE proofs SET status='STALE' WHERE status='ACTIVE'")
            else:
                cur = conn.execute(
                    "UPDATE proofs SET status='STALE' WHERE status='ACTIVE' "
                    "AND run_id<>?", (except_run,))
            return cur.rowcount

    def mark_proofs_stale_except_version(self, version: str) -> int:
        """钉选新版本建 run 时，把其他版本的 ACTIVE 证明置为 STALE。"""
        with self._wlock, self._connect() as conn:
            cur = conn.execute(
                "UPDATE proofs SET status='STALE' WHERE status='ACTIVE' "
                "AND version<>?", (version,))
            return cur.rowcount

    def delete_proof(self, proof_id: int) -> None:
        with self._wlock, self._connect() as conn:
            conn.execute("DELETE FROM proofs WHERE proof_id=?", (proof_id,))

    def list_proofs(self, include_stale: bool = True) -> List[dict]:
        sql = "SELECT * FROM proofs"
        if not include_stale:
            sql += " WHERE status='ACTIVE'"
        sql += " ORDER BY proof_id DESC"
        with self._connect() as conn:
            rows = conn.execute(sql).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["detail_json"] = json.loads(d["detail_json"])
            out.append(d)
        return out

    # ---------------- request log ----------------
    def log_request(self, run_id: Optional[int], kind: str,
                    detail: str) -> None:
        with self._wlock, self._connect() as conn:
            conn.execute(
                "INSERT INTO request_log(run_id,kind,detail,created_at) "
                "VALUES(?,?,?,?)",
                (run_id, kind, detail, time.time()))
