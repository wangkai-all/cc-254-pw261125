"""采集-核验-补抓引擎（状态机）。

设计要点：
1. 版本钉选：run 创建时钉住来源当前版本；之后每次权威核验都带版本号比对，
   一旦来源换版，run 立即中止并把旧版本 ACTIVE 证明全部标记 STALE——
   采集与核验必须基于同一来源版本，旧证明随换版失效。
2. 三类结论严格区分：
   - PROVEN  -> 证明 COMPLETE：该区间通过了权威 count+digest 核验（补抓后通过也算）
   - SAMPLED -> 仅抽检：本 run 未对该区间发起权威核验，只能说"抽检未见异常"
   - GAP     -> 确认缺口：补抓预算用尽后仍与权威不一致
3. 预算：rows（分页采集）、verify（count+digest 核验）、refetch（补抓闭环）
   三本账分别设上限，任何一本耗尽即优雅中止，已产生的证明仍然有效但不得扩大结论。
4. 补抓范围限定在可疑区间覆盖的页，且每区间补抓轮数有上限、全局有熔断比例，
   不允许"发现一次疑点就全表重抓"，也不允许无限重试。
5. 崩溃恢复：每页采集与每次区间状态变化都立即落盘；进程重启后按 run 恢复，
   已终态区间跳过，预算按已用额度继续扣减。
"""

from __future__ import annotations

import os
import threading
from typing import Any, Dict, List, Optional, Tuple

from . import dataset
from .client import SourceClient
from .storage import Storage


class RunStatus:
    RUNNING = "RUNNING"
    CRASHED = "CRASHED"
    COMPLETE = "COMPLETE"
    ABORTED_DRIFT = "ABORTED_DRIFT"
    ABORTED_BUDGET = "ABORTED_BUDGET"
    ABORTED_BREAKER = "ABORTED_BREAKER"
    ABORTED_ERROR = "ABORTED_ERROR"


class RangeState:
    NEW = "NEW"
    COLLECTED = "COLLECTED"
    SUSPECT = "SUSPECT"
    REFETCHED = "REFETCHED"
    PROVEN = "PROVEN"
    SAMPLED_OK = "SAMPLED_OK"
    GAP = "GAP"


TERMINAL_RANGE_STATES = {RangeState.PROVEN, RangeState.SAMPLED_OK,
                         RangeState.GAP}

DEFAULT_BUDGETS = {"rows": 60, "verify": 40, "refetch": 20}
DEFAULT_SAMPLE_RATE = 0.5
DEFAULT_REFETCH_ROUNDS = 2
DEFAULT_BREAKER_RATIO = 0.5


class DriftDetected(RuntimeError):
    pass


class BudgetExhausted(RuntimeError):
    pass


class Engine:
    def __init__(self, storage: Storage, client: SourceClient,
                 page_size: int = dataset.PAGE_SIZE,
                 refetch_rounds: int = DEFAULT_REFETCH_ROUNDS,
                 breaker_ratio: float = DEFAULT_BREAKER_RATIO,
                 crash_after: Optional[int] = None):
        self.db = storage
        self.client = client
        self.page_size = page_size
        self.refetch_rounds = refetch_rounds
        self.breaker_ratio = breaker_ratio
        # crash_after: 进程内累计第 N 次来源请求后模拟崩溃（写 CRASHED 后退出）
        self.crash_after = crash_after
        self._req_count = 0
        self._lock = threading.Lock()  # 同一时刻只允许一个 run 推进

    # ---------------- 对外入口 ----------------
    def start_run(self, mode: str = "full",
                  sample_rate: float = DEFAULT_SAMPLE_RATE,
                  budgets: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
        with self._lock:
            active = self.db.recoverable_run()
            if active is not None:
                raise RuntimeError(
                    f"run {active['run_id']} 仍处于 {active['status']}，"
                    "请先恢复或放弃后再发起新 run")
            budgets = {**DEFAULT_BUDGETS, **(budgets or {})}
            run_id = self.db.create_run(mode, sample_rate, budgets)
            try:
                self._drive(run_id, mode, sample_rate)
            except (DriftDetected, BudgetExhausted, BreakerTripped) as exc:
                self._abort(run_id, exc)
            except Exception as exc:  # noqa: BLE001
                self.db.update_run(run_id, status=RunStatus.ABORTED_ERROR,
                                   note=f"{type(exc).__name__}: {exc}")
                raise
            return self.run_summary(run_id)

    def resume_run(self, run_id: int) -> Dict[str, Any]:
        with self._lock:
            run = self.db.get_run(run_id)
            if run is None:
                raise RuntimeError(f"unknown run {run_id}")
            if run["status"] not in (RunStatus.RUNNING, RunStatus.CRASHED):
                raise RuntimeError(f"run {run_id} 状态为 {run['status']}，"
                                   "无需恢复")
            self.db.update_run(run_id, status=RunStatus.RUNNING,
                               note="恢复执行")
            try:
                self._drive(run_id, run["mode"], run["sample_rate"])
            except (DriftDetected, BudgetExhausted, BreakerTripped) as exc:
                self._abort(run_id, exc)
            return self.run_summary(run_id)

    def discard_run(self, run_id: int) -> None:
        """显式放弃一个中断态 run（其证明保留为历史记录）。"""
        with self._lock:
            run = self.db.get_run(run_id)
            if run and run["status"] in (RunStatus.RUNNING,
                                         RunStatus.CRASHED):
                self.db.update_run(run_id, status=RunStatus.ABORTED_ERROR,
                                   note="人工放弃")

    # ---------------- 主驱动 ----------------
    def _drive(self, run_id: int, mode: str, sample_rate: float) -> None:
        run = self.db.get_run(run_id)
        if not run["pinned_version"]:
            pinned = self._request("version",
                                   lambda: self.client.get_version(),
                                   run_id)["version"]
            # 建 run 时取一次全表数量（权威口，计入 verify 预算）
            total = self._request(
                "count", lambda: self.client.get_count(0, 10 ** 9), run_id,
                budget="verify")["count"]
            self.db.update_run(run_id, pinned_version=pinned, total=total)
            for lo, hi in dataset.partitions(total):
                self.db.upsert_range(run_id, lo, hi, RangeState.NEW)
            # 钉选版本与旧证明版本不同 => 旧证明立即失效
            self.db.mark_proofs_stale_except_version(pinned)
        else:
            pinned = run["pinned_version"]
            total = run["total"]
            # 恢复时先确认来源版本仍是钉选版本
            self._assert_version(run_id, pinned)

        self._collect(run_id, pinned, total)
        self._assert_version(run_id, pinned)
        self._verify(run_id, pinned, mode, sample_rate)
        self._finalize(run_id, pinned)

    # ---------------- 采集阶段 ----------------
    def _collect(self, run_id: int, pinned: str, total: int) -> None:
        ck_key = f"run:{run_id}:collect_offset"
        offset = self.db.kv_get(ck_key) or 0
        while offset < total:
            run = self.db.get_run(run_id)
            if run["used_rows"] >= run["budget_rows"]:
                raise BudgetExhausted("rows 预算耗尽，采集中止")
            resp = self._request(
                "rows", lambda: self.client.get_rows(offset, self.page_size),
                run_id, budget="rows")
            served = resp["version"]
            page = resp["rows"]
            if served != pinned:
                # 可能是旧缓存故障，也可能是来源换版：以权威 /version 裁决
                current = self._request(
                    "version", lambda: self.client.get_version(), run_id)
                if current["version"] != pinned:
                    raise DriftDetected(
                        f"来源已换版 {pinned} -> {current['version']}")
                self._mark_suspect(run_id, offset, offset + self.page_size,
                                   f"分页返回旧缓存版本 {served}")
            is_short = len(page) < self.page_size and \
                offset + self.page_size < total
            self.db.upsert_rows(run_id, page, served)
            if is_short:
                self._mark_suspect(run_id, offset, offset + self.page_size,
                                   f"页短行：期望 {self.page_size} 实得 "
                                   f"{len(page)}")
            self._touch_collected(run_id, offset, offset + self.page_size)
            offset += self.page_size
            self.db.kv_put(ck_key, offset)  # 崩溃恢复点

    def _touch_collected(self, run_id: int, lo: int, hi: int) -> None:
        for r in self.db.list_ranges(run_id):
            if r["hi"] <= lo or r["lo"] >= hi:
                continue
            if r["state"] == RangeState.NEW:
                self.db.upsert_range(run_id, r["lo"], r["hi"],
                                     RangeState.COLLECTED)

    def _mark_suspect(self, run_id: int, lo: int, hi: int,
                      reason: str) -> None:
        for r in self.db.list_ranges(run_id):
            if r["hi"] <= lo or r["lo"] >= hi:
                continue
            if r["state"] in TERMINAL_RANGE_STATES:
                continue
            detail = r["detail"] or {"reasons": []}
            detail.setdefault("reasons", []).append(reason)
            self.db.upsert_range(run_id, r["lo"], r["hi"],
                                 RangeState.SUSPECT, detail=detail)

    # ---------------- 核验阶段 ----------------
    def _verify(self, run_id: int, pinned: str, mode: str,
                sample_rate: float) -> None:
        ranges = self.db.list_ranges(run_id)
        n = len(ranges)
        sampled_idx = {i for i in range(n) if self._is_sampled(
            i, n, sample_rate if mode == "sample" else 1.0)}
        for idx, r in enumerate(ranges):
            if r["state"] in TERMINAL_RANGE_STATES:
                continue  # 崩溃恢复：已终态区间不重复花费预算
            lo, hi = r["lo"], r["hi"]
            if mode == "sample" and idx not in sampled_idx:
                # 未抽中的区间：只声明"抽检未见异常"，不发起权威核验
                self.db.upsert_range(run_id, lo, hi,
                                     RangeState.SAMPLED_OK,
                                     sampled=0,
                                     detail={"note": "本 run 未抽中，"
                                             "未做权威核验，不等于完整"})
                self._write_proof(run_id, pinned, lo, hi)
                continue
            self._verify_one(run_id, pinned, lo, hi, round_no=0)
            self._check_breaker(run_id, n)

    def _is_sampled(self, idx: int, n: int, rate: float) -> bool:
        if rate >= 1.0:
            return True
        if rate <= 0.0:
            return idx in (0, n - 1)
        step = max(1, round(1.0 / rate))
        return idx % step == 0 or idx in (0, n - 1)

    def _verify_one(self, run_id: int, pinned: str, lo: int, hi: int,
                    round_no: int) -> None:
        """对一个区间做权威核验；不一致则在限定预算内补抓并复核。"""
        ok, authoritative, local = self._check_against_source(
            run_id, pinned, lo, hi, first_round=(round_no == 0))
        if ok:
            self.db.upsert_range(
                run_id, lo, hi, RangeState.PROVEN, sampled=1,
                expected_count=authoritative["count"],
                actual_count=local["count"],
                expected_digest=authoritative["digest"],
                actual_digest=local["digest"],
                detail={"verified_at_round": round_no})
            self._write_proof(run_id, pinned, lo, hi)
            return

        # 疑点确认 -> 限定范围补抓（只重抓覆盖该区间的页）
        r = self.db.get_range(run_id, lo, hi)
        detail = r["detail"] or {"reasons": []}
        detail["first_mismatch"] = {
            "expected": authoritative, "local": local}
        self.db.upsert_range(run_id, lo, hi, RangeState.SUSPECT,
                             sampled=1, detail=detail)
        for round_no in range(1, self.refetch_rounds + 1):
            run = self.db.get_run(run_id)
            if run["used_refetch"] >= run["budget_refetch"]:
                raise BudgetExhausted("refetch 预算耗尽，补抓中止")
            self._refetch_range(run_id, pinned, lo, hi)
            self.db.upsert_range(
                run_id, lo, hi, RangeState.REFETCHED,
                refetch_rounds=round_no)
            ok, authoritative, local = self._check_against_source(
                run_id, pinned, lo, hi, first_round=False)
            self._check_breaker(
                run_id, len(self.db.list_ranges(run_id)))
            if ok:
                self.db.upsert_range(
                    run_id, lo, hi, RangeState.PROVEN, sampled=1,
                    expected_count=authoritative["count"],
                    actual_count=local["count"],
                    expected_digest=authoritative["digest"],
                    actual_digest=local["digest"],
                    detail={"verified_at_round": round_no,
                            "reasons": detail.get("reasons", [])})
                self._write_proof(run_id, pinned, lo, hi)
                return
        # 补抓轮数耗尽仍不一致 -> 确认缺口（只对该区间下结论，不扩大）
        missing = self.db.missing_ids(run_id, lo, hi)
        gap_detail = {
            "missing_ids": missing,
            "expected_count": authoritative["count"],
            "local_count": local["count"],
            "digest_mismatch":
                authoritative["digest"] != local["digest"],
            "refetch_rounds": self.refetch_rounds,
            "reasons": detail.get("reasons", []),
        }
        self.db.upsert_range(run_id, lo, hi, RangeState.GAP, sampled=1,
                             expected_count=authoritative["count"],
                             actual_count=local["count"],
                             expected_digest=authoritative["digest"],
                             actual_digest=local["digest"],
                             detail=gap_detail)
        self._write_proof(run_id, pinned, lo, hi)

    def _check_against_source(self, run_id: int, pinned: str, lo: int,
                              hi: int, first_round: bool
                              ) -> Tuple[bool, dict, dict]:
        run = self.db.get_run(run_id)
        budget_kind = "verify" if first_round else "refetch"
        if first_round and run["used_verify"] + 2 > run["budget_verify"]:
            raise BudgetExhausted("verify 预算耗尽，核验中止")
        if not first_round and run["used_refetch"] + 2 > \
                run["budget_refetch"]:
            raise BudgetExhausted("refetch 预算耗尽，无法复核补抓结果")
        count_resp = self._request(
            budget_kind, lambda: self.client.get_count(lo, hi), run_id,
            budget=budget_kind)
        digest_resp = self._request(
            budget_kind, lambda: self.client.get_digest(lo, hi), run_id,
            budget=budget_kind)
        if count_resp["version"] != pinned or \
                digest_resp["version"] != pinned:
            raise DriftDetected(
                f"核验响应版本 {digest_resp['version']} 与钉选版本 {pinned} "
                "不一致")
        local_rows = self.db.get_rows(run_id, lo, hi)
        local = {"count": len(local_rows),
                 "digest": dataset.range_digest(local_rows)}
        authoritative = {"count": count_resp["count"],
                         "digest": digest_resp["digest"],
                         "version": digest_resp["version"]}
        ok = local["count"] == authoritative["count"] and \
            local["digest"] == authoritative["digest"]
        return ok, authoritative, local

    def _refetch_range(self, run_id: int, pinned: str, lo: int,
                       hi: int) -> None:
        """只重抓与 [lo,hi) 相交的页，而非全表。"""
        start = (lo // self.page_size) * self.page_size
        offset = start
        while offset < hi:
            run = self.db.get_run(run_id)
            if run["used_refetch"] >= run["budget_refetch"]:
                raise BudgetExhausted("refetch 预算耗尽，补抓中止")
            resp = self._request(
                "refetch",
                lambda off=offset: self.client.get_rows(
                    off, self.page_size),
                run_id, budget="refetch")
            if resp["version"] != pinned:
                current = self._request(
                    "version", lambda: self.client.get_version(), run_id)
                if current["version"] != pinned:
                    raise DriftDetected("补抓期间来源换版")
                self._mark_suspect(run_id, offset,
                                   offset + self.page_size,
                                   f"补抓页仍返回旧缓存 {resp['version']}")
            self.db.upsert_rows(run_id, resp["rows"], resp["version"])
            offset += self.page_size

    def _check_breaker(self, run_id: int, n_ranges: int) -> None:
        bad = sum(1 for r in self.db.list_ranges(run_id)
                  if r["state"] in (RangeState.SUSPECT, RangeState.GAP))
        if n_ranges and bad > self.breaker_ratio * n_ranges:
            raise BreakerTripped(
                f"可疑/缺口区间 {bad}/{n_ranges} 超过熔断比例 "
                f"{self.breaker_ratio}，停止补抓以免全表重抓")

    # ---------------- 收尾与证明 ----------------
    def _write_proof(self, run_id: int, pinned: str, lo: int,
                     hi: int) -> None:
        """区间到达终态时即时写证明（中止/崩溃后已得证明依然有效）。"""
        r = self.db.get_range(run_id, lo, hi)
        kind = {RangeState.PROVEN: "COMPLETE",
                RangeState.SAMPLED_OK: "SAMPLED",
                RangeState.GAP: "GAP"}[r["state"]]
        self._upsert_proof(run_id, kind, pinned, r)

    def _finalize(self, run_id: int, pinned: str) -> None:
        ranges = self.db.list_ranges(run_id)
        unfinished = [r for r in ranges
                      if r["state"] not in TERMINAL_RANGE_STATES]
        if unfinished:
            # 理论上只有预算/熔断中止才会走到这里；守住边界，不冒认完成
            self.db.update_run(
                run_id, status=RunStatus.ABORTED_BUDGET,
                note=f"仍有 {len(unfinished)} 个区间未到终态，保持未决")
            return
        self.db.update_run(run_id, status=RunStatus.COMPLETE,
                           note=f"{len(ranges)} 个区间均已有结论")

    def _upsert_proof(self, run_id: int, kind: str, version: str,
                      r: dict) -> None:
        existing = [p for p in self.db.list_proofs()
                    if p["run_id"] == run_id and p["lo"] == r["lo"]
                    and p["hi"] == r["hi"]]
        detail = {"range_state": r["state"], "range_detail": r["detail"],
                  "sampled": bool(r["sampled"])}
        if existing:
            # 简化处理：删旧写新（proof_id 会变，历史请求流水仍保留审计轨迹）
            self.db.delete_proof(existing[0]["proof_id"])
        self.db.add_proof(run_id, kind, version, detail,
                          lo=r["lo"], hi=r["hi"])

    def _assert_version(self, run_id: int, pinned: str) -> None:
        resp = self._request("version",
                             lambda: self.client.get_version(), run_id)
        if resp["version"] != pinned:
            raise DriftDetected(
                f"来源版本 {resp['version']} != 钉选版本 {pinned}")

    def _abort(self, run_id: int, exc: Exception) -> None:
        if isinstance(exc, DriftDetected):
            self.db.mark_proofs_stale()  # 换版：所有 ACTIVE 证明失效
            self.db.update_run(run_id, status=RunStatus.ABORTED_DRIFT,
                               note=str(exc))
        elif isinstance(exc, BudgetExhausted):
            self.db.update_run(run_id, status=RunStatus.ABORTED_BUDGET,
                               note=str(exc))
        elif isinstance(exc, BreakerTripped):
            self.db.update_run(run_id, status=RunStatus.ABORTED_BREAKER,
                               note=str(exc))

    # ---------------- 预算与请求记账 ----------------
    def _request(self, kind: str, fn, run_id: Optional[int],
                 budget: Optional[str] = None) -> Any:
        if budget is not None and run_id is not None:
            run = self.db.get_run(run_id)
            used = run[f"used_{budget}"]
            cap = run[f"budget_{budget}"]
            if used >= cap:
                raise BudgetExhausted(f"{budget} 预算耗尽（{used}/{cap}）")
        resp = fn()
        self._req_count += 1
        if run_id is not None and budget is not None:
            field = f"used_{budget}"
            self.db.update_run(run_id, **{field: self.db.get_run(
                run_id)[field] + 1})
        if run_id is not None:
            self.db.log_request(run_id, kind,
                                f"{kind} #{self._req_count}")
        if self.crash_after is not None and \
                self._req_count >= self.crash_after:
            # 模拟核验途中崩溃：状态已落盘，进程退出后可恢复
            self.db.update_run(run_id, status=RunStatus.CRASHED,
                               note=f"模拟崩溃（第 {self._req_count} 次请求）")
            os._exit(3)
        return resp

    # ---------------- 汇总视图 ----------------
    def run_summary(self, run_id: int) -> Dict[str, Any]:
        run = self.db.get_run(run_id)
        ranges = self.db.list_ranges(run_id)
        counts = {s: 0 for s in (RangeState.NEW, RangeState.COLLECTED,
                                 RangeState.SUSPECT, RangeState.REFETCHED,
                                 RangeState.PROVEN, RangeState.SAMPLED_OK,
                                 RangeState.GAP)}
        for r in ranges:
            counts[r["state"]] = counts.get(r["state"], 0) + 1
        return {
            "run": run,
            "range_counts": counts,
            "ranges": ranges,
            "proofs": [p for p in self.db.list_proofs()
                      if p["run_id"] == run_id],
            "budgets": {
                "rows": {"used": run["used_rows"], "cap": run["budget_rows"]},
                "verify": {"used": run["used_verify"],
                           "cap": run["budget_verify"]},
                "refetch": {"used": run["used_refetch"],
                            "cap": run["budget_refetch"]},
            },
        }


class BreakerTripped(RuntimeError):
    pass


def main(argv: Optional[List[str]] = None) -> int:
    import argparse
    import json
    parser = argparse.ArgumentParser(description="采集核验引擎 CLI")
    parser.add_argument("--db", required=True)
    parser.add_argument("--source", default="http://127.0.0.1:8081")
    parser.add_argument("--mode", choices=["full", "sample"], default="full")
    parser.add_argument("--sample-rate", type=float,
                        default=DEFAULT_SAMPLE_RATE)
    parser.add_argument("--budget-rows", type=int,
                        default=DEFAULT_BUDGETS["rows"])
    parser.add_argument("--budget-verify", type=int,
                        default=DEFAULT_BUDGETS["verify"])
    parser.add_argument("--budget-refetch", type=int,
                        default=DEFAULT_BUDGETS["refetch"])
    parser.add_argument("--resume", type=int, default=None,
                        help="恢复指定 run_id")
    parser.add_argument("--crash-after", type=int, default=None)
    args = parser.parse_args(argv)

    db = Storage(args.db)
    client = SourceClient(args.source)
    engine = Engine(db, client, crash_after=args.crash_after)
    budgets = {"rows": args.budget_rows, "verify": args.budget_verify,
               "refetch": args.budget_refetch}
    if args.resume is not None:
        summary = engine.resume_run(args.resume)
    else:
        summary = engine.start_run(args.mode, args.sample_rate, budgets)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
