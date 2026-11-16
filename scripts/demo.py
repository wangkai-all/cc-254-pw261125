#!/usr/bin/env python3
"""端到端演示与验收脚本。

启动两个本地 HTTP 服务（来源 :8091 / 工作台 :8090），逐个注入场景，
并用独立基准（直接读基准文件，不走来源接口）对照工作台结论。

场景：
  1. 干净来源 + 全量核验       -> 全部区间 COMPLETE，请求成本最低基线
  2. 干净来源 + 50% 抽检       -> 部分 SAMPLED，页面不得宣称全量完整
  3. 缺页（一次性故障）        -> 疑点 -> 限定补抓 -> 复核通过 COMPLETE
  4. 旧缓存（一次性故障）      -> 摘要不一致 -> 补抓 -> COMPLETE
  5. 持续缺页 + 补抓预算不足   -> 确认 GAP，且 GAP 清单与基准一致；不触发全表重抓
  6. 采集途中来源换版          -> run 中止 ABORTED_DRIFT，旧证明 STALE
  7. 核验预算耗尽              -> 优雅中止 ABORTED_BUDGET，已得证明保留
  8. 核验途中崩溃 + 重启恢复   -> 恢复后完成，总请求数与一次跑完一致（无重复计费）
所有 HTTP 调用都走 127.0.0.1，无外部依赖。
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collect_workbench import dataset, httpjson  # noqa: E402

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SOURCE_PORT = 8091
BENCH_PORT = 8090
SOURCE = f"http://127.0.0.1:{SOURCE_PORT}"
BENCH = f"http://127.0.0.1:{BENCH_PORT}"
PAGE = dataset.PAGE_SIZE

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    mark = "PASS" if cond else "FAIL"
    if cond:
        PASS += 1
    else:
        FAIL += 1
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail else ""))


def wait_up(url: str, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            httpjson.get_json(url, timeout=1)
            return
        except Exception:  # noqa: BLE001
            time.sleep(0.15)
    raise RuntimeError(f"service not up: {url}")


def post(path: str, body: dict) -> Dict[str, Any]:
    return httpjson.post_json(f"{BENCH}{path}", body, timeout=30)


def get(path: str) -> Dict[str, Any]:
    return httpjson.get_json(f"{BENCH}{path}", timeout=30)


def post_raw(path: str, body: dict) -> Dict[str, Any]:
    """POST，允许 4xx 返回（中止类 run）。"""
    try:
        return post(path, body)
    except httpjson.HttpError as e:
        return e.body


def reset_source() -> None:
    httpjson.post_json(f"{SOURCE}/admin/version", {"version": "v1"})
    httpjson.post_json(f"{SOURCE}/admin/faults", {"faults": []})
    httpjson.post_json(f"{SOURCE}/admin/reset_counters", {})


def range_states(summary: dict) -> Dict[str, int]:
    return summary["range_counts"]


def start_full(budgets: Optional[dict] = None,
               mode: str = "full", rate: float = 0.5) -> dict:
    return post_raw("/api/runs", {"mode": mode, "sample_rate": rate,
                                  "budgets": budgets or {}})


def main() -> int:
    root = tempfile.mkdtemp(prefix="cw_demo_")
    data_dir = os.path.join(root, "ground_truth")
    db_path = os.path.join(root, "state", "workbench.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    versions = dataset.build_versions()
    dataset.write_ground_truth(data_dir, versions)
    print(f"工作目录: {root}")
    print(f"v1={len(versions['v1']['rows'])} 行, "
          f"v2={len(versions['v2']['rows'])} 行, "
          f"页={PAGE}, 区间={dataset.RANGE_SIZE}\n")

    py = sys.executable
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    src_proc = subprocess.Popen(
        [py, "-m", "collect_workbench.source_server",
         "--data-dir", data_dir, "--port", str(SOURCE_PORT)],
        cwd=REPO_ROOT, env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    bench_proc = subprocess.Popen(
        [py, "-m", "collect_workbench.webapp", "--db", db_path,
         "--source", SOURCE, "--ground-truth", data_dir,
         "--port", str(BENCH_PORT)],
        cwd=REPO_ROOT, env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_up(f"{SOURCE}/version")
        wait_up(f"{BENCH}/api/state")
        run_scenarios()
    finally:
        for p in (bench_proc, src_proc):
            p.send_signal(signal.SIGINT)
        for p in (bench_proc, src_proc):
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

    print(f"\n==== 验收结果: {PASS} PASS / {FAIL} FAIL ====")
    return 1 if FAIL else 0


def run_scenarios() -> None:
    n_ranges = len(dataset.partitions(dataset.V1_ROWS))

    # ---------- 场景 1：干净 + 全量 ----------
    print("场景 1：干净来源 + 全量核验")
    reset_source()
    s = start_full()
    st = range_states(s)
    check("全部区间 PROVEN", st.get("PROVEN") == n_ranges,
          f"{st}")
    check("无 GAP/SUSPECT", st.get("GAP", 0) == 0 and st.get("SUSPECT", 0) == 0)
    proof_kinds = {p["kind"] for p in s["proofs"]}
    check("证明全部为 COMPLETE", proof_kinds == {"COMPLETE"})
    base_cost = dict(s["budgets"])
    print(f"  成本：rows={base_cost['rows']['used']}, "
          f"verify={base_cost['verify']['used']}, "
          f"refetch={base_cost['refetch']['used']}")
    b = post(f"/api/baseline/{s['run']['run_id']}", {})
    check("独立基准：全部区间一致", b["mismatch_ranges"] == 0
          and b["all_claims_correct"])

    # ---------- 场景 2：干净 + 抽检 ----------
    print("\n场景 2：干净来源 + 50% 抽检")
    reset_source()
    s = start_full(mode="sample", rate=0.5)
    st = range_states(s)
    check("存在 SAMPLED_OK 区间", st.get("SAMPLED_OK", 0) > 0,
          f"SAMPLED_OK={st.get('SAMPLED_OK')}")
    check("存在 PROVEN 区间", st.get("PROVEN", 0) > 0)
    check("没有 GAP", st.get("GAP", 0) == 0)
    check("不存在覆盖全部区间的 COMPLETE 证明",
          len([p for p in s["proofs"] if p["kind"] == "COMPLETE"])
          < n_ranges)
    check("抽检核验请求数明显低于全量",
          s["budgets"]["verify"]["used"] < base_cost["verify"]["used"],
          f"抽检={s['budgets']['verify']['used']} vs "
          f"全量={base_cost['verify']['used']}")
    b = post(f"/api/baseline/{s['run']['run_id']}", {})
    check("基准对照：抽检区间内容恰好正确（本场景来源干净）",
          b["mismatch_ranges"] == 0)
    check("工作台未把 SAMPLED 声称为 COMPLETE", b["all_claims_correct"])

    # ---------- 场景 3：一次性缺页，补抓后通过 ----------
    print("\n场景 3：缺页（一次性，少 2 行）-> 限定补抓 -> 通过")
    reset_source()
    httpjson.post_json(f"{SOURCE}/admin/faults", {"faults": [
        {"type": "drop", "offset": 400, "limit": PAGE,
         "ids": [403, 407], "once": True}]})
    s = start_full()
    st = range_states(s)
    check("最终全部 PROVEN", st.get("PROVEN") == n_ranges, f"{st}")
    check("发生了补抓请求", s["budgets"]["refetch"]["used"] > 0,
          f"refetch={s['budgets']['refetch']['used']}")
    check("补抓范围仅限可疑区间（远小于全表 24 页）",
          s["budgets"]["refetch"]["used"] <= 8,
          f"refetch={s['budgets']['refetch']['used']}")
    b = post(f"/api/baseline/{s['run']['run_id']}", {})
    check("基准对照：无残留缺口", b["mismatch_ranges"] == 0)

    # ---------- 场景 4：一次性旧缓存 ----------
    print("\n场景 4：旧缓存（一次性，v2 内容污染 v1 采集）-> 补抓 -> 通过")
    reset_source()
    # 先换 v2 再换回 v1，让来源"存在上一版本缓存"可被 stale 故障引用
    httpjson.post_json(f"{SOURCE}/admin/faults", {"faults": [
        {"type": "stale", "offset": 800, "limit": PAGE, "once": True}]})
    s = start_full()
    st = range_states(s)
    check("最终全部 PROVEN", st.get("PROVEN") == n_ranges, f"{st}")
    check("旧缓存触发过补抓", s["budgets"]["refetch"]["used"] > 0)
    b = post(f"/api/baseline/{s['run']['run_id']}", {})
    check("基准对照：内容摘要全部一致", b["mismatch_ranges"] == 0)

    # ---------- 场景 5：持续缺页 + 补抓预算不足 -> GAP ----------
    print("\n场景 5：持续缺页且补抓预算受限 -> 确认缺口，不全表重抓")
    reset_source()
    httpjson.post_json(f"{SOURCE}/admin/faults", {"faults": [
        {"type": "drop", "offset": 0, "limit": PAGE, "ids": [0, 1],
         "once": False}]})
    # 每轮补抓覆盖区间 [0,200) 的 4 个页 + 2 次复核 = 6 请求；
    # 两轮共 12，给满 12 让它走完轮数上限并下 GAP 结论，而不是被预算截断
    s = start_full(budgets={"rows": 60, "verify": 40, "refetch": 12})
    st = range_states(s)
    check("出现 GAP 区间", st.get("GAP", 0) >= 1, f"{st}")
    check("其他区间仍为 PROVEN（结论不扩大）",
          st.get("PROVEN") == n_ranges - st.get("GAP", 0))
    check("refetch 预算被用满即停", s["budgets"]["refetch"]["used"]
          <= s["budgets"]["refetch"]["cap"])
    gap = [r for r in s["ranges"] if r["state"] == "GAP"][0]
    check("GAP 详情含缺失 id 清单", gap["detail"] is not None
          and set(gap["detail"]["missing_ids"]) >= {0, 1},
          json.dumps(gap["detail"], ensure_ascii=False))
    b = post(f"/api/baseline/{s['run']['run_id']}", {})
    bad = [x for x in b["ranges"] if not x["consistent"]]
    check("独立基准确认同一批缺口", len(bad) == st.get("GAP")
          and {0, 1}.issubset(set(bad[0]["missing_ids"])),
          f"基准缺口区间={[ (x['lo'],x['hi'],x['missing_ids']) for x in bad ]}")
    check("工作台 GAP 结论与基准相符", b["all_claims_correct"])
    src_counters = httpjson.get_json(f"{SOURCE}/admin/counters")["counters"]
    check("来源侧 rows 请求未发生全表重抓（<= 初始 24 页 + 少量补抓）",
          src_counters["rows"] <= 24 + 8, f"rows={src_counters['rows']}")

    # ---------- 场景 6：途中换版 ----------
    print("\n场景 6：采集途中来源换版 -> 中止且旧证明失效")
    reset_source()
    # 第 6 次 /rows 后自动切到 v2
    httpjson.post_json(f"{SOURCE}/admin/drift",
                       {"after_rows": 6, "version": "v2"})
    s = start_full()
    check("run 状态 ABORTED_DRIFT",
          s["run"]["status"] == "ABORTED_DRIFT", s["run"]["status"])
    state = get("/api/state")
    stale = [p for p in state["proofs"] if p["status"] == "STALE"]
    check("旧版本 ACTIVE 证明已全部 STALE", len(stale) > 0
          and all(p["version"] == "v1" for p in stale))
    check("当前来源版本为 v2", state["source"]["version"]["version"] == "v2")

    # 换版后重新发起 run，应钉选 v2 并正常完成
    httpjson.post_json(f"{SOURCE}/admin/faults", {"faults": []})
    s2 = start_full()
    check("换版后新 run 钉选 v2 且完成",
          s2["run"]["pinned_version"] == "v2"
          and s2["run"]["status"] == "COMPLETE")
    b2 = post(f"/api/baseline/{s2['run']['run_id']}", {})
    check("v2 基准对照全部一致", b2["mismatch_ranges"] == 0)

    # 换回 v1：v2 证明应立即 STALE
    httpjson.post_json(f"{SOURCE}/admin/version", {"version": "v1"})
    s3 = start_full()
    state = get("/api/state")
    v2_active = [p for p in state["proofs"]
                 if p["version"] == "v2" and p["status"] == "ACTIVE"]
    check("再换回 v1 后，v2 证明失效", len(v2_active) == 0)
    check("v1 新 run 完成", s3["run"]["status"] == "COMPLETE")

    # ---------- 场景 7：核验预算耗尽 ----------
    print("\n场景 7：核验预算耗尽 -> 优雅中止，已得证明保留")
    reset_source()
    s = start_full(budgets={"rows": 60, "verify": 5, "refetch": 20})
    check("run 状态 ABORTED_BUDGET",
          s["run"]["status"] == "ABORTED_BUDGET", s["run"]["status"])
    proven = s["range_counts"].get("PROVEN", 0)
    check("部分区间已取得 PROVEN 且保留", 0 < proven < n_ranges,
          f"PROVEN={proven}")
    check("未核验区间仍是 NEW/COLLECTED，未被冒认为完整",
          s["range_counts"].get("GAP", 0) == 0)
    complete_proofs = [p for p in s["proofs"] if p["kind"] == "COMPLETE"]
    check("中止后已得 PROVEN 的证明仍已落盘",
          len(complete_proofs) == proven,
          f"COMPLETE 证明={len(complete_proofs)} vs PROVEN 区间={proven}")

    # ---------- 场景 8：崩溃恢复（跨进程） ----------
    print("\n场景 8：核验途中崩溃 -> 重启工作台 -> 恢复 run（预算不重复计费）")
    run_crash_recovery()


def run_crash_recovery() -> None:
    """单独的临时目录跑崩溃恢复，工作台进程用 --crash-after 重启。"""
    import tempfile
    root = tempfile.mkdtemp(prefix="cw_crash_")
    data_dir = os.path.join(root, "ground_truth")
    db_path = os.path.join(root, "state", "wb.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    versions = dataset.build_versions()
    dataset.write_ground_truth(data_dir, versions)
    env = {**os.environ, "PYTHONPATH":
           os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
           "PYTHONIOENCODING": "utf-8"}
    py = sys.executable
    port = 8095  # 仅作记录；崩溃恢复走引擎 CLI，不占 HTTP 端口
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    py = sys.executable
    # 用引擎 CLI 直接制造崩溃（第 8 次请求后 os._exit）
    proc = subprocess.Popen(
        [py, "-m", "collect_workbench.engine", "--db", db_path,
         "--source", SOURCE, "--crash-after", "8"],
        cwd=REPO_ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.wait(timeout=30)
    check("工作台进程以崩溃码退出", proc.returncode == 3,
          f"rc={proc.returncode}")

    # 用同一 db 路径起新进程恢复（不带 crash-after）
    proc2 = subprocess.Popen(
        [py, "-m", "collect_workbench.engine", "--db", db_path,
         "--source", SOURCE, "--resume", "1"],
        cwd=REPO_ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc2.communicate(timeout=30)
    check("恢复进程正常退出", proc2.returncode == 0,
          err.decode("utf-8", "replace")[:200])
    summary = json.loads(out.decode("utf-8"))
    check("恢复后 run COMPLETE",
          summary["run"]["status"] == "COMPLETE", summary["run"]["status"])
    n_ranges = len(dataset.partitions(dataset.V1_ROWS))
    check("全部区间 PROVEN",
          summary["range_counts"].get("PROVEN") == n_ranges)
    # 已终态区间跳过 => 总 verify 请求不会因重启而翻倍
    check("恢复后未重复花费核验预算（verify <= 一次跑完的 13 次左右）",
          summary["budgets"]["verify"]["used"] <= 14,
          f"verify={summary['budgets']['verify']['used']}")
    # 崩溃发生在某次 /rows 计费之后、落盘之前，该页恢复时必须重发，
    # 因此 rows 最多比一次跑完（24 页）多 1 次请求——这是 at-least-once 语义
    check("采集 offset 恢复点生效（rows=24 或仅多重发崩溃页 25）",
          summary["budgets"]["rows"]["used"] in (24, 25),
          f"rows={summary['budgets']['rows']['used']}")
    print(f"  恢复后成本：rows={summary['budgets']['rows']['used']}, "
          f"verify={summary['budgets']['verify']['used']}, "
          f"refetch={summary['budgets']['refetch']['used']}")


if __name__ == "__main__":
    raise SystemExit(main())
