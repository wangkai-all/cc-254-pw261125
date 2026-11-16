"""独立基准。

与来源服务彻底隔离：直接读取基准目录下的 ground truth JSON 文件，
不发起任何 HTTP 请求，不经过采集端存储。用途是对工作台的结论做独立对照：
- COMPLETE 区间：应与基准逐行一致；
- SAMPLED 区间：允许存在未被发现的差异，基准会如实列出，说明"抽检不等于完整"；
- GAP 区间：基准给出权威缺口清单，核验工作台报告的 missing_ids 必须与之相符。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import dataset
from .storage import Storage


class Baseline:
    def __init__(self, ground_truth_dir: str):
        self.ground_truth_dir = ground_truth_dir
        self._docs: Dict[str, dict] = {}

    def doc(self, version: str) -> dict:
        if version not in self._docs:
            self._docs[version] = dataset.load_version(
                self.ground_truth_dir, version)
        return self._docs[version]

    def expected(self, version: str, lo: int, hi: int) -> dict:
        rows = dataset.slice_rows(self.doc(version)["rows"], lo, hi)
        return {"count": len(rows),
                "digest": dataset.range_digest(rows),
                "ids": [r["id"] for r in rows]}

    def compare_run(self, db: Storage, run_id: int) -> Dict[str, Any]:
        """把一个 run 的全部区间与独立基准逐区间对照。"""
        run = db.get_run(run_id)
        if not run or not run["pinned_version"]:
            raise RuntimeError(f"run {run_id} 不存在或尚未钉选版本")
        version = run["pinned_version"]
        results: List[dict] = []
        mismatch_ranges = 0
        for r in db.list_ranges(run_id):
            lo, hi = r["lo"], r["hi"]
            exp = self.expected(version, lo, hi)
            local_rows = db.get_rows(run_id, lo, hi)
            actual_ids = sorted(row["id"] for row in local_rows)
            actual_digest = dataset.range_digest(local_rows)
            missing = [i for i in exp["ids"] if i not in set(actual_ids)]
            extra = [i for i in actual_ids if i not in set(exp["ids"])]
            content_match = actual_digest == exp["digest"]
            consistent = (not missing and not extra and content_match
                          and len(actual_ids) == exp["count"])
            # 工作台自己声称的结论是否与独立基准一致
            claimed_gap = r["state"] == "GAP"
            claim_correct = (consistent and r["state"] in
                             ("PROVEN", "SAMPLED_OK")) or \
                            (not consistent and claimed_gap)
            if not consistent:
                mismatch_ranges += 1
            results.append({
                "lo": lo, "hi": hi,
                "range_state": r["state"],
                "baseline_count": exp["count"],
                "local_count": len(actual_ids),
                "missing_ids": missing,
                "extra_ids": extra,
                "content_match": content_match,
                "consistent": consistent,
                "claim_correct": claim_correct,
            })
        return {
            "run_id": run_id,
            "pinned_version": version,
            "total_ranges": len(results),
            "mismatch_ranges": mismatch_ranges,
            "all_claims_correct": all(x["claim_correct"] for x in results),
            "ranges": results,
        }

    def compare_proof(self, db: Storage, proof_id: int) -> Optional[dict]:
        proofs = [p for p in db.list_proofs() if p["proof_id"] == proof_id]
        if not proofs:
            return None
        p = proofs[0]
        exp = self.expected(p["version"], p["lo"], p["hi"])
        return {"proof_id": proof_id, "kind": p["kind"],
                "status": p["status"], "version": p["version"],
                "baseline": {"count": exp["count"],
                             "digest": exp["digest"]}}
