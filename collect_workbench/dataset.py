"""基准数据与摘要协议。

关键约束：
1. 数据行的规范化字节序列 (canonical_bytes) 是唯一的"内容指纹"口径，
   来源端的 /digest 与核验端本地重算必须使用完全相同的实现。
2. 区间按行 id 左闭右开划分：[lo, hi)，id 在数据版本内连续。
3. v1 / v2 两个版本同时落盘为基准文件，来源服务与独立基准各自读取，
   基准文件是唯一的事实来源 (ground truth)。
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Dict, List, Tuple

PAGE_SIZE = 50      # 分页读取接口的页大小
RANGE_SIZE = 200    # 核验区间大小（200 行/区间，1200 行 => 6 个区间）

V1_NAME = "v1"
V2_NAME = "v2"
V1_ROWS = 1200
V2_APPEND = 120     # v2 在 v1 基础上追加的行数
V2_REV_EVERY = 7    # v2 中每隔多少行修订一次 rev（内容变化，摘要随之变化）


def canonical_bytes(row: dict) -> bytes:
    """单行的规范化字节：键排序、无多余空白，保证跨进程字节一致。"""
    return json.dumps(row, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":")).encode("utf-8")


def range_digest(rows: List[dict]) -> str:
    """对一组行计算区间内容摘要。

    每行先写入长度前缀再写规范化字节，避免行之间的边界歧义。
    行顺序固定按 id 升序，调用方负责排序。
    """
    h = hashlib.sha256()
    for row in rows:
        b = canonical_bytes(row)
        h.update(str(len(b)).encode("ascii"))
        h.update(b"\n")
        h.update(b)
    return h.hexdigest()


def _payload_for(row_id: int) -> str:
    """由 id 确定性生成的负载，跨进程稳定，不依赖随机种子。"""
    seed = hashlib.sha256(f"row-{row_id}".encode("utf-8")).hexdigest()
    return f"payload-{seed[:16]}"


def build_versions() -> Dict[str, dict]:
    """构造 v1 / v2 两版完整数据。

    v1: id 0..1199，rev=1
    v2: 保留全部 v1 行，其中 id%7==0 的行 rev 升为 2（模拟内容修订），
        并追加 id 1200..1319（模拟新增）。因此 v2 的数量与摘要均与 v1 不同。
    """
    v1_rows = [
        {"id": i, "rev": 1, "payload": _payload_for(i)}
        for i in range(V1_ROWS)
    ]
    v2_rows = []
    for row in v1_rows:
        if row["id"] % V2_REV_EVERY == 0:
            v2_rows.append({**row, "rev": 2})
        else:
            v2_rows.append(dict(row))
    next_id = V1_ROWS
    for _ in range(V2_APPEND):
        v2_rows.append({"id": next_id, "rev": 2,
                        "payload": _payload_for(next_id)})
        next_id += 1

    return {
        V1_NAME: {"version": V1_NAME, "range_size": RANGE_SIZE,
                  "rows": v1_rows},
        V2_NAME: {"version": V2_NAME, "range_size": RANGE_SIZE,
                  "rows": v2_rows},
    }


def partitions(total: int, range_size: int = RANGE_SIZE) -> List[Tuple[int, int]]:
    """把 [0, total) 切成左闭右开区间列表。"""
    out = []
    lo = 0
    while lo < total:
        hi = min(lo + range_size, total)
        out.append((lo, hi))
        lo = hi
    return out


def slice_rows(rows: List[dict], lo: int, hi: int) -> List[dict]:
    """按 id 边界取 [lo, hi) 切片并排序（数据已按 id 连续，此处仍显式排序）。"""
    part = [r for r in rows if lo <= r["id"] < hi]
    part.sort(key=lambda r: r["id"])
    return part


def write_ground_truth(dirpath: str, versions: Dict[str, dict]) -> None:
    """把两版基准数据落盘，供来源服务与独立基准分别读取。"""
    os.makedirs(dirpath, exist_ok=True)
    for name, doc in versions.items():
        path = os.path.join(dirpath, f"{name}.json")
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False)
        os.replace(tmp, path)
    meta = {"range_size": RANGE_SIZE, "page_size": PAGE_SIZE,
            "versions": list(versions.keys())}
    with open(os.path.join(dirpath, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def load_version(dirpath: str, version: str) -> dict:
    with open(os.path.join(dirpath, f"{version}.json"), "r",
              encoding="utf-8") as f:
        return json.load(f)
