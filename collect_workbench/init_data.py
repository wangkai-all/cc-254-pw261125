"""生成基准数据文件（v1/v2）到指定目录。"""

from __future__ import annotations

import argparse
from typing import Optional

from . import dataset


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="生成 ground truth 基准数据")
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    versions = dataset.build_versions()
    dataset.write_ground_truth(args.out, versions)
    for name, doc in versions.items():
        print(f"{name}: {len(doc['rows'])} rows -> {args.out}/{name}.json")
    print(f"range_size={dataset.RANGE_SIZE}, page_size={dataset.PAGE_SIZE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
