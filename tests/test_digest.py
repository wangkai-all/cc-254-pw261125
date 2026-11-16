"""摘要协议单元测试（无服务依赖）：

- 同一批行无论来源端还是核验端计算，摘要必须字节一致；
- 行内容修订（rev 变化）必然改变摘要；
- 长度前缀避免行边界拼接歧义。
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collect_workbench import dataset  # noqa: E402


class DigestProtocolTest(unittest.TestCase):
    def test_deterministic(self):
        rows = [{"id": 1, "rev": 1, "payload": "a"},
                {"id": 2, "rev": 1, "payload": "b"}]
        self.assertEqual(dataset.range_digest(rows),
                         dataset.range_digest([dict(r) for r in rows]))

    def test_order_matters(self):
        a = [{"id": 1, "rev": 1, "payload": "a"},
             {"id": 2, "rev": 1, "payload": "b"}]
        b = list(reversed(a))
        self.assertNotEqual(dataset.range_digest(a),
                            dataset.range_digest(b))

    def test_content_change_changes_digest(self):
        old = [{"id": 7, "rev": 1, "payload": "x"}]
        new = [{"id": 7, "rev": 2, "payload": "x"}]
        self.assertNotEqual(dataset.range_digest(old),
                            dataset.range_digest(new))

    def test_v1_v2_digests_differ_per_partition(self):
        versions = dataset.build_versions()
        lo, hi = 0, dataset.RANGE_SIZE
        d1 = dataset.range_digest(
            dataset.slice_rows(versions["v1"]["rows"], lo, hi))
        d2 = dataset.range_digest(
            dataset.slice_rows(versions["v2"]["rows"], lo, hi))
        self.assertNotEqual(d1, d2)

    def test_ground_truth_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            versions = dataset.build_versions()
            dataset.write_ground_truth(tmp, versions)
            loaded = dataset.load_version(tmp, "v1")
            self.assertEqual(
                dataset.range_digest(loaded["rows"]),
                dataset.range_digest(versions["v1"]["rows"]))

    def test_partitions(self):
        parts = dataset.partitions(1200, 200)
        self.assertEqual(parts[0], (0, 200))
        self.assertEqual(parts[-1], (1000, 1200))
        self.assertEqual(len(parts), 6)
        self.assertEqual(dataset.partitions(1230, 200)[-1], (1200, 1230))


if __name__ == "__main__":
    unittest.main(verbosity=2)
