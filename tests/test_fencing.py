"""fencing token：僵尸 worker 的写入必须被挡掉（断言 A2）。

这个测试不跑 worker 进程，直接构造"旧 worker 复活"的场景——因为要证明的是
一条 SQL 的条件是否成立，而不是进程能不能被杀。
"""
from __future__ import annotations

import time
import unittest

from replaylane import scheduler, sweeper
from replaylane.db import init_db
from replaylane.worker import Worker
import tempfile, os


class TestFencing(unittest.TestCase):
    def setUp(self):
        d = tempfile.mkdtemp(prefix="rl_fence_")
        self.store = init_db(os.path.join(d, "t.db"), os.path.join(d, "art"))
        self.tid = scheduler.submit(self.store, {"source": "fence.mp4"})
        for _ in range(3):
            scheduler.tick(self.store)

    def test_stale_token_cannot_commit(self):
        w1 = Worker(self.store, "w1")
        a = w1.claim()
        self.assertIsNotNone(a, "第一个 worker 应该能领到 probe")
        self.assertEqual(a["fencing_token"], 1)

        # 让租约过期：等价于 w1 被 kill -9
        with self.store.tx() as c:
            c.execute("UPDATE lease SET last_beat='2000-01-01T00:00:00.000Z' WHERE lease_id=?",
                      (a["lease_id"],))
        self.assertEqual(sweeper.reclaim_expired(self.store, ttl_s=1), 1)

        w2 = Worker(self.store, "w2")
        b = w2.claim()
        self.assertIsNotNone(b)
        self.assertEqual(b["fencing_token"], 2, "重新领取必须换号")

        # 旧的 (lease, token) 组合：任何写入都必须落空
        with self.store.tx() as c:
            n = self.store.cas(
                c,
                "UPDATE step_run SET status='SUCCEEDED' WHERE step_run_id=?"
                " AND lease_id=? AND fencing_token=? AND status='RUNNING'",
                (a["step_run_id"], a["lease_id"], a["fencing_token"]),
            )
        self.assertEqual(n, 0, "僵尸提交本应被拒绝")

        # 断言 A1：一个 step_run 至多一个未关闭租约
        open_leases = self.store.scalar(
            "SELECT COUNT(*) FROM lease WHERE step_run_id=? AND closed_at IS NULL",
            (a["step_run_id"],))
        self.assertEqual(open_leases, 1)

    def test_partial_unique_index_blocks_double_lease(self):
        w = Worker(self.store, "w1")
        a = w.claim()
        with self.assertRaises(Exception):
            with self.store.tx() as c:
                c.execute(
                    "INSERT INTO lease(lease_id, step_run_id, worker_id, fencing_token,"
                    " acquired_at, last_beat) VALUES('dup', ?, 'w9', 99, ?, ?)",
                    (a["step_run_id"], "now", "now"))


if __name__ == "__main__":
    unittest.main()
