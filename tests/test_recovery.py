"""kill -9 之后的自动恢复。这是整套设计最该被验证的一条。"""
from __future__ import annotations

import unittest

from .helpers import Cluster, attempts_of, steps_of


class TestRecovery(unittest.TestCase):
    def test_kill_mid_step_and_resume_from_checkpoint(self):
        c = Cluster(workers=2, time_scale="0.05", ttl_s=1.5).start()
        try:
            tid, st = c.run({"source": "kill_test.mp4",
                             "faults": {"restyle": {"kill_at": 0.45, "kill_on_attempt": 1}}},
                            timeout=120)
            self.assertEqual(st, "SUCCEEDED", "kill 之后任务没有恢复完成")

            step = steps_of(c.store, tid)["restyle"]
            atts = attempts_of(c.store, tid, "restyle")
            self.assertEqual(len(atts), 2, "应该正好重试一次")
            self.assertEqual(atts[0]["status"], "ABANDONED")
            # kill -9 之后我们不知道它几点死的：ended_at 必须为空，只记 observed_at
            self.assertIsNone(atts[0]["ended_at"])
            self.assertIsNotNone(atts[0]["observed_at"])
            self.assertIsNone(atts[0]["exit_code"])
            self.assertEqual(atts[1]["status"], "SUCCEEDED")

            # 执行权换过一次号：这是断言 A2 的前提
            self.assertEqual(step["fencing_token"], 2)

            # 已完成的分片被复用，没有被重算
            adopted = c.store.scalar(
                "SELECT COUNT(*) FROM checkpoint WHERE step_run_id=? AND attempt_no=1",
                (step["step_run_id"],))
            self.assertEqual(adopted, 6, "第 1 次 attempt 的 6 个分片应当被保留下来")

            # 租约表里必须留痕：关闭原因 = expired
            self.assertEqual(
                c.store.scalar("SELECT COUNT(*) FROM lease WHERE step_run_id=?"
                               " AND close_reason='expired'", (step["step_run_id"],)), 1)
        finally:
            c.close()


if __name__ == "__main__":
    unittest.main()
