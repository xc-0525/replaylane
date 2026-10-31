"""人工闸门：需要人审的步骤必须真的停下来，直到有人点"通过"。

这条规则容易被做成"自动通过"，那样闸门就是假的——所以单独测。
"""
from __future__ import annotations

import time
import unittest

from replaylane import scheduler

from .helpers import Cluster, attempts_of, steps_of


class TestHumanGate(unittest.TestCase):
    def test_review_holds_task_until_approved(self):
        c = Cluster(workers=1, time_scale="0.03").start()
        try:
            tid = scheduler.submit(c.store, {"source": "gate.mp4", "auto_approve": False})

            deadline = time.time() + 60
            status = None
            while time.time() < deadline:
                row = c.store.q1(
                    "SELECT status FROM step_run WHERE task_id=? AND step_name='qc_review'",
                    (tid,))
                status = row["status"] if row else None
                if status == "ATTENTION":
                    break
                time.sleep(0.1)
            self.assertEqual(status, "ATTENTION", "人工步骤没有停下来等人")

            # 任务不能自己结束
            self.assertNotIn(c.store.scalar("SELECT status FROM task WHERE task_id=?", (tid,)),
                             ("SUCCEEDED", "FAILED", "CANCELLED"))

            # 挂起不是失败：attempt 必须是 SUSPENDED，否则失败率统计会被污染
            atts = attempts_of(c.store, tid, "qc_review")
            self.assertEqual(len(atts), 1)
            self.assertEqual(atts[0]["status"], "SUSPENDED")
            self.assertIsNotNone(atts[0]["ended_at"])

            # 人通过之后，这一步直接算完成，不再让机器跑一遍
            self.assertTrue(scheduler.approve(c.store, tid, "qc_review", reviewer="tester"))

            deadline = time.time() + 30
            while time.time() < deadline:
                if c.store.scalar("SELECT status FROM task WHERE task_id=?", (tid,)) == "SUCCEEDED":
                    break
                time.sleep(0.1)
            self.assertEqual(c.store.scalar("SELECT status FROM task WHERE task_id=?", (tid,)),
                             "SUCCEEDED")

            step = steps_of(c.store, tid)["qc_review"]
            self.assertEqual(step["status"], "SUCCEEDED")
            atts = attempts_of(c.store, tid, "qc_review")
            self.assertEqual([a["status"] for a in atts], ["SUSPENDED", "SUCCEEDED"])
            self.assertTrue(atts[1]["worker_id"].startswith("human:"),
                            "通过动作要留痕：这一轮是人做的")
        finally:
            c.close()

    def test_approve_rejected_when_not_waiting(self):
        c = Cluster(workers=1, time_scale="0.03").start()
        try:
            tid = scheduler.submit(c.store, {"source": "gate2.mp4", "auto_approve": True})
            deadline = time.time() + 60
            while time.time() < deadline:
                if c.store.scalar("SELECT status FROM task WHERE task_id=?", (tid,)) == "SUCCEEDED":
                    break
                time.sleep(0.1)
            self.assertFalse(scheduler.approve(c.store, tid, "qc_review"),
                             "已经结束的步骤不该还能被通过")
        finally:
            c.close()


if __name__ == "__main__":
    unittest.main()
