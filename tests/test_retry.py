"""重试策略：分类表与步骤预设冲突时取严；OOM 走降级而不是重试。"""
from __future__ import annotations

import unittest

from replaylane.model import Failure, decide

from .helpers import Cluster, attempts_of, steps_of


class TestDecideRules(unittest.TestCase):
    """纯函数层：不跑数据库，把裁决规则钉死。"""

    def test_never_preset_refuses_even_429(self):
        """restyle 的 retry=never 意味着白名单为空：429 也不重试。"""
        self.assertEqual(decide("external_429", "never", 0, 0)[0], "fail")

    def test_transient_retries_up_to_budget(self):
        self.assertEqual(decide("transient", "fast", 0, 0), ("retry", 1))
        self.assertEqual(decide("transient", "fast", 0, 8)[0], "fail")   # 预算用尽

    def test_oom_degrades_not_retries_even_when_never(self):
        """OOM 不是"再试一次"，是"换个跑法"，所以不受 never 的限制。"""
        self.assertEqual(decide("oom", "never", 0, 0), ("degrade", "bs_half"))
        self.assertEqual(decide("oom", "never", 3, 5)[0], "attention")

    def test_deterministic_failure_never_retries(self):
        self.assertEqual(decide("invalid_input", "fast", 0, 0)[0], "fail")


class TestRetryIntegration(unittest.TestCase):
    def test_two_transient_failures_then_success(self):
        c = Cluster(workers=1, time_scale="0.03").start()
        try:
            tid, st = c.run({"source": "retry_test.mp4",
                             "faults": {"matting": {"fail_times": 2, "class": "transient"}}},
                            timeout=120)
            self.assertEqual(st, "SUCCEEDED")
            atts = attempts_of(c.store, tid, "matting")
            self.assertEqual(len(atts), 3)
            self.assertEqual([a["status"] for a in atts], ["FAILED", "FAILED", "SUCCEEDED"])
            self.assertTrue(all(a["variant"] == "base" for a in atts),
                            "可重试失败不该触发降级")
            kinds = [r["type"] for r in c.store.q(
                "SELECT type FROM event WHERE task_id=? AND type LIKE 'step.%'", (tid,))]
            self.assertEqual(kinds.count("step.retrying"), 2)
        finally:
            c.close()

    def test_oom_degrades_variant_and_succeeds(self):
        c = Cluster(workers=1, time_scale="0.03").start()
        try:
            tid, st = c.run({"source": "oom_test.mp4",
                             "faults": {"matting": {"fail_times": 1, "class": "oom"}}},
                            timeout=120)
            self.assertEqual(st, "SUCCEEDED")
            atts = attempts_of(c.store, tid, "matting")
            self.assertEqual([a["variant"] for a in atts], ["base", "bs_half"])
            # 降级参数不进 step_key：同一个 step_run，key 不变
            keys = {r["step_key"] for r in c.store.q(
                "SELECT step_key FROM step_run WHERE task_id=?", (tid,))}
            self.assertEqual(len(keys), 8, "每个步骤一个 key，降级不该产生新 key")
        finally:
            c.close()


if __name__ == "__main__":
    unittest.main()
