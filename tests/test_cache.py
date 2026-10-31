"""结果复用：相同输入第二次提交必须零执行。"""
from __future__ import annotations

import unittest

from .helpers import Cluster, steps_of


class TestCacheReuse(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = Cluster(workers=1, time_scale="0.03").start()

    @classmethod
    def tearDownClass(cls):
        cls.c.close()

    def test_second_identical_task_reuses_all_computational_steps(self):
        t1, st1 = self.c.run({"source": "cache_test.mp4"})
        self.assertEqual(st1, "SUCCEEDED", "第一个任务没跑成功，后面的断言没有意义")
        attempts_after_first = self.c.store.scalar("SELECT COUNT(*) FROM attempt")

        t2, st2 = self.c.run({"source": "cache_test.mp4"})
        self.assertEqual(st2, "SUCCEEDED")

        steps = steps_of(self.c.store, t2)
        self.assertEqual(len(steps), 8)
        # 会耗算力的 7 步必须全部复用
        for name in ("probe", "shot_split", "frames", "matting",
                     "restyle", "temporal", "encode"):
            self.assertEqual(steps[name]["status"], "CACHED", f"{name} 没有命中缓存")
        # 人工审核不参与缓存：人看过并通过是针对"这一次交付"的判断，
        # 复用等于让新交付免检。所以它每次都要重新走一遍。
        self.assertEqual(steps["qc_review"]["status"], "SUCCEEDED")
        # 因此 attempt 表只会多出"人工审核"这一行
        self.assertEqual(self.c.store.scalar("SELECT COUNT(*) FROM attempt"),
                         attempts_after_first + 1)

    def test_different_seed_breaks_the_cache_chain(self):
        """换 seed 等于换 key：从 restyle 开始必须重新执行，之前的仍可复用。

        注意这里是先跑一个基线任务，再跑换 seed 的——否则"未命中"只是因为它
        是第一次执行，证明不了任何关于 seed 的事。
        """
        base, st0 = self.c.run({"source": "seed_test.mp4"})
        self.assertEqual(st0, "SUCCEEDED")

        t3, st3 = self.c.run({"source": "seed_test.mp4", "nondet_seed": 999})
        self.assertEqual(st3, "SUCCEEDED")
        steps = steps_of(self.c.store, t3)
        for name in ("probe", "shot_split", "frames", "matting"):
            self.assertEqual(steps[name]["status"], "CACHED", f"{name} 本应复用")
        for name in ("restyle", "temporal", "encode", "qc_review"):
            self.assertEqual(steps[name]["status"], "SUCCEEDED", f"{name} 不该命中缓存")


if __name__ == "__main__":
    unittest.main()
