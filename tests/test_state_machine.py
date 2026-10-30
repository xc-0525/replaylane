"""状态机与任务状态推导。不依赖数据库，最快的一层。"""
from __future__ import annotations

import unittest

from replaylane.model import (STEP_STATUSES, STEP_TERMINAL, STEP_TRANSITIONS,
                              IllegalTransition, check_step_transition,
                              derive_task_status)


class TestStepTransitions(unittest.TestCase):
    def test_every_status_has_an_entry(self):
        for s in STEP_STATUSES:
            self.assertIn(s, STEP_TRANSITIONS, f"{s} 没有转移表")

    def test_no_transition_to_unknown_status(self):
        for src, dsts in STEP_TRANSITIONS.items():
            for d in dsts:
                self.assertIn(d, STEP_STATUSES, f"{src} -> {d} 是未定义状态")

    def test_terminal_states_are_strictly_immutable_except_failed(self):
        self.assertEqual(STEP_TRANSITIONS["SUCCEEDED"], frozenset())
        self.assertEqual(STEP_TRANSITIONS["CACHED"], frozenset())
        self.assertEqual(STEP_TRANSITIONS["CANCELLED"], frozenset())
        # FAILED 是唯一可以被人工重试拉回队列的终态
        self.assertEqual(STEP_TRANSITIONS["FAILED"], frozenset({"QUEUED"}))

    def test_every_non_terminal_can_reach_cancelled(self):
        """取消必须有出口：否则 CANCELLING 永远等不到"全部退出"。"""
        for s in STEP_STATUSES:
            if s in STEP_TERMINAL:
                continue
            self.assertIn("CANCELLED", STEP_TRANSITIONS[s], f"{s} 无法被取消")

    def test_illegal_transition_raises(self):
        with self.assertRaises(IllegalTransition):
            check_step_transition("SUCCEEDED", "RUNNING")
        with self.assertRaises(IllegalTransition):
            check_step_transition("QUEUED", "SUCCEEDED")   # 没跑过不可能成功
        check_step_transition("QUEUED", "RUNNING")


class TestTaskDerivation(unittest.TestCase):
    def test_all_cached_goes_straight_to_succeeded(self):
        """全缓存命中的任务不该卡在 QUEUED。

        这是评审里发现的一个真实漏洞：如果 QUEUED -> RUNNING 要求"有步骤入队"，
        那么零执行的任务永远没有步骤入队，也就永远到不了 SUCCEEDED。
        """
        got = derive_task_status(["CACHED"] * 8, cancel_requested=False)
        self.assertEqual(got, "SUCCEEDED")

    def test_all_blocked_is_queued(self):
        self.assertEqual(derive_task_status(["BLOCKED"] * 8, False), "QUEUED")

    def test_any_movement_makes_it_running(self):
        self.assertEqual(derive_task_status(["BLOCKED", "READY", "BLOCKED"], False), "RUNNING")
        self.assertEqual(derive_task_status(["SUCCEEDED", "RUNNING"], False), "RUNNING")

    def test_failure_wins_over_success(self):
        self.assertEqual(derive_task_status(["SUCCEEDED", "FAILED", "CANCELLED"], False), "FAILED")

    def test_cancel_requested_mid_flight(self):
        self.assertEqual(derive_task_status(["SUCCEEDED", "RUNNING"], True), "CANCELLING")
        self.assertEqual(derive_task_status(["SUCCEEDED", "CANCELLED"], True), "CANCELLED")

    def test_failed_step_does_not_finalize_until_downstream_is_cancelled(self):
        """有步骤 FAILED 时任务还不是终态：下游要先被 cascade 取消，然后才收敛到 FAILED。

        如果这里直接返回 FAILED，下游那些 BLOCKED 步骤就永远没有"离开 BLOCKED"的
        机会，任务状态和步骤状态会互相矛盾。
        """
        self.assertEqual(derive_task_status(["SUCCEEDED", "FAILED", "BLOCKED"], False), "RUNNING")
        self.assertEqual(derive_task_status(["SUCCEEDED", "FAILED", "CANCELLED"], False), "FAILED")


if __name__ == "__main__":
    unittest.main()
