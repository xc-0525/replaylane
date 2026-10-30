"""事件重放校验：投影能不能被事件表重建出来。

这是"event 是真源"这句话唯一的检验方式。如果重放结果和投影不一致，说明有人
绕过事件直接改了状态——那就不是事件溯源，只是"一边写状态一边顺手记日志"。

刻意不重建的东西（见 docs/01 §5.3）：lease、gpu_lease、fencing_token、gpu_ready。
它们是运行态，不是业务状态；重放它们既无意义也不安全（正在跑的 worker 会因为
租约行消失而心跳失败）。
"""
from __future__ import annotations

import sys

from .db import Store, loads
from .model import derive_task_status

# 事件 -> 该步骤重放后的状态
STEP_EVENTS = {
    "step.ready": "READY",
    "step.queued": "QUEUED",
    "step.requeued": "QUEUED",
    "step.claimed": "RUNNING",
    "step.retrying": "RETRY_WAIT",
    "step.attention": "ATTENTION",
    "step.succeeded": "SUCCEEDED",
    "step.cached": "CACHED",
    "step.failed": "FAILED",
    "step.cancelled": "CANCELLED",
    "step.manual_retry": "QUEUED",
}


def replay_task(store: Store, task_id: str) -> dict[str, str]:
    state: dict[str, str] = {}
    rows = store.q(
        "SELECT seq, type, step_run_id, payload_json FROM event"
        " WHERE task_id=? ORDER BY seq", (task_id,))
    for r in rows:
        target = STEP_EVENTS.get(r["type"])
        if target is None or not r["step_run_id"]:
            continue
        state[r["step_run_id"]] = target
    return state


def check_task(store: Store, task_id: str) -> list[dict]:
    problems: list[dict] = []
    replayed = replay_task(store, task_id)
    rows = store.q("SELECT step_run_id, step_name, status FROM step_run WHERE task_id=?",
                   (task_id,))
    for r in rows:
        want = replayed.get(r["step_run_id"], "BLOCKED")
        if want != r["status"]:
            problems.append({"kind": "step", "step": r["step_name"],
                             "projection": r["status"], "replayed": want})

    # task 状态必须是"从步骤状态推导"出来的，而不是被谁直接写的
    statuses = [replayed.get(r["step_run_id"], "BLOCKED") for r in rows]
    cancel = store.scalar("SELECT cancel_requested FROM task WHERE task_id=?", (task_id,))
    task_status = store.scalar("SELECT status FROM task WHERE task_id=?", (task_id,))
    expect = derive_task_status(statuses, bool(cancel))
    # CANCELLING 是过程态，重放到终态时按 CANCELLED 比
    if expect != task_status and not (task_status == "CANCELLED" and expect == "CANCELLING"):
        problems.append({"kind": "task", "projection": task_status, "replayed": expect})

    # 事件流本身：seq 必须严格递增（回滚可能留空洞，但不许重复/倒流）
    seqs = [r["seq"] for r in store.q(
        "SELECT seq FROM event WHERE task_id=? ORDER BY seq", (task_id,))]
    if seqs != sorted(set(seqs)):
        problems.append({"kind": "event_order", "detail": "seq 非严格递增"})
    return problems


def main(argv=None) -> int:
    import argparse
    p = argparse.ArgumentParser(prog="replaylane.verify")
    p.add_argument("--db", required=True)
    p.add_argument("--artifacts", required=True)
    p.add_argument("--task", default=None, help="只校验某个任务")
    a = p.parse_args(argv)

    store = Store(a.db, a.artifacts)
    tasks = ([a.task] if a.task
             else [r["task_id"] for r in store.q("SELECT task_id FROM task")])
    bad = 0
    for tid in tasks:
        problems = check_task(store, tid)
        if problems:
            bad += 1
            print(f"✗ {tid}")
            for pr in problems:
                print("   ", pr)
    print(f"\nchecked {len(tasks)} task(s), {bad} inconsistent")
    return 0 if bad == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
