"""sweeper：回收过期租约、检测卡住、清理产物。

它是"恢复不需要特殊分支"这一主张的支点：进程被 kill 之后，没有任何人写任何
东西，系统靠这里的 last_beat 判定它死了，然后把 step_run 放回 QUEUED——
接下来走的就是正常调度路径，和第一次执行没有区别。
"""
from __future__ import annotations

import os
import time

from . import gpu
from .db import age_s, now_iso
from .pipeline import STEP_BY_NAME

TTL_S = float(os.environ.get("REPLAYLANE_LEASE_TTL_S", "15"))
GC_AGE_S = float(os.environ.get("REPLAYLANE_GC_AGE_S", "86400"))


def reclaim_expired(store, ttl_s: float = TTL_S) -> int:
    rows = store.q("SELECT * FROM lease WHERE closed_at IS NULL")
    killed = 0
    for lease in rows:
        if age_s(lease["last_beat"]) <= ttl_s:
            continue
        sr = store.q1("SELECT * FROM step_run WHERE step_run_id=?", (lease["step_run_id"],))
        if sr is None:
            continue
        cancel = store.scalar("SELECT cancel_requested FROM task WHERE task_id=?",
                              (sr["task_id"],)) or 0
        target = "CANCELLED" if cancel else "QUEUED"
        now = now_iso()
        with store.tx() as c:
            n = store.cas(c, "UPDATE lease SET closed_at=?, close_reason='expired'"
                             " WHERE lease_id=? AND closed_at IS NULL", (now, lease["lease_id"]))
            if not n:                      # 别人先关的，跳过
                continue
            # attempt 的 ended_at 故意留 NULL：kill -9 之后我们确实不知道它几点死的
            c.execute(
                "UPDATE attempt SET status='ABANDONED', observed_at=?"
                " WHERE step_run_id=? AND status='RUNNING'", (now, lease["step_run_id"]))
            moved = store.cas(
                c,
                "UPDATE step_run SET status=?, lease_id=NULL, requeued_at=?,"
                " last_progress_at=?, error_class='lease_expired'"
                " WHERE step_run_id=? AND status='RUNNING' AND lease_id=?",
                (target, now, now, lease["step_run_id"], lease["lease_id"]),
            )
            if not moved:
                continue
            store.event(c, "lease.expired", task_id=sr["task_id"],
                        step_run_id=sr["step_run_id"],
                        payload={"lease_id": lease["lease_id"],
                                 "worker": lease["worker_id"],
                                 "fencing_token": lease["fencing_token"],
                                 "ttl_s": ttl_s,
                                 # 从"最后一次心跳"到"被判定死亡"隔了多久。
                                 # 没有这个数，甘特图只能把这段死寂画成执行中。
                                 "silent_ms": int(age_s(lease["last_beat"]) * 1000),
                                 "action": "requeued" if target == "QUEUED" else "cancelled"})
            # 状态变化必须同时有一条 step 级事件，否则投影重建不出来。
            # lease.expired 说明"为什么"，这条说明"变成了什么"。
            store.event(c, "step.requeued" if target == "QUEUED" else "step.cancelled",
                        task_id=sr["task_id"], step_run_id=sr["step_run_id"],
                        payload={"step": sr["step_name"], "reason": "lease_expired"})
            killed += 1
        # 显存必须跟着还：否则一次 kill -9 会让这张卡永远显示"已占用"
        gpu.release(store, lease["step_run_id"], "lease_expired")
        # 还完再重新申请：排队等的就是这次准入，抢不到就等下轮 sweeper/scheduler
        if target == "QUEUED" and sr["gpu_mb"]:
            step = STEP_BY_NAME.get(sr["step_name"])
            gpu.admit(store, sr["step_run_id"], sr["gpu_mb"],
                      step.gpu_mode if step else "shared", sr["fencing_token"])
    return killed


def detect_stalled(store) -> int:
    """心跳正常但进度不动 = 卡住。这是小时级任务唯一能发现"假装在跑"的手段。"""
    moved = 0
    for r in store.q("SELECT * FROM step_run WHERE status='RUNNING'"):
        if not r["last_progress_at"]:
            continue
        if age_s(r["last_progress_at"]) * 1000 <= r["stall_after_ms"]:
            continue
        with store.tx() as c:
            n = store.cas(
                c,
                "UPDATE step_run SET status='ATTENTION', error_class='stalled',"
                " blocked_reason='progress_stalled'"
                " WHERE step_run_id=? AND status='RUNNING' AND lease_id IS ?",
                (r["step_run_id"], r["lease_id"]),
            )
            if n:
                store.event(c, "step.attention", task_id=r["task_id"],
                            step_run_id=r["step_run_id"],
                            payload={"step": r["step_name"], "class": "stalled",
                                     "last_progress_ago_ms":
                                         int(age_s(r["last_progress_at"]) * 1000)})
                moved += 1
    return moved


def gc_artifacts(store, age_s_threshold: float = GC_AGE_S) -> int:
    """删产物必须同时看两种引用。

    只看 artifact_ref 会删掉仍在 step_cache 里的产物——任务一 GC，缓存索引就成了
    指向空气的指针，下一个任务命中缓存时拿到一个不存在的路径。这是 A9 的反面。
    """
    rows = store.q(
        "SELECT a.sha256, a.rel_path FROM artifact a"
        " WHERE NOT EXISTS(SELECT 1 FROM artifact_ref r WHERE r.sha256=a.sha256)"
        "   AND NOT EXISTS(SELECT 1 FROM step_cache c WHERE c.sha256=a.sha256)"
        "   AND COALESCE(a.last_access_at, a.created_at) < ?",
        (time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(time.time() - age_s_threshold)) + ".000Z",),
    )
    n = 0
    for r in rows:
        path = os.path.join(store.artifacts_dir, r["rel_path"])
        with store.tx() as c:
            c.execute("DELETE FROM artifact WHERE sha256=?", (r["sha256"],))
            store.event(c, "artifact.gc", payload={"sha256": r["sha256"][:16]})
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            pass          # 删不掉就留着，下一轮再来 —— 事务已经提交，不会回滚
        n += 1
    return n


def loop(store, interval_s: float = 3.0, max_ticks: int | None = None):
    i = 0
    while max_ticks is None or i < max_ticks:
        r = {"reclaimed": reclaim_expired(store), "stalled": detect_stalled(store)}
        if i % 20 == 0:
            r["gc"] = gc_artifacts(store)
            r["gpu_orphans"] = gpu.orphan_cleanup(store)
        if any(r.values()):
            print("[sweeper]", r, flush=True)
        i += 1
        time.sleep(interval_s)
