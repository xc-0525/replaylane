"""显存账本：一个带预算的信号量，不是通用调度器。

为什么准入必须和 gpu_ready 绑在一起：如果准入通过就让人抢，会出现"显存批下来了
但没抢到"或"抢到了但显存没批"，两种都会让账本和现实脱节。所以准入成功的那一个
事务里顺手把 step_run.gpu_ready 置 1，领取 SQL 用它过滤。
"""
from __future__ import annotations

import uuid

from .db import new_id, now_iso


def _gpus(store) -> list:
    return store.q("SELECT gpu_id, total_mb, headroom FROM gpu ORDER BY gpu_id")


def admit(store, step_run_id: str, need_mb: int, mode: str, token: int | None) -> int | None:
    """成功返回 gpu_id，失败返回 None。整个过程在一个写事务里，靠单写者保证原子。"""
    if need_mb <= 0:
        with store.tx() as c:
            store.cas(c, "UPDATE step_run SET gpu_ready=1 WHERE step_run_id=?", (step_run_id,))
        return 0

    with store.tx() as c:
        for g in _gpus(store):
            used = c.execute(
                "SELECT COALESCE(SUM(vram_mb),0) FROM gpu_lease"
                " WHERE gpu_id=? AND released_at IS NULL", (g["gpu_id"],)
            ).fetchone()[0]
            budget = int(g["total_mb"] * g["headroom"])
            ok = (used == 0) if mode == "exclusive" else (used + need_mb <= budget)
            if not ok:
                continue
            c.execute(
                "INSERT INTO gpu_lease(gpu_lease_id, gpu_id, step_run_id, worker_id,"
                " fencing_token, vram_mb, mode, acquired_at) VALUES(?,?,?,?,?,?,?,?)",
                (new_id("G"), g["gpu_id"], step_run_id, None, token, need_mb, mode, now_iso()),
            )
            store.cas(c, "UPDATE step_run SET gpu_ready=1 WHERE step_run_id=?", (step_run_id,))
            task_id = c.execute("SELECT task_id FROM step_run WHERE step_run_id=?",
                                (step_run_id,)).fetchone()[0]
            store.event(c, "gpu.granted", task_id=task_id, step_run_id=step_run_id,
                        payload={"gpu_id": g["gpu_id"], "vram_mb": need_mb, "mode": mode})
            return g["gpu_id"]
    return None


def release(store, step_run_id: str, reason: str = "released") -> None:
    """幂等：重复调用只影响 0 行。

    注意 gpu_ready 只在"确实释放了一行租约"时才清零。无脑清零会把不需要显存的
    步骤也一起挡在队列里（它们本来就靠 gpu_ready=1 才能被领取）。
    """
    with store.tx() as c:
        n = c.execute(
            "UPDATE gpu_lease SET released_at=? WHERE step_run_id=? AND released_at IS NULL",
            (now_iso(), step_run_id),
        ).rowcount
        if n:
            c.execute("UPDATE step_run SET gpu_ready=0 WHERE step_run_id=?", (step_run_id,))
            task_id = c.execute("SELECT task_id FROM step_run WHERE step_run_id=?",
                                (step_run_id,)).fetchone()[0]
            store.event(c, "gpu.released", task_id=task_id, step_run_id=step_run_id,
                        payload={"reason": reason})


def used_mb(store, gpu_id: int | None = None) -> int:
    if gpu_id is None:
        return store.scalar(
            "SELECT COALESCE(SUM(vram_mb),0) FROM gpu_lease WHERE released_at IS NULL") or 0
    return store.scalar(
        "SELECT COALESCE(SUM(vram_mb),0) FROM gpu_lease WHERE gpu_id=? AND released_at IS NULL",
        (gpu_id,)) or 0


def orphan_cleanup(store) -> int:
    """孤儿清理：断电后 worker 本身不存在，所以不能按 worker 判，只能按 token。

    判据：这条 gpu_lease 的 token 落后于 step_run 的当前 token，或它对应的
    step_run 已经不是 RUNNING —— 两种都说明持有者早就失去执行权了。
    """
    rows = store.q(
        "SELECT gl.gpu_lease_id, gl.step_run_id, gl.fencing_token, sr.fencing_token AS cur,"
        "       sr.status"
        "  FROM gpu_lease gl JOIN step_run sr ON sr.step_run_id = gl.step_run_id"
        " WHERE gl.released_at IS NULL"
    )
    n = 0
    for r in rows:
        stale_token = (r["fencing_token"] is not None and r["cur"] is not None
                       and r["fencing_token"] < r["cur"])
        if stale_token or r["status"] != "RUNNING":
            release(store, r["step_run_id"], reason="orphan")
            n += 1
    return n
