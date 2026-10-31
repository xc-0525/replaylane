"""调度器：唯一决定「下一步该干什么」的地方。

四个动作，每个 tick 跑一遍：
  1. expand    BLOCKED → READY     （上游产物齐了，才能算 key）
  2. resolve   READY → CACHED/QUEUED（缓存命中就是零执行）
  3. wake      RETRY_WAIT → QUEUED  （退避到期）；QUEUED 但没批显存的再试准入
  4. project   task 状态由 step 状态推导（scheduler 是唯一汇总者）
"""
from __future__ import annotations

import hashlib
import json
import os
import time

from . import gpu
from .db import dumps, loads, new_id, now_iso
from .model import STEP_TERMINAL, check_task_transition, derive_task_status
from .pipeline import (DEFAULT_PARAMS, PIPELINE_NAME, PIPELINE_VERSION, STEP_BY_NAME,
                       STEPS, canonical_params, compute_step_key, ensure_specs)
from .steps import aggregate_digest


# ------------------------------------------------------------------ 提交

def submit(store, params: dict | None = None, priority: int = 5,
           seed: int | None = None) -> str:
    ensure_specs(store)
    params = dict(DEFAULT_PARAMS, **(params or {}))
    # seed 是任务输入的一部分，默认由语义参数哈希导出：
    #   相同参数 -> 相同 seed -> 非确定性步骤也能全量命中缓存（"再提交一次，0 秒"）
    #   想换一批画面 -> 显式给 seed -> 换了 key -> 重新生成
    # 这样「重试不变 seed、换 seed 等于新任务」就不是约定，而是结构上成立的。
    if seed is None:
        seed = params.get("nondet_seed")
    if seed is None:
        seed = int(hashlib.sha256(dumps(canonical_params(params)).encode()).hexdigest()[:8], 16)
    params["nondet_seed"] = seed

    task_id = new_id("T")
    with store.tx() as c:
        c.execute(
            "INSERT INTO task(task_id, pipeline, version, params_json, status, priority,"
            " submitted_at) VALUES(?,?,?,?,?,?,?)",
            (task_id, PIPELINE_NAME, PIPELINE_VERSION, dumps(params), "QUEUED",
             priority, now_iso()),
        )
        for s in STEPS:
            c.execute(
                "INSERT INTO step_run(step_run_id, task_id, step_name, status, params_json,"
                " spec_cost, priority, gpu_mb, stall_after_ms) VALUES(?,?,?,?,?,?,?,?,?)",
                (new_id("S"), task_id, s.name, "BLOCKED", dumps(params), s.cost,
                 priority, s.gpu_mb, 300_000),
            )
        store.event(c, "task.submitted", task_id=task_id,
                    payload={"pipeline": PIPELINE_NAME, "version": PIPELINE_VERSION,
                             "seed": seed, "priority": priority})
    return task_id


# ------------------------------------------------------------ 1. expand

def expand(store) -> int:
    moved = 0
    tasks = store.q("SELECT task_id, params_json FROM task WHERE status IN ('QUEUED','RUNNING')")
    for t in tasks:
        params = loads(t["params_json"], {})
        runs = {r["step_name"]: r for r in
                store.q("SELECT * FROM step_run WHERE task_id=?", (t["task_id"],))}
        for s in STEPS:
            r = runs.get(s.name)
            if r is None or r["status"] != "BLOCKED":
                continue
            deps = [runs[d] for d in s.deps]
            if not all(d["status"] in ("SUCCEEDED", "CACHED") for d in deps):
                continue
            dep_digests = {d["step_name"]: d["output_digest"] for d in deps}
            seed = params.get("nondet_seed") if s.idem == "nondet" else None
            key = compute_step_key(s, params, dep_digests, seed)
            with store.tx() as c:
                n = store.cas(
                    c,
                    "UPDATE step_run SET status='READY', step_key=?, seed=?,"
                    " blocked_reason=NULL WHERE step_run_id=? AND status='BLOCKED'",
                    (key, seed, r["step_run_id"]),
                )
                if n:
                    store.event(c, "step.ready", task_id=t["task_id"],
                                step_run_id=r["step_run_id"],
                                payload={"step": s.name, "step_key": key[:16]})
                    moved += 1
    return moved


# ----------------------------------------------------------- 2. resolve

def _cache_hit(store, step_key: str) -> list[str] | None:
    """命中要求「索引里有 + 文件真的在」。少任一条件都算未命中。"""
    rows = store.q("SELECT sha256 FROM step_cache WHERE step_key=?", (step_key,))
    if not rows:
        return None
    shas = [r["sha256"] for r in rows]
    for sha in shas:
        art = store.q1("SELECT rel_path FROM artifact WHERE sha256=?", (sha,))
        if art is None or not os.path.exists(
                os.path.join(store.artifacts_dir, art["rel_path"])):
            # 索引指向一个已经不存在的产物：清掉这条脏索引，当未命中处理。
            with store.tx() as c:
                c.execute("DELETE FROM step_cache WHERE step_key=?", (step_key,))
                store.event(c, "cache.invalidated", payload={"step_key": step_key[:16],
                                                            "missing": sha[:12]})
            return None
    return shas


def resolve(store) -> int:
    moved = 0
    rows = store.q(
        "SELECT sr.*, t.params_json AS task_params FROM step_run sr"
        "  JOIN task t ON t.task_id = sr.task_id"
        " WHERE sr.status='READY' AND t.status IN ('QUEUED','RUNNING')")
    for r in rows:
        s = STEP_BY_NAME[r["step_name"]]
        # 人工步骤不参与缓存：人看过并通过，是针对"这一次交付"的判断，
        # 不是"这个输入"的属性。复用一次人工通过，等于让新交付免检。
        shas = None if s.idem == "human" else _cache_hit(store, r["step_key"])
        if shas:
            with store.tx() as c:
                n = store.cas(
                    c,
                    "UPDATE step_run SET status='CACHED', output_digest=?, gpu_ready=0,"
                    " progress_done=progress_total WHERE step_run_id=? AND status='READY'",
                    (aggregate_digest(shas), r["step_run_id"]),
                )
                if not n:
                    continue
                for sha in shas:
                    c.execute("INSERT OR IGNORE INTO artifact_ref(step_run_id, task_id, sha256)"
                              " VALUES(?,?,?)", (r["step_run_id"], r["task_id"], sha))
                    c.execute("UPDATE artifact SET last_access_at=? WHERE sha256=?",
                              (now_iso(), sha))
                store.event(c, "step.cached", task_id=r["task_id"],
                            step_run_id=r["step_run_id"],
                            payload={"step": s.name, "artifacts": len(shas)})
                moved += 1
            continue

        # 未命中：入队。显存够就同时置 gpu_ready，不够就留在队列里等下一轮准入。
        seq = store.scalar("SELECT COALESCE(MAX(ready_seq),0)+1 FROM step_run")
        with store.tx() as c:
            n = store.cas(
                c,
                "UPDATE step_run SET status='QUEUED', queued_at=?, ready_seq=?,"
                " blocked_reason=NULL WHERE step_run_id=? AND status='READY'",
                (now_iso(), seq, r["step_run_id"]),
            )
            if not n:
                continue
            store.event(c, "step.queued", task_id=r["task_id"],
                        step_run_id=r["step_run_id"],
                        payload={"step": s.name, "gpu_mb": s.gpu_mb})
            moved += 1
        if s.gpu_mb:
            gpu.admit(store, r["step_run_id"], s.gpu_mb, s.gpu_mode, r["fencing_token"])
        else:
            with store.tx() as c:
                store.cas(c, "UPDATE step_run SET gpu_ready=1 WHERE step_run_id=?",
                          (r["step_run_id"],))
    return moved


# -------------------------------------------------------------- 3. wake

def wake(store) -> int:
    moved = 0
    for r in store.q(
            "SELECT step_run_id, task_id, step_name FROM step_run"
            " WHERE status='RETRY_WAIT' AND retry_at IS NOT NULL AND retry_at <= ?",
            (now_iso(),)):
        with store.tx() as c:
            n = store.cas(
                c,
                "UPDATE step_run SET status='QUEUED', queued_at=?, retry_at=NULL"
                " WHERE step_run_id=? AND status='RETRY_WAIT'",
                (now_iso(), r["step_run_id"]),
            )
            if n:
                store.event(c, "step.requeued", task_id=r["task_id"],
                            step_run_id=r["step_run_id"],
                            payload={"step": r["step_name"], "reason": "backoff_elapsed"})
                moved += 1

    # 排队中但显存还没批下来的，每轮重试一次准入（这就是"等待 GPU"那段灰条）
    for r in store.q(
            "SELECT sr.step_run_id, sr.step_name, sr.gpu_mb, sr.fencing_token"
            "  FROM step_run sr JOIN task t ON t.task_id=sr.task_id"
            " WHERE sr.status='QUEUED' AND sr.gpu_ready=0"
            "   AND t.status IN ('QUEUED','RUNNING')"):
        if not r["gpu_mb"]:
            # 不需要显存的步骤没有"等批文"这回事，直接放行。
            # 少了这一条，人工重试过的非 GPU 步骤会永久卡在队列里——
            # 因为重试时把 gpu_ready 清成了 0，而这里原先只处理 gpu_mb>0。
            with store.tx() as c:
                store.cas(c, "UPDATE step_run SET gpu_ready=1"
                             " WHERE step_run_id=? AND status='QUEUED' AND gpu_ready=0",
                          (r["step_run_id"],))
            continue
        s = STEP_BY_NAME.get(r["step_name"])
        if s is None:
            continue
        gpu.admit(store, r["step_run_id"], r["gpu_mb"], s.gpu_mode, r["fencing_token"])
    return moved


# ------------------------------------------------------- 4. 失败传导/取消

def cascade(store) -> int:
    """上游 FAILED/CANCELLED → 下游 BLOCKED 必须取消，否则任务永远等不到终态。

    同理，任务被取消时，还没跑的（QUEUED/RETRY_WAIT/ATTENTION）也必须就地取消——
    否则 worker 会继续把它们领走执行，而 CANCELLING 永远等不到"全部退出"。
    """
    moved = 0
    tasks = store.q("SELECT task_id, cancel_requested FROM task"
                    " WHERE status IN ('QUEUED','RUNNING','CANCELLING')")
    for t in tasks:
        runs = {r["step_name"]: r for r in
                store.q("SELECT step_run_id, step_name, status FROM step_run WHERE task_id=?",
                        (t["task_id"],))}
        dead = {n for n, r in runs.items() if r["status"] in ("FAILED", "CANCELLED")}
        for name, r in runs.items():
            if r["status"] not in ("BLOCKED", "QUEUED", "RETRY_WAIT", "ATTENTION"):
                continue
            blocked_by = [d for d in STEP_BY_NAME[name].deps if d in dead]
            if not blocked_by and not t["cancel_requested"]:
                continue
            with store.tx() as c:
                n = store.cas(
                    c,
                    "UPDATE step_run SET status='CANCELLED', gpu_ready=0, blocked_reason=?"
                    " WHERE step_run_id=? AND status IN"
                    " ('BLOCKED','QUEUED','RETRY_WAIT','ATTENTION')",
                    ("upstream_failed" if blocked_by else "cancel_requested", r["step_run_id"]),
                )
                if not n:
                    continue
                store.event(c, "step.cancelled", task_id=t["task_id"],
                            step_run_id=r["step_run_id"],
                            payload={"step": name,
                                     "reason": "upstream_failed" if blocked_by else "cancel_requested",
                                     "blocked_by": blocked_by})
                moved += 1
            if blocked_by:
                gpu.release(store, r["step_run_id"], "upstream_failed")
    return moved


# ------------------------------------------------------------ 5. project

def project(store) -> int:
    moved = 0
    for t in store.q("SELECT task_id, status, cancel_requested FROM task"
                     " WHERE status NOT IN ('SUCCEEDED','FAILED','CANCELLED')"):
        statuses = [r["status"] for r in
                    store.q("SELECT status FROM step_run WHERE task_id=?", (t["task_id"],))]
        target = derive_task_status(statuses, bool(t["cancel_requested"]))
        if target == t["status"]:
            continue
        check_task_transition(t["status"], target)
        with store.tx() as c:
            n = store.cas(c, "UPDATE task SET status=?, started_at=COALESCE(started_at,? )"
                             " ,ended_at=CASE WHEN ? IN ('SUCCEEDED','FAILED','CANCELLED')"
                             " THEN ? ELSE ended_at END"
                             " WHERE task_id=? AND status=?",
                          (target, now_iso(), target, now_iso(), t["task_id"], t["status"]))
            if n:
                store.event(c, f"task.{target.lower()}", task_id=t["task_id"],
                            payload={"from": t["status"]})
                moved += 1
    return moved


def approve(store, task_id: str, step_name: str, reviewer: str = "human") -> bool:
    """人工通过。人工环节的"执行者"就是人，所以直接把结果记成成功，不再跑一遍。

    记两条事件：step.approved 说明"谁批的"，step.succeeded 说明"变成了什么"——
    只有前者的话，事件重放重建不出最终状态。
    """
    sr = store.q1("SELECT * FROM step_run WHERE task_id=? AND step_name=?", (task_id, step_name))
    if sr is None or sr["status"] != "ATTENTION":
        return False
    now = now_iso()
    mono = int(time.monotonic() * 1000)
    with store.tx() as c:
        n = store.cas(
            c,
            "UPDATE step_run SET status='SUCCEEDED', blocked_reason=NULL, error_class=NULL,"
            " fencing_token=fencing_token+1, attempt_no=attempt_no+1,"
            " output_digest=COALESCE(output_digest, ?), progress_done=1, progress_total=1,"
            " last_progress_at=? WHERE step_run_id=? AND status='ATTENTION'",
            (aggregate_digest([]), now, sr["step_run_id"]),
        )
        if not n:
            return False
        c.execute(
            "INSERT INTO attempt(attempt_id, step_run_id, attempt_no, worker_id, status,"
            " variant, started_at, started_mono_ms, ended_at, ended_mono_ms, exit_code)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,0)",
            (new_id("A"), sr["step_run_id"], sr["attempt_no"] + 1, f"human:{reviewer}",
             "SUCCEEDED", sr["variant"], now, mono, now, mono),
        )
        store.event(c, "step.approved", task_id=task_id, step_run_id=sr["step_run_id"],
                    payload={"step": step_name, "by": reviewer})
        store.event(c, "step.succeeded", task_id=task_id, step_run_id=sr["step_run_id"],
                    payload={"step": step_name, "by": reviewer})
    return True


def tick(store) -> dict:
    return {
        "expanded": expand(store),
        "resolved": resolve(store),
        "woken": wake(store),
        "cancelled": cascade(store),
        "projected": project(store),
    }


# --------------------------------------------------------- 人工干预接口

def cancel_task(store, task_id: str) -> bool:
    """取消只写一个标志。真正的退出由执行者自己观察到标志后完成。"""
    with store.tx() as c:
        n = store.cas(c, "UPDATE task SET cancel_requested=1"
                         " WHERE task_id=? AND status IN ('QUEUED','RUNNING')", (task_id,))
        if n:
            store.event(c, "task.cancel_requested", task_id=task_id, payload={})
    return n > 0


def manual_retry(store, task_id: str, step_name: str) -> bool:
    """人工重试。API 也必须发号（fencing+1），否则等于绕开了 2.4.1 的全部论证。"""
    sr = store.q1("SELECT * FROM step_run WHERE task_id=? AND step_name=?", (task_id, step_name))
    if sr is None or sr["status"] not in ("FAILED", "ATTENTION", "CANCELLED"):
        return False
    with store.tx() as c:
        n = store.cas(
            c,
            "UPDATE step_run SET status='QUEUED', lease_id=NULL,"
            " fencing_token=fencing_token+1, queued_at=?, retry_at=NULL,"
            " error_class=NULL, blocked_reason=NULL, gpu_ready=0"
            " WHERE step_run_id=? AND status=?",
            (now_iso(), sr["step_run_id"], sr["status"]),
        )
        if not n:
            return False
        # 被上游失败连坐取消的下游，要放回 BLOCKED，否则重试跑完也接不上
        c.execute("UPDATE step_run SET status='BLOCKED', blocked_reason=NULL"
                  " WHERE task_id=? AND status='CANCELLED' AND blocked_reason='upstream_failed'",
                  (task_id,))
        c.execute("UPDATE task SET status='QUEUED', ended_at=NULL, cancel_requested=0"
                  " WHERE task_id=? AND status='FAILED'", (task_id,))
        store.event(c, "step.manual_retry", task_id=task_id, step_run_id=sr["step_run_id"],
                    payload={"step": step_name, "from": sr["status"],
                             "fencing_token": sr["fencing_token"] + 1})
    return True


def loop(store, interval_s: float = 0.5, max_ticks: int | None = None, quiet: bool = False):
    n = 0
    while max_ticks is None or n < max_ticks:
        r = tick(store)
        if not quiet and any(r.values()):
            print("[scheduler]", json.dumps(r, ensure_ascii=False))
        n += 1
        time.sleep(interval_s)
