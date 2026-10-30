"""worker：唯一执行步骤的地方。

一条 claim 语句同时做四件事：选中、加锁、发号（fencing）、建租约。
关键不是它短，而是它**只有一条**——恢复路径和正常路径走的是同一条代码，
所以不存在"崩溃分支没被测到"这种事。

所有写入都带 (lease_id, fencing_token) 做条件，僵尸 worker 复活时 rowcount=0，
它自己就知道该退出了（LeaseLost），不需要任何人去杀它。
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone

from . import gpu
from .db import Store, loads, new_id, now_iso
from .model import (MAX_VARIANT_LEVEL, VARIANT_LADDER, Failure, PRESETS, decide)
from .pipeline import STEP_BY_NAME
from .steps import StepContext, aggregate_digest, execute, write_artifact

HEARTBEAT_S = 2.0
LEASE_TTL_S = 15.0

# 老化必须在 ORDER BY 里现算，否则 7.3 承诺的防饿死只是装饰。
CLAIM_SQL = """
SELECT s.step_run_id
  FROM step_run s
  JOIN task t ON t.task_id = s.task_id
 WHERE s.status = 'QUEUED'
   AND s.gpu_ready = 1
   AND t.cancel_requested = 0
   AND t.status IN ('QUEUED','RUNNING')
 ORDER BY MAX(0, s.priority - CAST((julianday('now') - julianday(COALESCE(s.queued_at, 'now'))) * 288 AS INTEGER)) ASC,
          COALESCE(s.ready_seq, 0) ASC
 LIMIT 1
"""


class LeaseLost(Exception):
    """租约已不属于我。这不是错误，是设计里的正常退出信号。"""


class Worker:
    def __init__(self, store: Store, worker_id: str,
                 heartbeat_s: float = HEARTBEAT_S, ttl_s: float = LEASE_TTL_S):
        self.store = store
        self.id = worker_id
        self.heartbeat_s = heartbeat_s
        self.ttl_s = ttl_s

    # ------------------------------------------------------------ claim

    def claim(self) -> dict | None:
        lease_id = new_id("L")
        with self.store.tx() as c:
            cand = c.execute(CLAIM_SQL).fetchone()
            if cand is None:
                return None
            sr_id = cand["step_run_id"]
            cur = c.execute(
                "UPDATE step_run SET status='RUNNING', lease_id=?,"
                " fencing_token=fencing_token+1, attempt_no=attempt_no+1,"
                " last_progress_at=?, gpu_ready=1"
                " WHERE step_run_id=? AND status='QUEUED' AND gpu_ready=1"
                " RETURNING fencing_token, attempt_no, step_name, task_id, step_key,"
                "           seed, params_json, variant, spec_cost",
                (lease_id, now_iso(), sr_id),
            )
            row = cur.fetchone()
            if row is None:          # 被别的 worker 抢先了，正常退让
                return None
            attempt_id = new_id("A")
            c.execute(
                "INSERT INTO lease(lease_id, step_run_id, worker_id, fencing_token,"
                " acquired_at, last_beat) VALUES(?,?,?,?,?,?)",
                (lease_id, sr_id, self.id, row["fencing_token"], now_iso(), now_iso()),
            )
            c.execute(
                "INSERT INTO attempt(attempt_id, step_run_id, attempt_no, worker_id, lease_id,"
                " fencing_token, status, variant, started_at, started_mono_ms)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                (attempt_id, sr_id, row["attempt_no"], self.id, lease_id,
                 row["fencing_token"], "RUNNING", row["variant"], now_iso(),
                 int(time.monotonic() * 1000)),
            )
            self.store.event(c, "step.claimed", task_id=row["task_id"], step_run_id=sr_id,
                             attempt_id=attempt_id,
                             payload={"step": row["step_name"], "worker": self.id,
                                      "fencing_token": row["fencing_token"],
                                      "attempt_no": row["attempt_no"],
                                      "variant": row["variant"]})
        return {
            "lease_id": lease_id, "step_run_id": sr_id, "attempt_id": attempt_id,
            "fencing_token": row["fencing_token"], "attempt_no": row["attempt_no"],
            "step_name": row["step_name"], "task_id": row["task_id"],
            "step_key": row["step_key"], "seed": row["seed"],
            "params": loads(row["params_json"], {}), "variant": row["variant"],
        }

    # -------------------------------------------------------- heartbeat

    def _start_heartbeat(self, claim: dict) -> tuple[threading.Event, threading.Event]:
        stop = threading.Event()
        lost = threading.Event()

        def beat():
            # 独立连接：心跳不能被步骤的主线程写事务阻塞
            conn = Store(self.store.db_path, self.store.artifacts_dir).conn()
            try:
                while not stop.wait(self.heartbeat_s):
                    try:
                        n = conn.execute(
                            "UPDATE lease SET last_beat=? WHERE lease_id=? AND closed_at IS NULL",
                            (now_iso(), claim["lease_id"]),
                        ).rowcount
                    except Exception:
                        n = 1        # 数据库抖一下不算丢租约，下轮再说
                    if n == 0:
                        lost.set()
                        return
            finally:
                conn.close()

        t = threading.Thread(target=beat, daemon=True, name=f"hb-{claim['lease_id']}")
        t.start()
        return stop, lost

    # ------------------------------------------------------------ 主体

    def run_one(self, claim: dict) -> str:
        step = STEP_BY_NAME[claim["step_name"]]
        stop, lost = self._start_heartbeat(claim)
        adopted = self._load_adopted(claim)
        deadline = (time.monotonic() + step.timeout_s) if step.timeout_s else None

        def guard() -> None:
            if lost.is_set():
                raise LeaseLost()
            if deadline is not None and time.monotonic() > deadline:
                raise Failure("timeout", f"step exceeded {step.timeout_s}s")
            # 取消不是"从外面杀进程"，是执行者自己观察到标志后干净退出
            if self.store.scalar("SELECT cancel_requested FROM task WHERE task_id=?",
                                 (claim["task_id"],)):
                raise Failure("cancel", "task cancelled")

        def on_progress(done: int, total: int) -> None:
            guard()
            self._report_progress(claim, done, total)

        ctx = StepContext(
            store=self.store, task_id=claim["task_id"], step_run_id=claim["step_run_id"],
            attempt_id=claim["attempt_id"], step_key=claim["step_key"], seed=claim["seed"],
            params=claim["params"], variant=claim["variant"], attempt_no=claim["attempt_no"],
            adopted_shards=adopted,
            faults=(claim["params"].get("faults") or {}),
            progress_cb=on_progress,
            checkpoint_cb=lambda shard, payload: self._save_checkpoint(claim, shard, payload),
            kill_cb=lambda code: os._exit(code),   # 真 kill：不走 finally，不关租约
        )

        try:
            guard()
            ctx.maybe_fail(claim["attempt_no"], step.name)
            dep_digest = self._dep_digest(claim)
            out = execute(step, ctx, dep_digest)
        except LeaseLost:
            # 别人已经接手了，安静退出，不要写任何东西
            stop.set()
            return "lease_lost"
        except Failure as f:
            stop.set()
            return self._on_failure(claim, step, f)
        except Exception as exc:                       # 未分类异常一律当 bug
            stop.set()
            return self._on_failure(claim, step, Failure("bug", repr(exc)))

        stop.set()
        return self._commit(claim, step, out)

    # ------------------------------------------------------------ 提交

    def _commit(self, claim: dict, step, out: dict) -> str:
        sha = out["sha256"]
        rel = write_artifact(self.store, sha, out["body"], out["name"],
                             producer=claim["step_run_id"])
        # output_digest 是"产物集合"的摘要，不是单个产物的 sha。
        # 两条路径（真跑成功 / 命中缓存）必须用同一个函数算，否则下游 key 永远对不上：
        # 上游真跑时写 sha(A)，命中缓存时写 sha(sha(A))，下游就再也命不中了。
        digest = aggregate_digest([sha])
        with self.store.tx() as c:
            n = self.store.cas(
                c,
                "UPDATE step_run SET status='SUCCEEDED', output_digest=?, progress_done=?,"
                " last_progress_at=?, error_class=NULL, retry_at=NULL"
                " WHERE step_run_id=? AND lease_id=? AND fencing_token=? AND status='RUNNING'",
                (digest, step.shards or 1, now_iso(), claim["step_run_id"],
                 claim["lease_id"], claim["fencing_token"]),
            )
            if n == 0:
                return "rejected_stale"      # 断言 A2 的现场：僵尸提交被挡
            c.execute("INSERT OR IGNORE INTO artifact_ref(step_run_id, task_id, sha256)"
                      " VALUES(?,?,?)", (claim["step_run_id"], claim["task_id"], sha))
            c.execute("INSERT OR REPLACE INTO step_cache(step_key, sha256, step_name, created_at)"
                      " VALUES(?,?,?,?)", (claim["step_key"], sha, step.name, now_iso()))
            c.execute("UPDATE artifact SET last_access_at=? WHERE sha256=?", (now_iso(), sha))
            c.execute("UPDATE lease SET closed_at=?, close_reason='committed'"
                      " WHERE lease_id=? AND closed_at IS NULL", (now_iso(), claim["lease_id"]))
            c.execute("UPDATE attempt SET status='SUCCEEDED', ended_at=?, ended_mono_ms=?,"
                      " exit_code=0 WHERE attempt_id=?",
                      (now_iso(), int(time.monotonic() * 1000), claim["attempt_id"]))
            self.store.event(c, "step.succeeded", task_id=claim["task_id"],
                             step_run_id=claim["step_run_id"], attempt_id=claim["attempt_id"],
                             payload={"step": step.name, "digest": sha[:16]})
        gpu.release(self.store, claim["step_run_id"], "committed")
        return "succeeded"

    # ------------------------------------------------------------ 失败

    def _same_variant_failures(self, claim: dict) -> int:
        return self.store.scalar(
            "SELECT COUNT(*) FROM attempt WHERE step_run_id=? AND variant=?"
            " AND status IN ('FAILED','TIMEOUT','ABANDONED')",
            (claim["step_run_id"], claim["variant"])) or 0

    def _on_failure(self, claim: dict, step, failure: Failure) -> str:
        preset = step.retry
        level = VARIANT_LADDER.index(claim["variant"]) if claim["variant"] in VARIANT_LADDER else 0
        action, detail = decide(failure.cls, preset, level, self._same_variant_failures(claim))

        now = now_iso()
        mono = int(time.monotonic() * 1000)
        with self.store.tx() as c:
            base_params = (claim["step_run_id"], claim["lease_id"], claim["fencing_token"])
            if action == "retry":
                target_status, retry_at, extra = "RETRY_WAIT", \
                    (datetime.now(timezone.utc) + timedelta(seconds=detail)).strftime(
                        "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z", {}
            elif action == "degrade":
                target_status, retry_at, extra = "QUEUED", None, {"variant": detail}
            elif action == "attention":
                target_status, retry_at, extra = "ATTENTION", None, {}
            elif action == "cancel":
                target_status, retry_at, extra = "CANCELLED", None, {}
            else:
                target_status, retry_at, extra = "FAILED", None, {}

            sets = "status=?, retry_at=?, error_class=?, last_progress_at=?"
            vals: list = [target_status, retry_at, failure.cls, now]
            for k, v in extra.items():
                sets += f", {k}=?"
                vals.append(v)
            n = self.store.cas(
                c,
                f"UPDATE step_run SET {sets}"
                " WHERE step_run_id=? AND lease_id=? AND fencing_token=? AND status='RUNNING'",
                (*vals, *base_params),
            )
            if n == 0:
                return "rejected_stale"

            c.execute("UPDATE lease SET closed_at=?, close_reason=? WHERE lease_id=?"
                      " AND closed_at IS NULL", (now, action, claim["lease_id"]))
            c.execute("UPDATE attempt SET status=?, ended_at=?, ended_mono_ms=?,"
                      " error_class=?, error_msg=?, exit_code=? WHERE attempt_id=?",
                      ("FAILED" if action != "attention" else "FAILED", now, mono,
                       failure.cls, failure.msg, failure.exit_code or 1, claim["attempt_id"]))

            evt = {"retry": "step.retrying", "degrade": "step.degraded",
                   "attention": "step.attention", "fail": "step.failed",
                   "cancel": "step.cancelled"}[action]
            self.store.event(c, evt, task_id=claim["task_id"],
                             step_run_id=claim["step_run_id"], attempt_id=claim["attempt_id"],
                             payload={"step": step.name, "class": failure.cls,
                                      "action": action, "detail": detail,
                                      "attempt_no": claim["attempt_no"],
                                      "variant": claim["variant"]})
        gpu.release(self.store, claim["step_run_id"], f"failed_{action}")
        return f"failed_{action}"

    # -------------------------------------------------------- checkpoint

    def _load_adopted(self, claim: dict) -> dict[int, dict]:
        """只采纳 (step_key, seed) 都对得上、且属于更早 attempt 的分片。

        换 seed 等于换了 step_key，所以这条 where 顺便保证了"绝不混用两个 seed 的产物"。
        """
        rows = self.store.q(
            "SELECT shard, payload_json FROM checkpoint"
            " WHERE step_run_id=? AND step_key=? AND seed IS ? AND attempt_no<?",
            (claim["step_run_id"], claim["step_key"], claim["seed"], claim["attempt_no"]))
        out = {}
        for r in rows:
            try:
                out[r["shard"]] = json.loads(r["payload_json"])
            except json.JSONDecodeError:
                continue
        return out

    def _save_checkpoint(self, claim: dict, shard: int, payload: dict) -> None:
        with self.store.tx() as c:
            alive = c.execute(
                "SELECT 1 FROM step_run WHERE step_run_id=? AND lease_id=? AND"
                " fencing_token=? AND status='RUNNING'",
                (claim["step_run_id"], claim["lease_id"], claim["fencing_token"])).fetchone()
            if alive is None:
                raise LeaseLost()
            c.execute(
                "INSERT INTO checkpoint(step_run_id, shard, step_key, seed, attempt_no,"
                " payload_json, created_at) VALUES(?,?,?,?,?,?,?)"
                " ON CONFLICT(step_run_id, shard) DO UPDATE SET"
                "  step_key=excluded.step_key, seed=excluded.seed,"
                "  attempt_no=excluded.attempt_no, payload_json=excluded.payload_json,"
                "  created_at=excluded.created_at",
                (claim["step_run_id"], shard, claim["step_key"], claim["seed"],
                 claim["attempt_no"], json.dumps(payload, ensure_ascii=False), now_iso()),
            )

    def _report_progress(self, claim: dict, done: int, total: int) -> None:
        with self.store.tx() as c:
            n = self.store.cas(
                c,
                "UPDATE step_run SET progress_done=?, progress_total=?, last_progress_at=?"
                " WHERE step_run_id=? AND lease_id=? AND fencing_token=? AND status='RUNNING'",
                (done, total, now_iso(), claim["step_run_id"], claim["lease_id"],
                 claim["fencing_token"]),
            )
            if n == 0:
                raise LeaseLost()
            self.store.event(c, "step.progress", task_id=claim["task_id"],
                             step_run_id=claim["step_run_id"], attempt_id=claim["attempt_id"],
                             payload={"step": claim["step_name"], "done": done, "total": total})

    def _dep_digest(self, claim: dict) -> str:
        step = STEP_BY_NAME[claim["step_name"]]
        if not step.deps:
            return ""
        qs = ",".join("?" * len(step.deps))
        rows = self.store.q(
            f"SELECT output_digest FROM step_run WHERE task_id=? AND step_name IN ({qs})",
            (claim["task_id"], *step.deps))
        return "|".join(sorted(r["output_digest"] or "" for r in rows))


def run_forever(store: Store, worker_id: str, poll_s: float = 0.5, max_steps: int | None = None):
    w = Worker(store, worker_id)
    done = 0
    idle = 0
    while max_steps is None or done < max_steps:
        claim = w.claim()
        if claim is None:
            idle += 1
            time.sleep(poll_s)
            continue
        idle = 0
        result = w.run_one(claim)
        done += 1
        print(f"[worker {worker_id}] {claim['step_name']} attempt#{claim['attempt_no']} "
              f"token={claim['fencing_token']} -> {result}", flush=True)
