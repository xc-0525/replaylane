"""8 个 Mock 步骤。

真 sleep、真写产物文件、真分片 checkpoint——因为要演的东西（kill -9 续跑、
缓存命中、退避条）全都依赖"有真实的中间状态落盘"，纯 print 演不出来。

一个步骤的执行模型：
  分片(0..n-1) → 每片产出一个小 payload → 写 checkpoint(shard)
  → 全部完成后把 payload 拼起来算 sha256 → 落一个 artifact → 成功
崩溃在任何位置，已完成的分片都能被下一次 attempt 复用（前提是 step_key 与
seed 一致，见 worker.load_adopted_shards）。
"""
from __future__ import annotations

import hashlib
import json
import os
import time

from .db import dumps, mono_ms, now_iso
from .model import Failure

TIME_SCALE = float(os.environ.get("REPLAYLANE_TIME_SCALE", "1"))


class StepContext:
    """worker 传给步骤的一切。步骤不碰数据库事务边界，只通过这几个方法说话。"""

    def __init__(self, *, store, task_id, step_run_id, attempt_id, step_key,
                 seed, params, variant, attempt_no, adopted_shards, faults,
                 progress_cb, checkpoint_cb, kill_cb):
        self.store = store
        self.task_id = task_id
        self.step_run_id = step_run_id
        self.attempt_id = attempt_id
        self.step_key = step_key
        self.seed = seed
        self.params = params
        self.variant = variant
        self.attempt_no = attempt_no
        self.adopted_shards = adopted_shards
        self.faults = faults or {}
        self._progress_cb = progress_cb
        self._checkpoint_cb = checkpoint_cb
        self._kill_cb = kill_cb

    def progress(self, done: int, total: int) -> None:
        self._progress_cb(done, total)

    def save_checkpoint(self, shard: int, payload: dict) -> None:
        self._checkpoint_cb(shard, payload)

    def kill(self, code: int = 137) -> None:
        """模拟 kill -9：不跑 finally、不关租约、不留遗言。"""
        self._kill_cb(code)

    def maybe_fail(self, attempt_no: int, step_name: str) -> None:
        """按注入配置决定这次要不要失败。

        注入格式：faults = {"matting": {"fail_times": 2, "class": "transient"}}
        """
        spec = self.faults.get(step_name) or {}
        fail_times = int(spec.get("fail_times", 0))
        if attempt_no <= fail_times:
            cls = spec.get("class", "transient")
            raise Failure(cls, f"injected {cls} on attempt {attempt_no}",
                          exit_code=spec.get("exit_code", 1))
        if spec.get("kill_at") is not None and attempt_no == int(spec.get("kill_on_attempt", 1)):
            # kill 在分片循环里按进度触发，不在这里
            return


def _payload(step_name: str, shard: int, seed: int | None, dep_digest: str) -> dict:
    """分片的"计算"。真实的 AI 步骤在这里调模型，这里用摘要代替。

    关键：payload 里带 dep_digest 与 seed —— 所以相同输入必然得到相同产物，
    不同 seed 必然得到不同产物。缓存语义因此可被验证，而不是靠信任。
    """
    raw = f"{step_name}|{shard}|{dep_digest}|{seed}"
    return {
        "shard": shard,
        "digest": hashlib.sha256(raw.encode()).hexdigest()[:16],
        "recorded_mono_ms": mono_ms(),
    }


def execute(step, ctx: StepContext, dep_digest: str) -> dict:
    """跑一个步骤，返回 {sha256, bytes, name}。异常直接往上抛给 worker 分类。"""
    units = max(1, step.shards)
    per_unit_s = max(0.01, (step.mock_seconds * TIME_SCALE) / units)

    kill_at = ctx.faults.get(step.name, {}).get("kill_at")
    kill_on_attempt = int(ctx.faults.get(step.name, {}).get("kill_on_attempt", 1))

    results: list[dict] = []
    for shard in range(units):
        if shard in ctx.adopted_shards:
            # 上一次 attempt 已经算过这一片：复用它，不重算。
            results.append(ctx.adopted_shards[shard])
            ctx.progress(shard + 1, units)
            continue

        time.sleep(per_unit_s)
        payload = _payload(step.name, shard, ctx.seed, dep_digest)
        ctx.save_checkpoint(shard, payload)
        results.append(payload)
        ctx.progress(shard + 1, units)

        if kill_at is not None and ctx.attempt_no == kill_on_attempt:
            if (shard + 1) / units >= float(kill_at):
                ctx.kill(137)

    body = dumps({"step": step.name, "seed": ctx.seed, "variant": ctx.variant,
                  "shards": results}).encode("utf-8")
    sha = hashlib.sha256(body).hexdigest()
    return {"sha256": sha, "body": body, "name": f"{step.name}.json"}


def write_artifact(store, sha: str, body: bytes, name: str, producer: str) -> str:
    """内容寻址写盘：先写临时文件再 rename，避免半成品被当成产物。"""
    import uuid
    rel = os.path.join(sha[:2], f"{sha}.bin")
    abs_path = os.path.join(store.artifacts_dir, rel)
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    if not os.path.exists(abs_path):
        tmp = abs_path + f".{uuid.uuid4().hex}.tmp"
        with open(tmp, "wb") as f:
            f.write(body)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, abs_path)
    with store.tx() as c:
        c.execute(
            "INSERT INTO artifact(artifact_id, sha256, rel_path, size_bytes, producer,"
            " created_at, last_access_at) VALUES(?,?,?,?,?,?,?)"
            " ON CONFLICT(sha256) DO UPDATE SET last_access_at=excluded.last_access_at",
            (f"a_{sha[:16]}", sha, rel, len(body), producer, now_iso(), now_iso()),
        )
    return rel


def aggregate_digest(shas: list[str]) -> str:
    """多产物时 output_digest 的合成口径（写死在代码里，不在文档里含糊）。"""
    return hashlib.sha256("".join(sorted(shas)).encode()).hexdigest()
