"""测试脚手架：调度器+sweeper 跑在线程里，worker 跑在真子进程里。

为什么 worker 必须是子进程：kill -9 的故障注入是真的 os._exit，跑在线程里
会把测试进程一起带走。用子进程还有一个额外好处——它证明了"多进程共用一个
SQLite 文件"这件事是真的能工作的，而不是单进程里假装分布式。
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import threading
import time

from replaylane import scheduler, sweeper
from replaylane.db import init_db

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TERMINAL = ("SUCCEEDED", "FAILED", "CANCELLED")


class Cluster:
    def __init__(self, workers: int = 1, time_scale: str = "0.04", ttl_s: float = 2.0):
        self.dir = tempfile.mkdtemp(prefix="rl_test_")
        self.db = os.path.join(self.dir, "t.db")
        self.art = os.path.join(self.dir, "art")
        self.ttl_s = ttl_s
        os.environ["REPLAYLANE_TIME_SCALE"] = time_scale
        os.environ["REPLAYLANE_LEASE_TTL_S"] = str(ttl_s)
        self.store = init_db(self.db, self.art)
        self._stop = threading.Event()
        self._procs: list[subprocess.Popen] = []
        self._workers = workers

    def start(self) -> "Cluster":
        threading.Thread(target=self._sched, daemon=True).start()
        threading.Thread(target=self._sweep, daemon=True).start()
        for i in range(self._workers):
            self._procs.append(subprocess.Popen(
                [sys.executable, "-m", "replaylane.cli", "--db", self.db,
                 "--artifacts", self.art, "worker", "--id", f"w{i + 1}"],
                cwd=REPO_ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        return self

    def _sched(self):
        while not self._stop.is_set():
            try:
                scheduler.tick(self.store)
            except Exception:
                pass
            time.sleep(0.1)

    def _sweep(self):
        while not self._stop.is_set():
            try:
                sweeper.reclaim_expired(self.store, ttl_s=self.ttl_s)
            except Exception:
                pass
            time.sleep(0.2)

    def run(self, params: dict | None = None, timeout: float = 60.0) -> tuple[str, str]:
        # 默认让人工环节自动通过：多数用例关心的是调度/重试/缓存，
        # 不是"停在闸门前等人"。要测闸门本身，显式传 auto_approve=False。
        params = dict(params or {})
        params.setdefault("auto_approve", True)
        tid = scheduler.submit(self.store, params)
        deadline = time.time() + timeout
        while time.time() < deadline:
            st = self.store.scalar("SELECT status FROM task WHERE task_id=?", (tid,))
            if st in TERMINAL:
                return tid, st
            time.sleep(0.1)
        return tid, self.store.scalar("SELECT status FROM task WHERE task_id=?", (tid,)) or "TIMEOUT"

    def close(self):
        self._stop.set()
        for p in self._procs:
            p.terminate()
        for p in self._procs:
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
        self.store.close()          # 否则 unittest 会刷 ResourceWarning


def steps_of(store, task_id: str) -> dict:
    return {r["step_name"]: dict(r) for r in
            store.q("SELECT * FROM step_run WHERE task_id=?", (task_id,))}


def attempts_of(store, task_id: str, step: str) -> list:
    return [dict(r) for r in store.q(
        "SELECT a.* FROM attempt a JOIN step_run s ON s.step_run_id=a.step_run_id"
        " WHERE s.task_id=? AND s.step_name=? ORDER BY a.attempt_no", (task_id, step))]


def events_of(store, task_id: str) -> list:
    return [dict(r) for r in store.q(
        "SELECT * FROM event WHERE task_id=? ORDER BY seq", (task_id,))]
