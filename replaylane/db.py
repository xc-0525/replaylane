"""SQLite 访问层：单写者、CAS、事件追加。

关于「单写线程」的一个实现偏离，先说清楚：
设计文档里写的是「应用内单写线程 + 队列」。实现里改为 **依赖 SQLite 自身的
单写者锁**（BEGIN IMMEDIATE），因为 worker / scheduler / sweeper 是三个独立进程，
应用内的线程池管不到跨进程的写入，真正提供互斥的是数据库文件锁。
好处是少一个组件、多进程天然安全；代价是写冲突时靠 busy_timeout 重试，
高并发下 p99 会抖（见 9.3 第 7 条）。这是有意识的取舍，不是偷懒。
"""
from __future__ import annotations

import json
import os
import secrets
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


# ------------------------------------------------------------------ 时间

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
        f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"


def mono_ms() -> int:
    """单调毫秒。只用来算差值，绝不用来排序。

    Windows 上 time.monotonic 走 QPC、Linux 上走 CLOCK_MONOTONIC，两者都是
    系统级（自开机起），所以同一台机器上跨进程可比。重启后归零，因此
    跨越重启的历史事件不能用它算宽度——verify 会检查这一点。
    """
    return int(time.monotonic() * 1000)


def iso_to_dt(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def age_s(iso_ts: str) -> float:
    return (datetime.now(timezone.utc) - iso_to_dt(iso_ts)).total_seconds()


def new_id(prefix: str = "") -> str:
    return f"{prefix}{int(time.time() * 1000):011x}{secrets.token_hex(3)}"


# ------------------------------------------------------------------ Store

class Store:
    def __init__(self, db_path: str, artifacts_dir: str):
        self.db_path = os.path.abspath(db_path)
        self.artifacts_dir = os.path.abspath(artifacts_dir)
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        os.makedirs(self.artifacts_dir, exist_ok=True)
        self._local = threading.local()

    # -- 连接：每线程一个（sqlite3 连接不能跨线程共享）
    def conn(self) -> sqlite3.Connection:
        c = getattr(self._local, "conn", None)
        if c is None:
            c = sqlite3.connect(self.db_path, timeout=10.0, isolation_level=None)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=FULL")
            c.execute("PRAGMA foreign_keys=ON")
            c.execute("PRAGMA busy_timeout=10000")
            self._local.conn = c
        return c

    def close(self) -> None:
        c = getattr(self._local, "conn", None)
        if c is not None:
            c.close()
            self._local.conn = None

    # -- 只读
    def q(self, sql: str, params=()) -> list[sqlite3.Row]:
        return list(self.conn().execute(sql, params))

    def q1(self, sql: str, params=()) -> sqlite3.Row | None:
        return self.conn().execute(sql, params).fetchone()

    def scalar(self, sql: str, params=()):
        row = self.q1(sql, params)
        return None if row is None else row[0]

    # -- 写事务
    @contextmanager
    def tx(self):
        conn = self.conn()
        for i in range(6):
            try:
                conn.execute("BEGIN IMMEDIATE")
                break
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc) and i < 5:
                    time.sleep(0.05 * (i + 1))
                    continue
                raise
        else:  # pragma: no cover
            raise RuntimeError("could not acquire write lock")
        try:
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    # -- CAS：返回受影响行数，调用方必须检查
    @staticmethod
    def cas(conn: sqlite3.Connection, sql: str, params=()) -> int:
        return conn.execute(sql, params).rowcount

    # -- 事件：只能在事务内调用，保证「状态变化」与「事实记录」同生共死
    @staticmethod
    def event(conn, type_: str, *, task_id=None, step_run_id=None,
              attempt_id=None, payload: dict | None = None) -> int:
        cur = conn.execute(
            "INSERT INTO event(ts, mono_ms, type, task_id, step_run_id, attempt_id, payload_json)"
            " VALUES(?,?,?,?,?,?,?)",
            (now_iso(), mono_ms(), type_, task_id, step_run_id, attempt_id,
             json.dumps(payload, ensure_ascii=False) if payload is not None else None),
        )
        return int(cur.lastrowid)


def init_db(db_path: str, artifacts_dir: str) -> Store:
    store = Store(db_path, artifacts_dir)
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        store.conn().executescript(f.read())
    with store.tx() as c:
        c.execute("INSERT OR IGNORE INTO gpu(gpu_id, total_mb, headroom) VALUES(0, 24576, 0.9)")
        c.execute("INSERT OR IGNORE INTO gpu(gpu_id, total_mb, headroom) VALUES(1, 24576, 0.9)")
    return store


def dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def loads(s: str | None, default=None):
    if s is None:
        return default
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return default
