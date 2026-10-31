"""HTTP 服务：REST + SSE + 静态看板。

零框架的代价在这里最明显：没有路由分组、没有中间件、没有参数校验层。
收益是别人 `python -m replaylane.cli serve` 就能跑起来，不用装任何东西。

SSE 用 http.server 的线程模型：一个订阅者一个线程。超过 200 个并发看板会耗尽
线程（设计文档 9.3 第 4 条已列为已知薄弱点），生产要换 asyncio 或轮询兜底。
"""
from __future__ import annotations

import json
import mimetypes
import os
import queue
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import gpu, scheduler, sweeper
from .db import Store, loads, now_iso
from .model import STEP_TERMINAL
from .pipeline import (DEFAULT_PARAMS, PIPELINE_NAME, STEPS, STEP_BY_NAME,
                       TOTAL_COST, canonical_params)

WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")

# ---------------------------------------------------------------- SSE 广播

_subs: list[queue.Queue] = []
_subs_lock = threading.Lock()


def _publish(evt: dict) -> None:
    with _subs_lock:
        for q in list(_subs):
            try:
                q.put_nowait(evt)
            except queue.Full:
                pass


def _tail_events(store: Store, stop: threading.Event) -> None:
    """把 event 表的新行推给订阅者。用 seq 做游标，天然支持断线重连。"""
    last = store.scalar("SELECT COALESCE(MAX(seq),0) FROM event") or 0
    while not stop.is_set():
        rows = store.q("SELECT seq, ts, mono_ms, type, task_id, step_run_id, attempt_id,"
                       " payload_json FROM event WHERE seq > ? ORDER BY seq LIMIT 200", (last,))
        for r in rows:
            last = r["seq"]
            _publish(dict(r))
        stop.wait(0.25)


# ------------------------------------------------------------------ 派生

def task_progress(store: Store, task_id: str) -> int:
    rows = store.q("SELECT spec_cost, status, progress_done, progress_total"
                   " FROM step_run WHERE task_id=?", (task_id,))
    done = 0.0
    for r in rows:
        if r["status"] in ("SUCCEEDED", "CACHED"):
            done += r["spec_cost"]
        elif r["status"] == "RUNNING" and r["progress_total"]:
            done += r["spec_cost"] * r["progress_done"] / r["progress_total"]
    pct = int(round(done / TOTAL_COST * 100)) if TOTAL_COST else 0
    if any(r["status"] not in STEP_TERMINAL for r in rows):
        pct = min(pct, 99)      # 还有步骤没结束，进度条不许先到终点
    return pct


# 事件 -> 甘特段类型。宽度一律用 mono_ms 差值，seq 只负责顺序。
_SEG = {
    "step.ready": "ready",
    "step.queued": "queued",
    "step.claimed": "running",
    "step.retrying": "backoff",
    "step.requeued": "queued",
    "step.degraded": "degraded",
    "step.attention": "attention",
    "step.succeeded": "ok",
    "step.cached": "cached",
    "step.failed": "failed",
    "step.cancelled": "cancelled",
}
_OPENERS = {"ready", "queued", "running", "backoff", "attention"}


def timeline(store: Store, task_id: str) -> dict:
    """从事件流重建甘特数据。

    这是"可观测性不是事后加日志"的具体体现：图上每一段都能指回一条 event。
    段宽用 mono_ms（单调时钟）相减，所以 NTP 回拨不会画出负宽度。
    """
    evs = store.q("SELECT seq, mono_ms, type, step_run_id, payload_json FROM event"
                  " WHERE task_id=? ORDER BY seq", (task_id,))
    if not evs:
        return {"base_mono_ms": 0, "segments": []}
    base = min(e["mono_ms"] for e in evs)
    names = {r["step_run_id"]: r["step_name"] for r in
             store.q("SELECT step_run_id, step_name FROM step_run WHERE task_id=?", (task_id,))}

    open_seg: dict[str, dict] = {}
    segs: list[dict] = []
    for e in evs:
        sid = e["step_run_id"]
        if sid not in names:
            continue

        # 崩溃：把"执行中"切断在它真正停止响应的那一刻（最后一次心跳），
        # 之后到被回收之间是一段"死掉的空窗"，必须单独画出来，
        # 否则整段死寂会被算进执行时长，图上看起来就像正常跑完了。
        if e["type"] == "lease.expired":
            cur = open_seg.pop(sid, None)
            if cur is not None:
                p = loads(e["payload_json"], {}) or {}
                silence = int(p.get("silent_ms") or (float(p.get("ttl_s", 15)) * 1000))
                cut = max(cur["t0"], e["mono_ms"] - silence)
                cur["t1"] = cut
                segs.append(cur)
                open_seg[sid] = {"step": names[sid], "type": "dead",
                                 "t0": cut, "t1": e["mono_ms"], "seq": e["seq"]}
            continue

        kind = _SEG.get(e["type"])
        if kind is None:
            continue

        # 降级：失败一次、换个档位立刻重排。它没有"等待"，所以必须画成标记，
        # 否则图上就是一条连续的蓝条，看不出中间换过跑法。
        if kind == "degraded":
            cur = open_seg.pop(sid, None)
            if cur is not None:
                cur["t1"] = e["mono_ms"]
                segs.append(cur)
            p = loads(e["payload_json"], {}) or {}
            segs.append({"step": names[sid], "type": "degraded", "t0": e["mono_ms"],
                         "t1": e["mono_ms"], "seq": e["seq"],
                         "variant": p.get("detail")})
            open_seg[sid] = {"step": names[sid], "type": "queued",
                             "t0": e["mono_ms"], "t1": e["mono_ms"], "seq": e["seq"]}
            continue

        cur = open_seg.pop(sid, None)
        if cur is not None:
            cur["t1"] = e["mono_ms"]
            if cur["t1"] < cur["t0"]:
                cur["t0"] = cur["t1"]
            if cur["type"] in ("retry_marker",):
                continue
            segs.append(cur)
        if kind in _OPENERS:
            seg = {"step": names[sid], "type": kind, "t0": e["mono_ms"],
                   "t1": e["mono_ms"], "seq": e["seq"]}
            if kind == "running":
                # 这一段是用哪个档位跑的？降级跑出来的要画成另一种颜色。
                seg["variant"] = (loads(e["payload_json"], {}) or {}).get("variant")
            open_seg[sid] = seg
        else:
            segs.append({"step": names[sid], "type": kind, "t0": e["mono_ms"],
                         "t1": e["mono_ms"], "seq": e["seq"]})

    now = int(time.monotonic() * 1000)
    for cur in open_seg.values():
        cur["t1"] = max(cur["t0"], now)
        segs.append(cur)

    for s in segs:
        s["t0"] -= base
        s["t1"] -= base
    return {"base_mono_ms": base, "segments": segs}


def stats(store: Store) -> dict:
    by_status = {r["status"]: r["n"] for r in
                 store.q("SELECT status, COUNT(*) n FROM step_run GROUP BY status")}
    total_steps = sum(by_status.values()) or 1
    cached = by_status.get("CACHED", 0)
    tasks = {r["status"]: r["n"] for r in
             store.q("SELECT status, COUNT(*) n FROM task GROUP BY status")}
    gpu_rows = []
    for g in store.q("SELECT gpu_id, total_mb, headroom FROM gpu ORDER BY gpu_id"):
        used = gpu.used_mb(store, g["gpu_id"])
        gpu_rows.append({"gpu_id": g["gpu_id"], "total_mb": g["total_mb"],
                         "used_mb": used, "util_pct": round(used / g["total_mb"] * 100, 1)
                         if g["total_mb"] else 0})
    return {
        "tasks": tasks,
        "steps": by_status,
        "cache_hit_rate": round(cached / total_steps * 100, 1),
        "queue_depth": by_status.get("QUEUED", 0),
        "running": by_status.get("RUNNING", 0),
        "gpu": gpu_rows,
        "events": store.scalar("SELECT COUNT(*) FROM event"),
        "artifacts": store.scalar("SELECT COUNT(*) FROM artifact"),
    }


# ------------------------------------------------------------------ 路由

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "replaylane/1.0"
    store: Store = None            # 由 serve() 注入
    stop: threading.Event = None

    # -- 工具
    def _json(self, obj, code: int = 200) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _err(self, code: int, msg: str) -> None:
        self._json({"error": msg}, code)

    def _body(self) -> dict:
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        try:
            return json.loads(self.rfile.read(n).decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return {}

    # -- GET
    def do_GET(self) -> None:  # noqa: N802
        path, _, query = self.path.partition("?")
        qs = urllib.parse.parse_qs(query)
        try:
            if path == "/api/tasks":
                return self._list_tasks(qs)
            if path.startswith("/api/tasks/"):
                return self._task_route(path, qs)
            if path == "/api/stream":
                return self._sse(int(qs.get("from_seq", ["0"])[0]))
            if path == "/api/events":
                since = int(qs.get("from_seq", ["0"])[0])
                rows = self.store.q(
                    "SELECT seq, ts, mono_ms, type, task_id, step_run_id, attempt_id,"
                    " payload_json FROM event WHERE seq > ? ORDER BY seq LIMIT 500", (since,))
                return self._json([dict(r) for r in rows])
            if path == "/api/stats":
                return self._json(stats(self.store))
            if path == "/api/pipeline":
                return self._json({
                    "pipeline": PIPELINE_NAME, "total_cost": TOTAL_COST,
                    "defaults": DEFAULT_PARAMS,
                    "steps": [{"name": s.name, "label": s.label, "desc": s.desc,
                               "deps": list(s.deps), "cost": s.cost,
                               "gpu_mb": s.gpu_mb, "retry": s.retry, "idem": s.idem,
                               "checkpoint": s.checkpoint} for s in STEPS],
                })
            return self._static(path)
        except BrokenPipeError:
            pass
        except Exception as exc:                       # 别让一个坏请求带走服务
            try:
                self._err(500, repr(exc))
            except Exception:
                pass

    def _list_tasks(self, qs) -> None:
        limit = int(qs.get("limit", ["30"])[0])
        rows = self.store.q("SELECT * FROM task ORDER BY submitted_at DESC LIMIT ?", (limit,))
        out = []
        for r in rows:
            d = dict(r)
            d["progress"] = task_progress(self.store, r["task_id"])
            d["params"] = loads(r["params_json"], {})
            d["name"] = d["params"].get("name") or r["task_id"][-8:]
            del d["params_json"]
            out.append(d)
        self._json(out)

    def _task_route(self, path: str, qs) -> None:
        parts = [p for p in path.split("/") if p]
        # /api/tasks/{id}[/steps/{name}[/retry]][/events|/attempts|/timeline]
        task_id = parts[2]
        t = self.store.q1("SELECT * FROM task WHERE task_id=?", (task_id,))
        if t is None:
            return self._err(404, "task not found")
        rest = parts[3:]

        if not rest:
            steps = [dict(r) for r in self.store.q(
                "SELECT * FROM step_run WHERE task_id=? ORDER BY rowid", (task_id,))]
            for s in steps:
                s["spec"] = dict(STEP_BY_NAME[s["step_name"]].__dict__)
            tparams = loads(t["params_json"], {})
            return self._json({"task": {**dict(t), "progress": task_progress(self.store, task_id),
                                        "params": tparams,
                                        "name": tparams.get("name") or task_id[-8:]},
                               "steps": steps})

        if rest == ["events"]:
            since = int(qs.get("from_seq", ["0"])[0])
            rows = self.store.q(
                "SELECT seq, ts, mono_ms, type, task_id, step_run_id, attempt_id, payload_json"
                " FROM event WHERE task_id=? AND seq>? ORDER BY seq LIMIT 500",
                (task_id, since))
            return self._json([dict(r) for r in rows])

        if rest == ["attempts"]:
            rows = self.store.q(
                "SELECT a.*, s.step_name FROM attempt a JOIN step_run s"
                " ON s.step_run_id=a.step_run_id WHERE s.task_id=? ORDER BY a.started_at",
                (task_id,))
            return self._json([dict(r) for r in rows])

        if rest == ["timeline"]:
            return self._json(timeline(self.store, task_id))

        if len(rest) == 3 and rest[0] == "steps" and rest[2] == "retry":
            return self._err(405, "use POST")

        if len(rest) == 2 and rest[0] == "steps":
            s = self.store.q1("SELECT * FROM step_run WHERE task_id=? AND step_name=?",
                              (task_id, rest[1]))
            if s is None:
                return self._err(404, "step not found")
            atts = [dict(r) for r in self.store.q(
                "SELECT * FROM attempt WHERE step_run_id=? ORDER BY attempt_no",
                (s["step_run_id"],))]
            return self._json({"step": dict(s), "attempts": atts})

        return self._err(404, "not found")

    def _sse(self, from_seq: int) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()

        # 先补历史（断线重连靠 Last-Event-ID，语义与 SSE 规范一致）
        last_id = self.headers.get("Last-Event-ID")
        if last_id:
            try:
                from_seq = max(from_seq, int(last_id))
            except ValueError:
                pass
        backlog = self.store.q(
            "SELECT seq, ts, mono_ms, type, task_id, step_run_id, attempt_id, payload_json"
            " FROM event WHERE seq > ? ORDER BY seq LIMIT 300", (from_seq,))
        for r in backlog:
            self._write_sse(dict(r))

        q: queue.Queue = queue.Queue(maxsize=1000)
        with _subs_lock:
            _subs.append(q)
        try:
            while not self.stop.is_set():
                try:
                    evt = q.get(timeout=15)
                except queue.Empty:
                    self.wfile.write(b": keep-alive\n\n")   # 注释行，防中间层掐连接
                    self.wfile.flush()
                    continue
                self._write_sse(evt)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            with _subs_lock:
                if q in _subs:
                    _subs.remove(q)

    def _write_sse(self, evt: dict) -> None:
        # 只发 id + data，不发 `event:` 字段。
        # 一旦带上命名事件，浏览器的 EventSource.onmessage 就永远收不到（要走 addEventListener），
        # 而事件类型本来就在 JSON 里，没必要再用 SSE 的字段分频道。
        payload = json.dumps(evt, ensure_ascii=False)
        chunk = f"id: {evt['seq']}\ndata: {payload}\n\n"
        self.wfile.write(chunk.encode("utf-8"))
        self.wfile.flush()

    def _static(self, path: str) -> None:
        rel = "index.html" if path in ("/", "") else path.lstrip("/")
        if rel.startswith("static/"):
            rel = rel[len("static/"):]
        full = os.path.normpath(os.path.join(WEB_DIR, rel))
        if not full.startswith(WEB_DIR) or not os.path.isfile(full):
            return self._err(404, "not found")
        ctype = mimetypes.guess_type(full)[0] or "application/octet-stream"
        if ctype.startswith("text/") or ctype in ("application/javascript",):
            ctype += "; charset=utf-8"
        with open(full, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # -- POST
    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?")[0]
        parts = [p for p in path.split("/") if p]
        try:
            body = self._body()
            if path == "/api/tasks":
                params = body.get("params") or {}
                if body.get("name"):
                    params["name"] = body["name"]
                if isinstance(params.get("faults"), str):
                    params["faults"] = json.loads(params["faults"])
                tid = scheduler.submit(self.store, params,
                                       priority=int(body.get("priority", 5)),
                                       seed=body.get("seed"))
                return self._json({"task_id": tid}, 201)
            if len(parts) == 4 and parts[:2] == ["api", "tasks"] and parts[3] == "cancel":
                ok = scheduler.cancel_task(self.store, parts[2])
                return self._json({"ok": ok}, 200 if ok else 409)
            if (len(parts) == 6 and parts[:2] == ["api", "tasks"]
                    and parts[3] == "steps" and parts[5] in ("retry", "approve")):
                if parts[5] == "approve":
                    ok = scheduler.approve(self.store, parts[2], parts[4])
                else:
                    ok = scheduler.manual_retry(self.store, parts[2], parts[4])
                return self._json({"ok": ok}, 200 if ok else 409)
            return self._err(404, "not found")
        except BrokenPipeError:
            pass
        except Exception as exc:
            try:
                self._err(500, repr(exc))
            except Exception:
                pass

    def log_message(self, fmt: str, *args) -> None:
        if os.environ.get("REPLAYLANE_HTTP_LOG"):
            super().log_message(fmt, *args)


# ------------------------------------------------------------------ 启动

def serve(store: Store, host: str = "127.0.0.1", port: int = 8000,
          workers: int = 2) -> None:
    import subprocess
    import sys

    stop = threading.Event()
    Handler.store = store
    Handler.stop = stop

    threading.Thread(target=_tail_events, args=(store, stop), daemon=True).start()

    def sched():
        while not stop.is_set():
            try:
                scheduler.tick(store)
            except Exception:
                pass
            stop.wait(0.3)

    def sweep():
        i = 0
        while not stop.is_set():
            try:
                sweeper.reclaim_expired(store)
                sweeper.detect_stalled(store)
                if i % 20 == 0:
                    sweeper.gc_artifacts(store)
                    gpu.orphan_cleanup(store)
            except Exception:
                pass
            i += 1
            stop.wait(1.0)

    threading.Thread(target=sched, daemon=True).start()
    threading.Thread(target=sweep, daemon=True).start()

    procs = []
    for i in range(workers):
        procs.append(subprocess.Popen(
            [sys.executable, "-m", "replaylane.cli", "--db", store.db_path,
             "--artifacts", store.artifacts_dir, "worker", "--id", f"w{i + 1}"]))

    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.daemon_threads = True
    print(f"replaylane 看板: http://{host}:{port}   (db={store.db_path})", flush=True)
    print(f"worker 子进程: {[p.pid for p in procs]}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping...")
    finally:
        stop.set()
        for p in procs:
            p.terminate()
        httpd.server_close()
