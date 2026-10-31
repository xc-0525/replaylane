"""接口层测试：路由、状态码、以及"点了按钮之后到底发生了什么"。

为什么单独一组：核心逻辑（状态机、fencing、恢复）已经有用例了，但 HTTP 路由
一条都没覆盖——结果三个 bug 全是手工点出来的（重试 404、重试后卡队列、全缓存没提示）。
路由下标错位这种错，只有真发一次请求才抓得到。
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
import warnings
from http.server import ThreadingHTTPServer

# ThreadingHTTPServer 每个连接一个线程，Store 按线程缓存连接，
# 线程结束时靠 GC 回收 → 会刷一屏 ResourceWarning。不影响正确性，这里静音。
warnings.filterwarnings("ignore", category=ResourceWarning)

from replaylane import scheduler
from replaylane.db import init_db
from replaylane.server import Handler
from replaylane.worker import Worker


class APITestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp(prefix="rl_api_")
        cls.store = init_db(os.path.join(cls.dir, "t.db"), os.path.join(cls.dir, "art"))
        Handler.store = cls.store
        Handler.stop = threading.Event()
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.httpd.daemon_threads = True
        cls.port = cls.httpd.server_address[1]
        threading.Thread(target=cls.httpd.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.store.close()

    # -- 工具
    def req(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                   method=method,
                                   headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(r, timeout=10) as resp:
                raw = resp.read()
                ctype = resp.headers.get("Content-Type", "")
                return resp.status, (json.loads(raw) if "json" in ctype else raw.decode("utf-8", "replace"))
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                return e.code, json.loads(raw)
            except json.JSONDecodeError:
                return e.code, raw.decode("utf-8", "replace")

    def new_task(self, source: str, faults: dict | None = None) -> str:
        params = {"source": source}
        if faults:
            params["faults"] = faults
        code, body = self.req("POST", "/api/tasks", {"params": params})
        self.assertEqual(code, 201, body)
        return body["task_id"]

    def step(self, task_id: str, name: str) -> dict:
        return dict(self.store.q1(
            "SELECT * FROM step_run WHERE task_id=? AND step_name=?", (task_id, name)))


class TestReadRoutes(APITestCase):
    def test_pipeline_has_labels(self):
        code, body = self.req("GET", "/api/pipeline")
        self.assertEqual(code, 200)
        self.assertEqual(len(body["steps"]), 8)
        self.assertEqual(body["total_cost"], 3022)
        for s in body["steps"]:
            self.assertTrue(s["label"], f"{s['name']} 缺中文名")
            self.assertTrue(s["desc"], f"{s['name']} 缺说明")

    def test_static_and_index(self):
        code, body = self.req("GET", "/")
        self.assertEqual(code, 200)
        self.assertIn("replaylane", body)
        code, body = self.req("GET", "/static/app.js")
        self.assertEqual(code, 200)
        self.assertIn("renderGantt", body)

    def test_stats_shape(self):
        code, body = self.req("GET", "/api/stats")
        self.assertEqual(code, 200)
        for k in ("tasks", "steps", "cache_hit_rate", "queue_depth", "gpu", "events"):
            self.assertIn(k, body)

    def test_unknown_task_is_404(self):
        code, body = self.req("GET", "/api/tasks/NOPE")
        self.assertEqual(code, 404)
        self.assertIn("error", body)


class TestTaskLifecycle(APITestCase):
    def test_submit_creates_eight_blocked_steps(self):
        tid = self.new_task("api_life1.mp4")
        code, body = self.req("GET", f"/api/tasks/{tid}")
        self.assertEqual(code, 200)
        self.assertEqual(body["task"]["status"], "QUEUED")
        # 详情要带解析后的 params，前端靠它判断"注入的故障有没有真的执行到"
        self.assertEqual(body["task"]["params"]["source"], "api_life1.mp4")
        self.assertEqual(len(body["steps"]), 8)
        self.assertTrue(all(s["status"] == "BLOCKED" for s in body["steps"]))

    def test_retry_rejected_with_409_when_running(self):
        tid = self.new_task("api_life2.mp4")
        code, body = self.req("POST", f"/api/tasks/{tid}/steps/restyle/retry")
        self.assertEqual(code, 409, "BLOCKED 的步骤不该允许重试")
        self.assertFalse(body["ok"])

    def test_retry_route_reaches_manual_retry(self):
        """回归：曾经因为下标错位，这个请求直接 404。"""
        tid = self.new_task("api_life3.mp4")
        with self.store.tx() as c:
            c.execute("UPDATE step_run SET status='ATTENTION', error_class='human_reject'"
                      " WHERE task_id=? AND step_name='qc_review'", (tid,))
        before = self.step(tid, "qc_review")
        code, body = self.req("POST", f"/api/tasks/{tid}/steps/qc_review/retry")
        self.assertEqual(code, 200, body)
        self.assertTrue(body["ok"])
        after = self.step(tid, "qc_review")
        self.assertEqual(after["status"], "QUEUED")
        # API 自己也要发号，否则等于绕开执行权那套论证
        self.assertEqual(after["fencing_token"], before["fencing_token"] + 1)

    def test_retried_non_gpu_step_gets_picked_up(self):
        """回归：非 GPU 步骤重试后 gpu_ready=0，不会再有人给它置 1，会永久卡队列。"""
        tid = self.new_task("api_life4.mp4")
        with self.store.tx() as c:
            c.execute("UPDATE step_run SET status='ATTENTION'"
                      " WHERE task_id=? AND step_name='qc_review'", (tid,))
        self.assertEqual(self.req("POST",
                                  f"/api/tasks/{tid}/steps/qc_review/retry")[0], 200)

        scheduler.tick(self.store)          # 调度器应当放行（gpu_mb=0）
        s = self.step(tid, "qc_review")
        self.assertEqual(s["gpu_ready"], 1, "非 GPU 步骤必须被直接放行")

        claim = Worker(self.store, "w_test").claim()
        self.assertIsNotNone(claim)
        self.assertEqual(claim["step_name"], "qc_review")

    def test_approve_rejected_when_not_waiting(self):
        tid = self.new_task("api_life7.mp4")
        code, body = self.req("POST", f"/api/tasks/{tid}/steps/qc_review/approve")
        self.assertEqual(code, 409, "只有停在 ATTENTION 的步骤才能被通过")
        self.assertFalse(body["ok"])

    def test_task_name_does_not_affect_cache_key(self):
        """名字只给人看：改了名字，指纹不该变，缓存还应该命中。"""
        a = self.new_task("api_name.mp4")
        self.req("POST", "/api/tasks", {"params": {"source": "api_name.mp4", "name": "第一次"}})
        ka = {r["step_name"]: r["step_key"] for r in
              self.store.q("SELECT step_name, step_key FROM step_run WHERE task_id=?", (a,))}
        b = self.new_task("api_name.mp4")
        kb = {r["step_name"]: r["step_key"] for r in
              self.store.q("SELECT step_name, step_key FROM step_run WHERE task_id=?", (b,))}
        self.assertEqual(ka, kb, "名字进了 step_key，缓存会无谓失效")

    def test_custom_name_is_returned(self):
        code, body = self.req("POST", "/api/tasks",
                              {"params": {"source": "api_name2.mp4"}, "name": "片头重绘"})
        self.assertEqual(code, 201)
        tid = body["task_id"]
        code, body = self.req("GET", f"/api/tasks/{tid}")
        self.assertEqual(body["task"]["name"], "片头重绘")
        code, body = self.req("GET", "/api/tasks?limit=5")
        self.assertIn("片头重绘", [t["name"] for t in body])

    def test_default_name_comes_from_params(self):
        tid = self.new_task("api_name3.mp4")
        code, body = self.req("GET", f"/api/tasks/{tid}")
        self.assertIn("api_name3", body["task"]["name"])

    def test_cancel_marks_flag_and_cascades(self):
        tid = self.new_task("api_life5.mp4")
        code, body = self.req("POST", f"/api/tasks/{tid}/cancel")
        self.assertEqual(code, 200)
        self.assertTrue(body["ok"])
        self.assertEqual(self.store.scalar(
            "SELECT cancel_requested FROM task WHERE task_id=?", (tid,)), 1)

        scheduler.tick(self.store)
        statuses = {r["status"] for r in
                    self.store.q("SELECT status FROM step_run WHERE task_id=?", (tid,))}
        self.assertEqual(statuses, {"CANCELLED"}, "取消后所有未开始的步骤都应终止")
        self.assertEqual(self.store.scalar(
            "SELECT status FROM task WHERE task_id=?", (tid,)), "CANCELLED")

    def test_cancel_twice_is_409(self):
        tid = self.new_task("api_life6.mp4")
        self.assertEqual(self.req("POST", f"/api/tasks/{tid}/cancel")[0], 200)
        # 任务已不在 QUEUED/RUNNING，第二次取消应当被拒
        with self.store.tx() as c:
            c.execute("UPDATE task SET status='SUCCEEDED' WHERE task_id=?", (tid,))
        self.assertEqual(self.req("POST", f"/api/tasks/{tid}/cancel")[0], 409)


class TestEventRoutes(APITestCase):
    def test_events_are_queryable_and_monotonic(self):
        tid = self.new_task("api_ev.mp4")
        code, body = self.req("GET", f"/api/tasks/{tid}/events")
        self.assertEqual(code, 200)
        self.assertTrue(body)
        seqs = [e["seq"] for e in body]
        self.assertEqual(seqs, sorted(set(seqs)))
        self.assertEqual(body[0]["type"], "task.submitted")

        code, body = self.req("GET", "/api/events?from_seq=0")
        self.assertEqual(code, 200)
        self.assertGreaterEqual(len(body), 1)

    def test_timeline_is_empty_but_valid_before_execution(self):
        tid = self.new_task("api_tl.mp4")
        code, body = self.req("GET", f"/api/tasks/{tid}/timeline")
        self.assertEqual(code, 200)
        self.assertIn("segments", body)
        self.assertIsInstance(body["segments"], list)


if __name__ == "__main__":
    unittest.main()
