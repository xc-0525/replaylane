"""命令行入口：一个进程能演全部角色，也能拆开跑。

  python -m replaylane.cli demo            # 一键：调度+2个worker+sweeper，跑一个任务
  python -m replaylane.cli serve           # 只起 HTTP（内含调度与 sweeper 线程）
  python -m replaylane.cli worker --id w1  # 单独起一个 worker
  python -m replaylane.cli submit          # 提交一个任务并打印 id
  python -m replaylane.cli status T...     # 打印任务与步骤状态
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time

from . import gpu, scheduler, sweeper
from .db import Store, init_db, loads, now_iso
from .model import STEP_TERMINAL
from .pipeline import STEP_BY_NAME, STEPS, TOTAL_COST
from .worker import Worker

DEFAULT_DB = os.path.join(os.getcwd(), "data", "replaylane.db")
DEFAULT_ART = os.path.join(os.getcwd(), "data", "artifacts")


def _open(args) -> Store:
    return init_db(args.db, args.artifacts)


def _progress(store, task_id: str) -> int:
    rows = store.q("SELECT spec_cost, status, progress_done, progress_total"
                   " FROM step_run WHERE task_id=?", (task_id,))
    done = 0
    for r in rows:
        if r["status"] in ("SUCCEEDED", "CACHED"):
            done += r["spec_cost"]
        elif r["status"] == "RUNNING" and r["progress_total"]:
            done += r["spec_cost"] * r["progress_done"] / r["progress_total"]
    pct = int(round(done / TOTAL_COST * 100))
    # 有非终态步骤时封顶 99%：进度条不许比任务先到终点
    if any(r["status"] not in STEP_TERMINAL for r in rows):
        pct = min(pct, 99)
    return pct


def _json_or_die(text: str) -> dict:
    """JSON 解析失败时给一句人话，而不是甩一个 traceback。

    最常见的触发原因：PowerShell 调原生程序时会把参数里的双引号吞掉，
    于是 `'{"a":1}'` 到 python 手里变成 `{a:1}`。
    """
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SystemExit(
            f"--params 不是合法 JSON（{exc.msg}，第 {exc.pos} 个字符）。\n"
            "  最常见的原因：PowerShell 会吞掉参数里的双引号。三个办法：\n"
            "    1) 不用 JSON，直接写：--fault matting:transient:2\n"
            "    2) 把 JSON 存成文件：--params-file params.json\n"
            "    3) 用 cmd 而不是 PowerShell，或把双引号写成 \\\"\n"
        )


def _build_params(args) -> dict:
    """把 --params-file / --params / --fault 合成一个 dict。"""
    params: dict = {}
    pf = getattr(args, "params_file", None)
    if pf:
        with open(pf, "r", encoding="utf-8") as f:
            params.update(json.load(f))
    raw = getattr(args, "params", None)
    if raw:
        params.update(_json_or_die(raw))
    _apply_faults(getattr(args, "fault", None), params)
    source = getattr(args, "source", None)
    if source:
        params["source"] = source
    name = getattr(args, "name", None)
    if name:
        params["name"] = name
    if getattr(args, "human_gate", False):
        params["auto_approve"] = False
    elif getattr(args, "auto_approve", False):
        params["auto_approve"] = True
    seed = getattr(args, "seed", None)
    if seed is not None:
        params["nondet_seed"] = seed
    return params


def _apply_faults(specs, params: dict) -> None:
    """--fault step:class[:times]，kill 用 --fault step:kill:at[:attempt]。

    例：--fault matting:transient:2 --fault restyle:kill:0.45
    """
    if not specs:
        return
    faults = params.setdefault("faults", {})
    for raw in specs:
        parts = raw.split(":")
        step = parts[0]
        kind = parts[1] if len(parts) > 1 else "transient"
        if kind == "kill":
            at = float(parts[2]) if len(parts) > 2 else 0.45
            attempt = int(parts[3]) if len(parts) > 3 else 1
            faults[step] = {"kill_at": at, "kill_on_attempt": attempt}
        else:
            times = int(parts[2]) if len(parts) > 2 else 1
            faults[step] = {"fail_times": times, "class": kind}


def cmd_demo(args) -> int:
    store = _open(args)
    params = _build_params(args)
    # 一键演示默认自动通过人工环节（否则会一直停在 ATTENTION 等超时）；
    # 想看真正的闸门就加 --human-gate。
    params.setdefault("auto_approve", not getattr(args, "human_gate", False))
    task_id = scheduler.submit(store, params)
    print(f"task {task_id} submitted (db={store.db_path})")

    stop = threading.Event()
    threading.Thread(target=_sched_loop, args=(store, stop), daemon=True).start()
    threading.Thread(target=_sweeper_loop, args=(store, stop), daemon=True).start()
    # worker 用真子进程：故障注入里的 kill 会 os._exit，跑在线程里会把演示进程一起带走
    procs = [_spawn_worker(args, i) for i in range(1, args.workers + 1)]

    t0 = time.time()
    last = -1
    try:
        while time.time() - t0 < args.timeout:
            st = store.scalar("SELECT status FROM task WHERE task_id=?", (task_id,))
            pct = _progress(store, task_id)
            if pct != last:
                print(f"  {st:<10} {pct:>3}%  {time.time()-t0:5.1f}s", flush=True)
                last = pct
            if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(0.3)
    finally:
        stop.set()
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=3)
            except Exception:
                p.kill()
    time.sleep(0.2)
    print_summary(store, task_id)
    st = store.scalar("SELECT status FROM task WHERE task_id=?", (task_id,))
    return 0 if st == "SUCCEEDED" else 1


def _spawn_worker(args, i: int):
    import subprocess
    cmd = [sys.executable, "-m", "replaylane.cli", "--db", args.db,
           "--artifacts", args.artifacts, "worker", "--id", f"w{i}"]
    return subprocess.Popen(cmd)


def _sweeper_loop(store: Store, stop: threading.Event):
    while not stop.is_set():
        try:
            sweeper.reclaim_expired(store)
            sweeper.detect_stalled(store)
        except Exception:
            pass
        time.sleep(1.0)


def _sched_loop(store: Store, stop: threading.Event):
    while not stop.is_set():
        try:
            scheduler.tick(store)
        except Exception:                   # 调度器不能因为一次异常就死
            import traceback
            traceback.print_exc()
        time.sleep(0.3)


def _worker_loop(store: Store, wid: str, stop: threading.Event):  # pragma: no cover
    from .worker import Worker
    w = Worker(store, wid)
    while not stop.is_set():
        claim = w.claim()
        if claim is None:
            time.sleep(0.2)
            continue
        result = w.run_one(claim)
        print(f"  [worker {wid}] {claim['step_name']}#{claim['attempt_no']} "
              f"tok={claim['fencing_token']} -> {result}", flush=True)


def print_summary(store: Store, task_id: str) -> None:
    t = store.q1("SELECT * FROM task WHERE task_id=?", (task_id,))
    print(f"\n== task {task_id} ==")
    print(f"status={t['status']}  params={t['params_json']}")
    print(f"{'step':<12}{'status':<12}{'tok':>4}{'att':>4}{'variant':>10}  digest")
    for r in store.q("SELECT * FROM step_run WHERE task_id=? ORDER BY rowid", (task_id,)):
        print(f"{r['step_name']:<12}{r['status']:<12}{r['fencing_token']:>4}"
              f"{r['attempt_no']:>4}{r['variant']:>10}  {(r['output_digest'] or '')[:12]}")
    print(f"progress={_progress(store, task_id)}%  events={store.scalar('SELECT COUNT(*) FROM event WHERE task_id=?', (task_id,))}")
    print("\n-- last 12 events --")
    for e in store.q("SELECT seq, type, payload_json FROM event WHERE task_id=?"
                     " ORDER BY seq DESC LIMIT 12", (task_id,))[::-1]:
        print(f"  #{e['seq']:<5}{e['type']:<18}{(e['payload_json'] or '')[:90]}")


def cmd_status(args) -> int:
    store = _open(args)
    for tid in args.task_ids:
        print_summary(store, tid)
    return 0


def cmd_submit(args) -> int:
    store = _open(args)
    params = _build_params(args)
    print(scheduler.submit(store, params, priority=args.priority))
    return 0


def cmd_worker(args) -> int:
    store = _open(args)
    from .worker import run_forever
    run_forever(store, args.id, poll_s=0.5)
    return 0


def cmd_serve(args) -> int:
    from .server import serve
    store = _open(args)
    serve(store, host=args.host, port=args.port)
    return 0


def cmd_verify(args) -> int:
    from . import verify
    store = _open(args)
    tasks = ([args.task] if args.task else
             [r["task_id"] for r in store.q("SELECT task_id FROM task")])
    bad = 0
    for tid in tasks:
        problems = verify.check_task(store, tid)
        if problems:
            bad += 1
            print(f"x {tid}")
            for pr in problems:
                print("   ", pr)
    print(f"\nchecked {len(tasks)} task(s), {bad} inconsistent")
    return 0 if bad == 0 else 1


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="replaylane", description="可观测任务管线")
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--artifacts", default=DEFAULT_ART)
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("demo", help="一键演示")
    d.add_argument("--workers", type=int, default=2)
    d.add_argument("--timeout", type=float, default=120)
    d.add_argument("--params", default=None, help="JSON 字符串（PowerShell 下注意引号）")
    d.add_argument("--params-file", default=None, help="JSON 文件路径，避免命令行引号问题")
    d.add_argument("--fault", action="append", metavar="STEP:CLASS[:N]",
                   help="注入故障，可重复；kill 写 STEP:kill:比例[:第几次]。"
                        "例：--fault matting:transient:2 --fault restyle:kill:0.45")
    d.add_argument("--source", default=None, help="素材名，换一个等于换一个新任务（不会命中上次的缓存）")
    d.add_argument("--human-gate", action="store_true",
                   help="人工环节真的停下来等人（默认自动通过，方便一键演示）")
    d.add_argument("--name", default=None, help="任务名，只给人看，不影响缓存")
    d.add_argument("--seed", type=int, default=None)
    d.set_defaults(func=cmd_demo)

    s = sub.add_parser("serve", help="起 HTTP 服务")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8000)
    s.set_defaults(func=cmd_serve)

    w = sub.add_parser("worker")
    w.add_argument("--id", default="w1")
    w.set_defaults(func=cmd_worker)

    sb = sub.add_parser("submit")
    sb.add_argument("--params", default=None)
    sb.add_argument("--params-file", default=None)
    sb.add_argument("--fault", action="append", metavar="STEP:CLASS[:N]")
    sb.add_argument("--source", default=None)
    sb.add_argument("--auto-approve", action="store_true",
                    help="人工环节自动通过（默认会停在 ATTENTION 等人）")
    sb.add_argument("--name", default=None, help="任务名，只给人看，不影响缓存")
    sb.add_argument("--seed", type=int, default=None)
    sb.add_argument("--priority", type=int, default=5)
    sb.set_defaults(func=cmd_submit)

    st = sub.add_parser("status")
    st.add_argument("task_ids", nargs="+")
    st.set_defaults(func=cmd_status)

    vf = sub.add_parser("verify", help="用事件重放校验投影")
    vf.add_argument("--task", default=None)
    vf.set_defaults(func=cmd_verify)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
