/* replaylane 看板：原生 JS，无构建步骤。
   数据来源只有两个：REST 拉全量快照，SSE 收增量事件。
   界面不做自己的状态机——所有颜色都由后端返回的 status/segment 决定。 */

const $ = (s, r = document) => r.querySelector(s);
const el = (tag, cls, html) => {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (html !== undefined) n.innerHTML = html;
  return n;
};
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

const state = { selected: null, tasks: [], steps: [], attempts: [], timeline: null };
let refreshTimer = null;
let listTimer = null;

/* ---------------------------------------------------------------- 任务列表 */

async function loadTasks() {
  const rows = await (await fetch("/api/tasks?limit=30")).json();
  state.tasks = rows;
  const ul = $("#task-list");
  ul.innerHTML = "";
  $("#task-count").textContent = `${rows.length} 个`;
  if (!rows.length) {
    ul.appendChild(el("li", "empty", "还没有任务。先在上面提交一个。"));
    return;
  }
  for (const t of rows) {
    const li = el("li");
    li.dataset.taskId = t.task_id;
    if (t.task_id === state.selected) li.classList.add("active");
    const prog = t.progress || 0;
    const cls = t.status === "SUCCEEDED" ? "bar done" : t.status === "FAILED" ? "bar fail" : "bar";
    li.innerHTML = `
      <div class="trow">
        <span class="st ${esc(t.status)}">${esc(t.status)}</span>
        <span class="tid">${esc(t.task_id.slice(-8))}</span>
        <span class="tmeta">${prog}%</span>
      </div>
      <div class="${cls}"><i style="width:${prog}%"></i></div>`;
    li.onclick = () => selectTask(t.task_id);
    ul.appendChild(li);
  }
}

async function loadStats() {
  const s = await (await fetch("/api/stats")).json();
  const gpuTxt = (s.gpu || []).map((g) => `GPU${g.gpu_id} ${g.util_pct}%`).join(" · ");
  $("#chips").innerHTML = [
    `<span class="chip">队列 <b>${s.queue_depth}</b></span>`,
    `<span class="chip">执行中 <b>${s.running}</b></span>`,
    `<span class="chip">缓存命中 <b>${s.cache_hit_rate}%</b></span>`,
    `<span class="chip">${gpuTxt || "GPU —"}</span>`,
    `<span class="chip">事件 <b>${s.events}</b></span>`,
  ].join("");
}

/* ---------------------------------------------------------------- 任务详情 */

async function selectTask(id) {
  state.selected = id;
  document.querySelectorAll("#task-list li").forEach((li) => {
    li.classList.toggle("active", li.dataset.taskId === id);
  });
  await loadDetail();
  await loadEvents(true);
}

async function loadDetail() {
  if (!state.selected) return;
  const [detail, tl, atts] = await Promise.all([
    (await fetch(`/api/tasks/${state.selected}`)).json(),
    (await fetch(`/api/tasks/${state.selected}/timeline`)).json(),
    (await fetch(`/api/tasks/${state.selected}/attempts`)).json(),
  ]);
  if (detail.error) return;
  state.steps = detail.steps;
  state.timeline = tl;
  state.attempts = atts;

  $("#detail-empty").style.display = "none";
  $("#detail-body").style.display = "block";
  renderNotice(detail);
  const t = detail.task;
  $("#detail-id").innerHTML =
    `<span class="st ${esc(t.status)}">${esc(t.status)}</span>
     <span class="mono" style="margin-left:8px">${esc(t.task_id)}</span>
     <span style="margin-left:8px">进度 ${t.progress}%</span>`;
  renderGantt();
  renderSteps();
}

const stepLabel = (s) => (s.spec && s.spec.label) || s.step_name;
const stepDesc = (s) => (s.spec && s.spec.desc) || "";

function renderNotice(detail) {
  const box = $("#notice");
  const steps = detail.steps || [];
  const cached = steps.filter((s) => s.status === "CACHED").length;
  const total = steps.length;
  const executed = steps.filter((s) => s.status !== "CACHED" && s.status !== "BLOCKED").length;
  const faults = (detail.task && detail.task.params && detail.task.params.faults) || {};

  // 最容易被误解的一种情况：给某一步注入了故障，但这一步命中了缓存、
  // 根本没执行——故障自然没机会触发。不说清楚，用户会以为故障注入坏了。
  const missed = Object.keys(faults).filter((n) =>
    steps.some((s) => s.step_name === n && s.status === "CACHED"));
  if (missed.length) {
    box.style.display = "flex";
    box.className = "notice warn";
    box.innerHTML = `<b>故障没有生效</b>
      <span>${esc(missed.join("、"))} 命中了缓存，这一步没有执行，注入的故障也就没机会触发。
      换个素材名或风格（改变输入 → 改变指纹）它才会真的跑起来。</span>`;
    return;
  }

  if (cached === total) {
    // 零执行的任务在甘特上是空的，不解释一句会让人以为系统没反应
    box.style.display = "flex";
    box.className = "notice ok";
    box.innerHTML = `<b>全部 ${total} 步命中缓存</b>
      <span>这次没有执行任何计算，产物直接复用（0 秒完成）。想看到执行过程：换个素材名、换个风格或 seed，或者勾选一个故障注入。</span>`;
  } else if (cached > 0) {
    box.style.display = "flex";
    box.className = "notice ok";
    box.innerHTML = `<b>${cached} 步命中缓存</b>
      <span>其余 ${executed} 步实际执行。绿色 CACHED 的步骤没有消耗算力。</span>`;
  } else {
    box.style.display = "none";
  }
}

function renderGantt() {
  const segs = (state.timeline && state.timeline.segments) || [];
  const g = $("#gantt");
  g.innerHTML = "";
  const maxT = Math.max(1000, ...segs.map((s) => s.t1));
  const pct = (v) => (v / maxT) * 100;

  for (const st of state.steps) {
    const row = el("div", "grow");
    const lab = el("div", "glabel", esc(stepLabel(st)));
    lab.title = `${st.step_name} · ${stepDesc(st)}`;
    row.appendChild(lab);
    const track = el("div", "gtrack");
    const mine = segs.filter((s) => s.step === st.step_name);
    for (const s of mine) {
      const w = Math.max(0, pct(s.t1) - pct(s.t0));
      // 降级跑出来的执行段用斜纹标出来：同一道工序、换了跑法
      const degraded = s.type === "running" && s.variant && s.variant !== "base";
      const cls = degraded ? "running degraded" : s.type;
      const tip = `${s.type}${degraded ? " · " + s.variant : ""}` +
                  `  ${(s.t0 / 1000).toFixed(1)}s → ${(s.t1 / 1000).toFixed(1)}s`;
      if (w < 0.15) {
        const m = el("div", `gmark ${cls}`);
        m.style.left = pct(s.t0) + "%";
        m.title = tip;
        track.appendChild(m);
      } else {
        const b = el("div", `gseg ${cls}`);
        b.style.left = pct(s.t0) + "%";
        b.style.width = w + "%";
        b.title = tip;
        track.appendChild(b);
      }
    }
    if (!mine.length) track.appendChild(el("div", "gseg ready", "")).style.width = "0%";
    row.appendChild(track);
    g.appendChild(row);
  }

  const ticks = $("#ticks");
  ticks.innerHTML = "";
  const n = 5;
  for (let i = 0; i <= n; i++) {
    ticks.appendChild(el("span", "", `${((maxT * i) / n / 1000).toFixed(1)}s`));
  }
}

function renderSteps() {
  const tb = $("#step-rows");
  tb.innerHTML = "";
  for (const s of state.steps) {
    const tr = el("tr");
    const p = s.progress_total ? Math.round((s.progress_done / s.progress_total) * 100) : 0;
    const canRetry = ["FAILED", "ATTENTION", "CANCELLED"].includes(s.status);
    tr.title = stepDesc(s);
    tr.innerHTML = `
      <td><div class="sname">${esc(stepLabel(s))}</div>
          <div class="sen">${esc(s.step_name)}</div></td>
      <td><span class="st ${esc(s.status)}">${esc(s.status)}</span></td>
      <td class="num">${s.attempt_no > 1 ? `<span class="att" title="重试过">${s.attempt_no}</span>` : s.attempt_no}</td>
      <td class="num">${s.fencing_token > 1 ? `<span class="att" title="换过执行者">${s.fencing_token}</span>` : s.fencing_token}</td>
      <td class="mono ${s.variant && s.variant !== "base" ? "att" : "dim"}">${esc(s.variant)}</td>
      <td class="num">${s.status === "RUNNING" ? p + "%" : s.status === "SUCCEEDED" || s.status === "CACHED" ? "100%" : "—"}</td>
      <td class="mono dim">${esc((s.output_digest || "").slice(0, 10))}</td>
      <td>${canRetry ? `<button class="tiny ghost" data-step="${esc(s.step_name)}">重试</button>` : ""}</td>`;
    tr.querySelector("td:first-child").onclick = () => showAttempts(s.step_name);
    const btn = tr.querySelector("button");
    if (btn) btn.onclick = (e) => { e.stopPropagation(); retryStep(s.step_name); };
    tb.appendChild(tr);
  }
}

function showAttempts(stepName) {
  const rows = state.attempts.filter((a) => a.step_name === stepName);
  const wrap = $("#attempts-wrap");
  if (!rows.length) {
    wrap.innerHTML = `<div class="hint">${esc(stepName)} 还没有任何 attempt（可能命中了缓存，从未执行）。</div>`;
    return;
  }
  const dur = (a) => {
    if (a.ended_mono_ms && a.started_mono_ms) return ((a.ended_mono_ms - a.started_mono_ms) / 1000).toFixed(1) + "s";
    if (a.status === "ABANDONED") return "≥?（进程被强杀，结束时刻未知）";
    return "—";
  };
  wrap.innerHTML = `
    <table class="attempts">
      <thead><tr><th>#</th><th>状态</th><th>variant</th><th>worker</th><th>token</th>
      <th>耗时</th><th>error</th><th>observed</th></tr></thead>
      <tbody>${rows.map((a) => `
        <tr>
          <td>${a.attempt_no}</td>
          <td><span class="st ${esc(a.status)}">${esc(a.status)}</span></td>
          <td>${esc(a.variant)}</td>
          <td>${esc(a.worker_id)}</td>
          <td>${a.fencing_token}</td>
          <td>${dur(a)}</td>
          <td class="dim">${esc(a.error_class || "")}</td>
          <td class="dim">${esc((a.observed_at || "").slice(11, 23))}</td>
        </tr>`).join("")}</tbody>
    </table>`;
}

async function retryStep(name) {
  const r = await fetch(`/api/tasks/${state.selected}/steps/${name}/retry`, { method: "POST" });
  if (!r.ok) {
    // 把后端给的原因显示出来：404 是路由/对象不存在，409 才是状态不允许
    let msg = `HTTP ${r.status}`;
    try { const j = await r.json(); if (j.error) msg = j.error; } catch { /* 空响应 */ }
    alert(r.status === 409 ? `当前状态不允许重试（${msg}）` : `重试失败：${msg}`);
    return;
  }
  await loadDetail();
}

/* ---------------------------------------------------------------- 事件流 */

async function loadEvents(reset) {
  const ul = $("#events");
  if (reset) ul.innerHTML = "";
  const rows = await (await fetch(`/api/events?from_seq=0`)).json();
  const mine = state.selected ? rows.filter((e) => e.task_id === state.selected) : rows;
  ul.innerHTML = "";
  for (const e of mine.slice(-120).reverse()) {
    addEventRow(e, ul, true);
  }
}

function addEventRow(e, ul, append) {
  const errish = /failed|expired|attention|cancel/.test(e.type);
  const okish = /succeeded|cached/.test(e.type);
  const li = el("li");
  li.innerHTML = `<span class="sq">${e.seq}</span>
    <span class="ty ${errish ? "err" : okish ? "ok" : ""}">${esc(e.type)}</span>
    <span class="pl">${esc((e.payload_json || "").slice(0, 160))}</span>`;
  if (append) ul.appendChild(li);
  else ul.prepend(li);
  while (ul.children.length > 160) ul.removeChild(ul.lastChild);
}

function connectSSE() {
  const es = new EventSource("/api/stream?from_seq=0");
  es.onmessage = (m) => {
    let e;
    try { e = JSON.parse(m.data); } catch { return; }
    if (!state.selected || e.task_id === state.selected) {
      addEventRow(e, $("#events"), false);
    }
    scheduleRefresh();
  };
  es.onerror = () => { /* EventSource 自己会带 Last-Event-ID 重连 */ };
}

function scheduleRefresh() {
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(() => {
    loadStats();
    loadTasks();
    if (state.selected) loadDetail();
  }, 220);
  clearTimeout(listTimer);
  listTimer = setTimeout(loadStats, 900);
}

/* ---------------------------------------------------------------- 提交 */

$("#submit-form").onsubmit = async (ev) => {
  ev.preventDefault();
  const f = ev.target;
  const faults = {};
  const oom = f.f_oom.checked;
  const retry = f.f_retry.checked;
  if (oom) faults.matting = { fail_times: 1, class: "oom" };
  else if (retry) faults.matting = { fail_times: 2, class: "transient" };
  if (f.f_kill.checked) faults.restyle = { kill_at: 0.45, kill_on_attempt: 1 };
  if (f.f_human.checked) faults.qc_review = { fail_times: 1, class: "human_reject" };

  const body = {
    params: { source: f.source.value, style: f.style.value, faults },
  };
  if (f.seed.value) body.params.nondet_seed = Number(f.seed.value);
  const r = await fetch("/api/tasks", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const out = await r.json();
  await loadTasks();
  if (out.task_id) selectTask(out.task_id);
};

/* ---------------------------------------------------------------- 启动 */

(async function boot() {
  await loadStats();
  await loadTasks();
  connectSSE();
  setInterval(() => { loadStats(); loadTasks(); }, 3000);
})();
