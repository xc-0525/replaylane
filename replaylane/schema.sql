-- replaylane / schema.sql
-- 全部 DDL 一个文件读完：任何"系统里有哪些状态、谁能写、写到哪"的问题都在这里。
-- 设计要点见 docs/01-架构与边界设计.md；实现层面的偏离记录在 docs/02-实现决策.md。
--
-- 时间列有两套，绝不混用：
--   *_at  TEXT  ISO8601 墙钟，给人看、写日志、算"多久以前"
--   *mono_ms INTEGER 单调毫秒，只用于算差值（甘特条宽、执行时长）
-- 排序一律用 seq，绝不用时间列（NTP 回拨会让时间倒流）。

PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;      -- 演示机是本地盘，FULL 换"断电也不丢提交"
PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------- task
CREATE TABLE IF NOT EXISTS task (
  task_id         TEXT PRIMARY KEY,
  pipeline        TEXT    NOT NULL,
  version         INTEGER NOT NULL,
  params_json     TEXT    NOT NULL,   -- 含 nondet_seed：同任务内所有非确定性步骤共享
  status          TEXT    NOT NULL,
  priority        INTEGER NOT NULL DEFAULT 5,
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  submitted_at    TEXT    NOT NULL,
  started_at      TEXT    NULL,
  ended_at        TEXT    NULL
);
CREATE INDEX IF NOT EXISTS ix_task_status ON task(status, submitted_at DESC);

-- ------------------------------------------------------------- step_spec
-- 代码即配置：管线版本号 + 步骤名唯一确定一份声明，任务只引用它。
CREATE TABLE IF NOT EXISTS step_spec (
  pipeline      TEXT    NOT NULL,
  version       INTEGER NOT NULL,
  step_name     TEXT    NOT NULL,
  deps_json     TEXT    NOT NULL,     -- ["frames","probe"]
  cost          INTEGER NOT NULL,     -- 进度权重（不是耗时，也不是钱）
  gpu_mb        INTEGER NOT NULL DEFAULT 0,
  gpu_mode      TEXT    NOT NULL DEFAULT 'shared',
  retry_preset  TEXT    NOT NULL,     -- fast | oom_aware | slow | never
  idem          TEXT    NOT NULL,     -- det | nondet | human
  checkpoint    TEXT    NULL,         -- NULL | per_shot | per_sec_window
  timeout_s     INTEGER NULL,
  PRIMARY KEY (pipeline, version, step_name)
);

-- -------------------------------------------------------------- step_run
-- 恢复的最小单位。一个 (task, step_name) 一行，attempt 才是物理执行。
CREATE TABLE IF NOT EXISTS step_run (
  step_run_id      TEXT PRIMARY KEY,
  task_id          TEXT    NOT NULL REFERENCES task(task_id) ON DELETE CASCADE,
  step_name        TEXT    NOT NULL,
  status           TEXT    NOT NULL,
  step_key         TEXT    NULL,          -- 内容寻址标识，READY 时才算出
  seed             INTEGER NULL,          -- nondet 步骤的采样种子（来自 task.params）
  params_json      TEXT    NOT NULL,      -- 归一化后的语义输入
  spec_cost        INTEGER NOT NULL DEFAULT 0,
  attempt_no       INTEGER NOT NULL DEFAULT 0,  -- 已发放的 attempt 数（只增）
  lease_id         TEXT    NULL,
  fencing_token    INTEGER NOT NULL DEFAULT 0,
  gpu_mb           INTEGER NOT NULL DEFAULT 0,
  gpu_ready        INTEGER NOT NULL DEFAULT 0,  -- 0 = 显存还没批下来，别抢
  priority         INTEGER NOT NULL DEFAULT 5,
  queued_at        TEXT    NULL,
  ready_seq        INTEGER NULL,          -- 入队序号，同优先级下的稳定顺序
  requeued_at      TEXT    NULL,
  progress_done    INTEGER NOT NULL DEFAULT 0,
  progress_total   INTEGER NOT NULL DEFAULT 0,
  last_progress_at TEXT    NULL,          -- 卡住检测的比较基准
  stall_after_ms   INTEGER NOT NULL DEFAULT 300000,
  variant          TEXT    NOT NULL DEFAULT 'base',  -- OOM 降级阶梯的当前位置，不进 key
  retry_at         TEXT    NULL,          -- RETRY_WAIT 的唤醒时刻
  output_digest    TEXT    NULL,
  error_class      TEXT    NULL,
  blocked_reason   TEXT    NULL,          -- external_unavailable / cache_artifact_missing
  UNIQUE (task_id, step_name)
);
-- 领取队列：status+gpu_ready 是谓词，priority+ready_seq 是排序。
-- 老化在 SQL 里现算（见 scheduler.claim_sql），所以索引里放原始 priority 就够。
CREATE INDEX IF NOT EXISTS ix_step_claim ON step_run(status, gpu_ready, priority, ready_seq);
CREATE INDEX IF NOT EXISTS ix_step_task  ON step_run(task_id, step_name);
CREATE INDEX IF NOT EXISTS ix_step_key   ON step_run(step_key);

-- ---------------------------------------------------------------- lease
-- 崩溃检测的唯一依据。领取时 INSERT，提交/回收/取消时写 closed_at，永不 DELETE。
-- 部分唯一索引 = 断言 A1 的实现：一个 step_run 至多一个未关闭租约。
CREATE TABLE IF NOT EXISTS lease (
  lease_id     TEXT PRIMARY KEY,
  step_run_id  TEXT    NOT NULL,
  worker_id    TEXT    NOT NULL,
  fencing_token INTEGER NOT NULL,
  acquired_at  TEXT    NOT NULL,
  last_beat    TEXT    NOT NULL,
  closed_at    TEXT    NULL,
  close_reason TEXT    NULL           -- committed | failed | expired | cancelled | stolen
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_lease_open ON lease(step_run_id) WHERE closed_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_lease_beat ON lease(last_beat) WHERE closed_at IS NULL;

-- --------------------------------------------------------------- attempt
-- append-only：离开 RUNNING 之后永不修改（唯一的例外是 ABANDONED 的 observed_at）。
CREATE TABLE IF NOT EXISTS attempt (
  attempt_id     TEXT PRIMARY KEY,
  step_run_id    TEXT    NOT NULL REFERENCES step_run(step_run_id) ON DELETE CASCADE,
  attempt_no     INTEGER NOT NULL,
  worker_id      TEXT    NOT NULL,
  lease_id       TEXT    NULL,
  fencing_token  INTEGER NULL,
  status         TEXT    NOT NULL,     -- RUNNING/SUCCEEDED/FAILED/TIMEOUT/ABANDONED
  variant        TEXT    NOT NULL DEFAULT 'base',  -- OOM 降级阶梯，不进 step_key
  started_at     TEXT    NOT NULL,
  started_mono_ms INTEGER NOT NULL,
  ended_at       TEXT    NULL,         -- ABANDONED 时故意留 NULL：我们不知道它什么时候死的
  ended_mono_ms  INTEGER NULL,
  observed_at    TEXT    NULL,         -- ABANDONED 时记 sweeper 的判定时刻
  error_class    TEXT    NULL,
  error_msg      TEXT    NULL,
  exit_code      INTEGER NULL,
  UNIQUE (step_run_id, attempt_no)
);
CREATE INDEX IF NOT EXISTS ix_attempt_step ON attempt(step_run_id, attempt_no);

-- ------------------------------------------------------------- artifact
CREATE TABLE IF NOT EXISTS artifact (
  artifact_id   TEXT PRIMARY KEY,
  sha256        TEXT    NOT NULL UNIQUE,
  rel_path      TEXT    NOT NULL,      -- 相对 data/artifacts/
  size_bytes    INTEGER NOT NULL,
  producer      TEXT    NULL,
  created_at    TEXT    NOT NULL,
  last_access_at TEXT   NULL           -- GC 的老化依据；命中缓存时更新
);

-- 引用有两种，GC 必须同时看：任务引用 + 缓存索引引用。
CREATE TABLE IF NOT EXISTS artifact_ref (
  step_run_id TEXT NOT NULL,
  task_id     TEXT NOT NULL,
  sha256      TEXT NOT NULL,
  PRIMARY KEY (step_run_id, sha256)
);
CREATE INDEX IF NOT EXISTS ix_ref_sha ON artifact_ref(sha256);

CREATE TABLE IF NOT EXISTS step_cache (
  step_key    TEXT NOT NULL,
  sha256      TEXT NOT NULL,
  step_name   TEXT NOT NULL,
  created_at  TEXT NOT NULL,
  PRIMARY KEY (step_key, sha256)
);
CREATE INDEX IF NOT EXISTS ix_cache_sha ON step_cache(sha256);

-- ------------------------------------------------------------ checkpoint
-- 分片绑定 (step_key, seed, attempt_no)：读的时候只认与当前一致的分片，
-- 否则非确定性步骤会拿两个 seed 的产物拼成一条时间线。
CREATE TABLE IF NOT EXISTS checkpoint (
  step_run_id  TEXT    NOT NULL REFERENCES step_run(step_run_id) ON DELETE CASCADE,
  shard        INTEGER NOT NULL,
  step_key     TEXT    NOT NULL,
  seed         INTEGER NULL,           -- determinism 步骤没有 seed，用 NULL 而不是 0
  attempt_no   INTEGER NOT NULL,
  payload_json TEXT    NOT NULL,
  artifact_sha TEXT    NULL,
  created_at   TEXT    NOT NULL,
  PRIMARY KEY (step_run_id, shard)
);

-- --------------------------------------------------------------- gpu
CREATE TABLE IF NOT EXISTS gpu (
  gpu_id     INTEGER PRIMARY KEY,
  total_mb   INTEGER NOT NULL,
  headroom   REAL    NOT NULL DEFAULT 0.9
);

CREATE TABLE IF NOT EXISTS gpu_lease (
  gpu_lease_id TEXT PRIMARY KEY,
  gpu_id       INTEGER NOT NULL,
  step_run_id  TEXT    NOT NULL,
  worker_id    TEXT    NULL,            -- 断电后按 worker 判不出孤儿，仍保留用于排查
  fencing_token INTEGER NULL,
  vram_mb      INTEGER NOT NULL,
  mode         TEXT    NOT NULL,        -- exclusive | shared
  acquired_at  TEXT    NOT NULL,
  released_at  TEXT    NULL             -- NULL = 仍占用
);
CREATE INDEX IF NOT EXISTS ix_gpu_live ON gpu_lease(gpu_id, released_at);

-- --------------------------------------------------------------- event
-- append-only 真源。投影可以从它重建，但它永不从投影重建。
CREATE TABLE IF NOT EXISTS event (
  seq         INTEGER PRIMARY KEY AUTOINCREMENT,
  ts          TEXT    NOT NULL,
  mono_ms     INTEGER NOT NULL,
  type        TEXT    NOT NULL,
  task_id     TEXT    NULL,
  step_run_id TEXT    NULL,
  attempt_id  TEXT    NULL,
  payload_json TEXT   NULL
);
CREATE INDEX IF NOT EXISTS ix_event_task ON event(task_id, seq);
CREATE INDEX IF NOT EXISTS ix_event_type ON event(type, seq);
