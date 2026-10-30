"""状态枚举、合法转移表、失败分类与重试裁决。

这个文件是「谁可以变成什么」的唯一真源。测试直接遍历这里的两张转移表，
而不是逐个场景手写断言——表变了，测试自动跟着变。
"""
from __future__ import annotations

# ---------------------------------------------------------------- 状态

STEP_STATUSES = (
    "BLOCKED", "READY", "CACHED", "QUEUED", "RUNNING",
    "RETRY_WAIT", "ATTENTION", "SUCCEEDED", "FAILED", "CANCELLED",
)

STEP_TERMINAL = frozenset({"CACHED", "SUCCEEDED", "FAILED", "CANCELLED"})

STEP_TRANSITIONS: dict[str, frozenset[str]] = {
    "BLOCKED": frozenset({"READY", "CANCELLED"}),
    "READY": frozenset({"CACHED", "QUEUED", "CANCELLED"}),
    "QUEUED": frozenset({"RUNNING", "CANCELLED"}),
    "RUNNING": frozenset({"SUCCEEDED", "RETRY_WAIT", "FAILED", "ATTENTION", "QUEUED", "CANCELLED"}),
    "RETRY_WAIT": frozenset({"QUEUED", "CANCELLED", "FAILED"}),
    "ATTENTION": frozenset({"QUEUED", "FAILED", "CANCELLED"}),
    "SUCCEEDED": frozenset(),          # 严格不可变：产物已持久化
    "CACHED": frozenset(),             # 严格不可变：产物缺失在 READY 判定时就当未命中
    "FAILED": frozenset({"QUEUED"}),   # 仅 API 人工重试可离开
    "CANCELLED": frozenset(),
}

TASK_TRANSITIONS: dict[str, frozenset[str]] = {
    "QUEUED": frozenset({"RUNNING", "SUCCEEDED", "FAILED", "CANCELLING", "CANCELLED"}),
    "RUNNING": frozenset({"SUCCEEDED", "FAILED", "CANCELLING"}),
    "CANCELLING": frozenset({"CANCELLED"}),
    "SUCCEEDED": frozenset(),
    "FAILED": frozenset({"QUEUED"}),   # 人工重试某步骤后重新进入调度
    "CANCELLED": frozenset(),
}


class IllegalTransition(Exception):
    def __init__(self, kind: str, src: str, dst: str):
        super().__init__(f"illegal {kind} transition {src} -> {dst}")
        self.kind, self.src, self.dst = kind, src, dst


def check_step_transition(src: str, dst: str) -> None:
    if dst not in STEP_TRANSITIONS.get(src, frozenset()):
        raise IllegalTransition("step_run", src, dst)


def check_task_transition(src: str, dst: str) -> None:
    if src == dst:
        return
    if dst not in TASK_TRANSITIONS.get(src, frozenset()):
        raise IllegalTransition("task", src, dst)


def derive_task_status(statuses, cancel_requested: bool) -> str:
    """Task 状态是推导出来的，不是被谁直接写的。

    这里覆盖了两条容易被漏掉的边界：
      * 全部步骤 CACHED（零实际执行）→ 直接 SUCCEEDED，不需要先经过 RUNNING；
      * 还有非终态步骤时，只有两种情况：全 BLOCKED（=QUEUED）或已经动起来了（=RUNNING）。
    """
    if not statuses:
        return "QUEUED"
    pending = [s for s in statuses if s not in STEP_TERMINAL]
    if pending:
        if cancel_requested:
            return "CANCELLING"
        return "QUEUED" if all(s == "BLOCKED" for s in statuses) else "RUNNING"
    if cancel_requested:
        return "CANCELLED"
    if any(s == "FAILED" for s in statuses):
        return "FAILED"
    if any(s == "CANCELLED" for s in statuses):
        return "CANCELLED"
    return "SUCCEEDED"


# ------------------------------------------------------------ 失败分类
#
# 分类表是「类别 -> 动作」的唯一真源。步骤上的 retry 预设只声明三件事：
# 允许重试的类别白名单、退避曲线、同一 variant 的失败次数上限。
# 两者冲突时取严（白名单为空 = 不重试）。
#
# 注意 retry 与 degrade 是两件事，各有一套预算：
#   retry   = 同参数再跑一次（受 preset.max_same_variant 约束）
#   degrade = 换 variant 再跑（受 OOM 阶梯约束，restyle 的 retry=never 也允许降级）

FAILURE_CLASSES = (
    "transient", "external_429", "timeout", "oom",
    "invalid_input", "artifact_io", "nondet_crash", "human_reject", "cancel", "bug",
)

PRESETS: dict[str, dict] = {
    "fast": dict(allowed=frozenset({"transient", "timeout", "artifact_io", "external_429"}),
                 max_same_variant=8, base_s=1, cap_s=30),
    "oom_aware": dict(allowed=frozenset({"transient", "timeout", "artifact_io", "external_429"}),
                      max_same_variant=3, base_s=2, cap_s=120),
    "slow": dict(allowed=frozenset({"transient", "artifact_io"}),
                 max_same_variant=2, base_s=30, cap_s=1800),
    "never": dict(allowed=frozenset(), max_same_variant=0, base_s=0, cap_s=0),
}

VARIANT_LADDER = ("base", "bs_half", "tiled", "small_model")
MAX_VARIANT_LEVEL = len(VARIANT_LADDER) - 1


class Failure(Exception):
    """步骤内抛这个，worker 才知道该怎么处理；其他异常一律当 bug。"""

    def __init__(self, cls: str, msg: str = "", exit_code: int | None = None):
        assert cls in FAILURE_CLASSES, f"unknown failure class {cls}"
        super().__init__(f"{cls}: {msg}" if msg else cls)
        self.cls = cls
        self.msg = msg
        self.exit_code = exit_code


def decide(cls: str, preset_name: str, variant_level: int, same_variant_failures: int):
    """返回 (action, detail)。

    action: retry / degrade / attention / fail / cancel
    detail: retry 时是退避秒数；degrade 时是新的 variant 名。
    """
    preset = PRESETS.get(preset_name)
    if preset is None:
        raise KeyError(f"unknown retry preset {preset_name}")

    if cls == "cancel":
        return "cancel", None

    # OOM 走降级阶梯：它不是"再试一次"，是"换个跑法"，所以不吃 never 的限制。
    if cls == "oom":
        if variant_level < MAX_VARIANT_LEVEL:
            return "degrade", VARIANT_LADDER[variant_level + 1]
        return "attention", "oom_ladder_exhausted"

    if cls in ("human_reject", "bug"):
        return "attention", cls

    # 严格判定：白名单没写就是不重试（restyle 的 429 也不重试）
    if cls not in preset["allowed"]:
        return "fail", f"{cls}_not_retryable_for_{preset_name}"

    if same_variant_failures >= preset["max_same_variant"]:
        return "fail", "retry_budget_exhausted"

    delay = min(preset["cap_s"], preset["base_s"] * (2 ** max(0, same_variant_failures)))
    return "retry", delay
