"""管线 DSL 与 8 个步骤的声明。

「代码即配置」：步骤声明写在 Python 里，启动时物化成 step_spec 行。
没有 YAML、没有解析器、没有可视化编排——那是另一个系统（见 09.1 明确不做）。

step_key 是内容寻址：把「语义输入」的规范化 JSON 做 sha256。
上游产物摘要作为输入的一部分，所以下游的 key 在上游跑完之前算不出来，
这就是「惰性展开」不是优化而是必然结果的原因。
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from .db import dumps

# 这些 key 不参与缓存键：它们描述「怎么跑」，不描述「跑什么」。
# 漏掉任何一个影响输出的字段都会导致缓存投毒（9.3 第 5 条），所以这里是白名单式排除。
NON_SEMANTIC_KEYS = frozenset({"faults", "nondet_seed", "priority", "note", "fail_step"})


@dataclass(frozen=True)
class Step:
    name: str
    deps: tuple[str, ...]
    cost: int                 # 进度权重
    gpu_mb: int = 0
    gpu_mode: str = "shared"  # exclusive | shared
    retry: str = "fast"       # fast | oom_aware | slow | never
    idem: str = "det"         # det | nondet | human
    checkpoint: str | None = None
    timeout_s: int | None = None
    mock_seconds: float = 1.0  # 演示用：这个步骤大概跑多久
    shards: int = 0            # checkpoint 分片数（0 = 不分片）


# 动画素材处理管线。8 步，其中 restyle 是唯一非确定性步骤，占 60% 成本。
PIPELINE_NAME = "anim_restyle"
PIPELINE_VERSION = 3

STEPS: tuple[Step, ...] = (
    Step("probe", (), cost=2, mock_seconds=0.6,
         retry="fast", idem="det", timeout_s=60),
    Step("shot_split", ("probe",), cost=50, mock_seconds=1.2,
         retry="fast", idem="det", timeout_s=300),
    Step("frames", ("probe",), cost=90, gpu_mb=2048, mock_seconds=2.0,
         retry="oom_aware", idem="det", checkpoint="per_shot",
         timeout_s=1800, shards=8),
    Step("matting", ("frames",), cost=600, gpu_mb=12288, mock_seconds=4.0,
         retry="oom_aware", idem="det", checkpoint="per_shot",
         timeout_s=3600, shards=8),
    Step("restyle", ("matting",), cost=1800, gpu_mb=20480, gpu_mode="exclusive",
         mock_seconds=9.0,
         # 非确定性 + 绝不重跑：重跑会得到另一批画面，且与已完成分片不同源。
         retry="never", idem="nondet", checkpoint="per_shot",
         timeout_s=14400, shards=12),
    Step("temporal", ("restyle",), cost=420, gpu_mb=8192, mock_seconds=3.0,
         # 跨镜头一致性校正：checkpoint 粒度不能小于它的依赖窗口，否则接缝闪烁。
         retry="fast", idem="det", checkpoint="per_shot",
         timeout_s=3600, shards=4),
    Step("encode", ("temporal", "shot_split"), cost=60, gpu_mb=1024,
         mock_seconds=1.6, retry="fast", idem="det", timeout_s=1800),
    Step("qc_review", ("encode",), cost=0, mock_seconds=0.0,
         retry="never", idem="human", timeout_s=None),
)

STEP_BY_NAME: dict[str, Step] = {s.name: s for s in STEPS}
TOTAL_COST = sum(s.cost for s in STEPS)          # 3022
LONGEST_COST = max(s.cost for s in STEPS)        # restyle 1800 → 59.6%

DEFAULT_PARAMS = {
    "source": "demo_shot.mp4",
    "resolution": "1080p",
    "style": "ghibli_v2",
    "target_fps": 24,
}


def canonical_params(task_params: dict) -> dict:
    """只保留语义输入。排序由 dumps(sort_keys=True) 保证。"""
    return {k: v for k, v in task_params.items() if k not in NON_SEMANTIC_KEYS}


def compute_step_key(step: Step, task_params: dict, dep_digests: dict[str, str],
                     seed: int | None) -> str:
    """下游的 key 依赖上游产物的 digest —— 所以必须等上游跑完才算得出来。"""
    payload = {
        "pipeline": PIPELINE_NAME,
        "version": PIPELINE_VERSION,
        "step": step.name,
        "params": canonical_params(task_params),
        "deps": {k: dep_digests[k] for k in sorted(dep_digests)},
    }
    if seed is not None:
        payload["seed"] = seed          # 只有 nondet 步骤才带 seed
    return hashlib.sha256(dumps(payload).encode("utf-8")).hexdigest()


def ensure_specs(store) -> None:
    """把 DSL 物化成 step_spec 行（幂等）。"""
    with store.tx() as c:
        for s in STEPS:
            c.execute(
                "INSERT OR REPLACE INTO step_spec"
                "(pipeline, version, step_name, deps_json, cost, gpu_mb, gpu_mode,"
                " retry_preset, idem, checkpoint, timeout_s)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (PIPELINE_NAME, PIPELINE_VERSION, s.name, dumps(list(s.deps)), s.cost,
                 s.gpu_mb, s.gpu_mode, s.retry, s.idem, s.checkpoint, s.timeout_s),
            )


def topo_order() -> tuple[str, ...]:
    return tuple(s.name for s in STEPS)   # 声明顺序即拓扑序，测试会校验这一点


def check_dag() -> None:
    seen: set[str] = set()
    for s in STEPS:
        missing = [d for d in s.deps if d not in seen]
        if missing:
            raise ValueError(f"step {s.name} 依赖了未声明或后置的步骤: {missing}")
        seen.add(s.name)


check_dag()
