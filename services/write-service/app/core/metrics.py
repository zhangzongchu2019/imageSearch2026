"""
write-service Prometheus 指标

FIX-B: 写入可见性端到端延迟指标
"""
from prometheus_client import Counter, Histogram

# 写入可见性: T0(受理) → T1(Milvus写入) → T2(bitmap push完成)
write_visibility_latency = Histogram(
    "image_write_visibility_seconds",
    "Write visibility end-to-end latency",
    ["level"],  # level: t0_to_t1 | t0_to_t2
    buckets=[0.5, 1, 2, 3, 5, 8, 10, 15, 20, 30],
)

# PushUpdate 结果
push_update_total = Counter(
    "image_write_push_update_total",
    "PushUpdate attempts and results",
    ["result"],  # result: ok | retry_ok | compensate | skip
)

# 写入链路阶段耗时
write_stage_latency = Histogram(
    "image_write_stage_seconds",
    "Write pipeline stage latency",
    ["stage"],  # stage: download | feature | milvus | pg | bitmap_push
    buckets=[0.01, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10],
)
