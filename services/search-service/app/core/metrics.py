"""
Prometheus 指标定义 — 对齐系统设计 v1.2 附录A
"""
from prometheus_client import Counter, Gauge, Histogram


class _Metrics:
    """延迟初始化, 避免多进程冲突"""

    def __init__(self):
        self._initialized = False

    def _init(self):
        if self._initialized:
            return

        # 检索
        self.search_total = Counter(
            "image_search_requests_total",
            "Total search requests",
            ["strategy", "confidence"],
        )
        self.search_stage_latency = Histogram(
            "image_search_stage_latency_seconds",
            "Per-stage latency",
            ["stage"],
            buckets=[0.001, 0.005, 0.01, 0.03, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5],
        )
        self.search_errors_total = Counter(
            "image_search_errors_total", "Search errors", ["code"]
        )

        # 级联
        self.cascade_triggered_total = Counter(
            "image_search_cascade_triggered_total",
            "Cascade path triggers (Top1 < threshold)",
        )
        self.cascade_penetration_rate = Gauge(
            "image_search_cascade_penetration_rate",
            "Rolling cascade penetration rate",
        )

        # GPU
        self.gpu_fallback_total = Counter(
            "feature_gpu_fallback_total", "GPU→CPU fallback count"
        )

        # ANN
        self.ann_timeout_total = Counter(
            "image_search_ann_timeout_total", "ANN timeout count", ["zone"]
        )

        # Tag Recall
        self.tag_recall_timeout_total = Counter(
            "image_search_tag_recall_timeout_total", "Tag recall timeout count"
        )

        # 过滤
        self.bitmap_filter_fallback = Counter(
            "bitmap_filter_fallback_total", "Filter fallback", ["level", "reason"]
        )

        # 降级
        self.degrade_transitions = Counter(
            "image_search_degrade_transitions_total",
            "Degrade state transitions",
            ["from_state", "to_state"],
        )
        self.degrade_state = Gauge(
            "image_search_degrade_state",
            "Current degrade state (0=S0, 1=S1, 2=S2, 3=S3, 4=S4)",
        )

        # 限流
        self.rate_limited_total = Counter(
            "image_search_rate_limited_total", "Rate limited requests", ["type"]
        )

        # 行为上报
        self.behavior_report_errors_total = Counter(
            "behavior_report_errors_total", "Behavior report errors"
        )

        # 写入可见性
        self.write_visibility_latency = Histogram(
            "image_write_visibility_seconds",
            "Write visibility latency",
            ["level"],
            buckets=[1, 2, 3, 5, 8, 10, 15, 20, 30, 45],
        )

        # FIX #8: 补充缺失指标
        self.search_recall_at_k = Histogram(
            "image_search_recall_at_k",
            "Recall@K distribution (sampled)",
            ["k"],
            buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
        )
        self.tag_recall_unique_add = Counter(
            "image_search_tag_recall_unique_add_total",
            "Unique candidates added by tag recall",
        )
        self.tag_recall_idf_truncated = Counter(
            "image_search_tag_recall_idf_truncated_total",
            "IDF truncation events",
        )
        self.refine_rank_change = Histogram(
            "image_search_refine_rank_change",
            "Rank changes after refine",
            buckets=[0, 1, 5, 10, 50, 100, 500, 1000],
        )
        self.evergreen_pool_watermark = Gauge(
            "image_search_evergreen_pool_watermark",
            "Current evergreen pool count",
        )
        self.evergreen_soft_deleted_total = Counter(
            "image_search_evergreen_soft_deleted_total",
            "Evergreen images soft-deleted",
        )
        self.evergreen_physically_deleted_total = Counter(
            "image_search_evergreen_physically_deleted_total",
            "Evergreen images physically deleted (7d+ cold)",
        )
        self.bitmap_filter_pg_diff = Gauge(
            "bitmap_filter_pg_rocksdb_diff",
            "PG vs RocksDB row count diff",
        )
        self.bitmap_push_update_total = Counter(
            "bitmap_filter_push_update_total",
            "Push update calls",
            ["result"],
        )
        self.non_hot_penetration_ratio = Gauge(
            "image_search_non_hot_penetration_ratio",
            "Rolling cascade penetration rate",
        )
        self.trace_propagation_total = Counter(
            "image_search_trace_propagation_total",
            "Distributed tracing propagation events",
        )
        self.error_code_total = Counter(
            "image_search_error_code_total",
            "Error code distribution",
            ["code"],
        )

        self._initialized = True


METRICS = _Metrics()


def setup_metrics():
    METRICS._init()
