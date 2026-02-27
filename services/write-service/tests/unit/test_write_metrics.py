"""
写入可见性指标测试
"""
import pytest
import sys
sys.path.insert(0, ".")

from app.core.metrics import write_visibility_latency, push_update_total, write_stage_latency


class TestWriteMetrics:
    def test_t0_to_t1_histogram(self):
        """T0→T1 histogram 可正常 observe"""
        write_visibility_latency.labels(level="t0_to_t1").observe(1.5)
        # Should not raise

    def test_t0_to_t2_histogram(self):
        """T0→T2 histogram 可正常 observe"""
        write_visibility_latency.labels(level="t0_to_t2").observe(3.0)

    def test_push_total_labels(self):
        """push_update_total 各 result 标签"""
        for result in ["ok", "retry_ok", "compensate", "skip"]:
            push_update_total.labels(result=result).inc()

    def test_stage_latency_labels(self):
        """write_stage_latency 各 stage 标签"""
        for stage in ["download", "feature", "milvus", "pg", "bitmap_push"]:
            write_stage_latency.labels(stage=stage).observe(0.1)
