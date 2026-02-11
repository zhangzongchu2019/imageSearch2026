"""
降级状态机测试 — 覆盖系统设计 v1.2 §11.2 八种状态转移
"""
import time
from unittest.mock import MagicMock

import pytest

# 确保模块可导入 (测试环境无实际依赖)
import sys
sys.path.insert(0, ".")

from app.core.degrade_fsm import DegradeStateMachine, RollingWindow
from app.model.schemas import DegradeState, SearchRequest, DataScope, TimeRange


@pytest.fixture
def fsm():
    return DegradeStateMachine(redis_client=None)


class TestDegradeTransitions:
    """八种状态转移测试矩阵 (对齐系统设计 v1.2 §11.2)"""

    def test_s0_normal_no_transition(self, fsm):
        """S0 正常: P99 < 阈值, 无转移"""
        for _ in range(300):
            fsm.tick(100.0, 0.0)
        assert fsm.state == DegradeState.S0

    def test_s0_to_s1_high_latency(self, fsm):
        """S0→S1: P99 > 450ms 持续 2min"""
        for _ in range(130):
            fsm.tick(500.0, 0.0)
        assert fsm.state == DegradeState.S1

    def test_s0_to_s2_extreme_latency(self, fsm):
        """S0→S2 直通: P99 > 800ms 持续 30s"""
        for _ in range(35):
            fsm.tick(900.0, 0.02)
        assert fsm.state == DegradeState.S2

    def test_s1_to_s2_worsening(self, fsm):
        """S1→S2: 预降级后继续恶化"""
        # 先进入 S1
        for _ in range(130):
            fsm.tick(500.0, 0.0)
        assert fsm.state == DegradeState.S1
        # 恶化到 S2
        for _ in range(35):
            fsm.tick(900.0, 0.02)
        assert fsm.state == DegradeState.S2

    def test_s1_recovery_to_s3(self, fsm):
        """S1→S3: 驻留5min + 指标恢复"""
        # 进入 S1
        for _ in range(130):
            fsm.tick(500.0, 0.0)
        assert fsm.state == DegradeState.S1
        # 模拟恢复 + 等待驻留
        fsm._entered_at = time.monotonic() - 350  # 超过 300s 驻留
        for _ in range(120):
            fsm.tick(100.0, 0.0)
        assert fsm.state == DegradeState.S3

    def test_s3_back_to_s2_on_worsening(self, fsm):
        """S3→S2: 恢复中再次恶化"""
        fsm._state = DegradeState.S3
        fsm._entered_at = time.monotonic()
        for _ in range(35):
            fsm.tick(900.0, 0.02)
        assert fsm.state == DegradeState.S2

    def test_s4_manual_override(self, fsm):
        """S0→S4: 人工覆盖"""
        fsm.force_state(DegradeState.S4, "maintenance")
        assert fsm.state == DegradeState.S4

    def test_s4_to_s0_release(self, fsm):
        """S4→S0: 解除人工覆盖"""
        fsm.force_state(DegradeState.S4, "maintenance")
        fsm.force_state(DegradeState.S0, "maintenance complete")
        assert fsm.state == DegradeState.S0


class TestDegradeParamOverride:
    """降级参数覆盖测试"""

    def test_s0_no_override(self, fsm):
        req = SearchRequest(query_image="dGVzdA==", top_k=100)
        params = fsm.apply(req)
        assert params.ef_search == 192
        assert params.refine_top_k == 2000
        assert params.enable_fallback is True

    def test_s1_params(self, fsm):
        fsm._state = DegradeState.S1
        req = SearchRequest(query_image="dGVzdA==")
        params = fsm.apply(req)
        assert params.ef_search == 128
        assert params.refine_top_k == 1000
        assert params.enable_fallback is False
        assert params.time_range == TimeRange.HOT_PLUS_EVERGREEN

    def test_s2_params(self, fsm):
        fsm._state = DegradeState.S2
        req = SearchRequest(query_image="dGVzdA==")
        params = fsm.apply(req)
        assert params.ef_search == 64
        assert params.refine_top_k == 500
        assert params.enable_cascade is False


class TestRollingWindow:
    def test_avg_p99(self):
        w = RollingWindow(window_s=10)
        for _ in range(10):
            w.push(100.0, 0.0)
        assert w.avg_p99() == 100.0

    def test_max_error_rate(self):
        w = RollingWindow(window_s=10)
        w.push(100.0, 0.001)
        w.push(100.0, 0.005)
        w.push(100.0, 0.002)
        assert w.max_error_rate() == 0.005
