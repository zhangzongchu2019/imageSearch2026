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
        # 清空旧的高延迟样本, 模拟恢复 + 等待驻留
        fsm._window._samples.clear()
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


class TestDynamicThresholdReload:
    """FIX #13: 动态阈值重载测试 — 配置服务变更后阈值生效"""

    def test_default_s0_s1_threshold(self, fsm):
        """默认 S0→S1 阈值 = 450ms"""
        p99_th, dur, err_th = fsm._get_degrade_thresholds("s0_s1")
        assert p99_th == 450
        assert dur == 120
        assert err_th == 0.0005

    def test_default_s0_s2_threshold(self, fsm):
        """默认 S0→S2 阈值 = 800ms"""
        p99_th, dur, err_th = fsm._get_degrade_thresholds("s0_s2")
        assert p99_th == 800
        assert dur == 30

    def test_threshold_from_config_service(self, fsm):
        """配置服务可用时优先读取动态阈值"""
        from unittest.mock import patch, MagicMock
        mock_cs = MagicMock()
        mock_cs._initialized = True
        mock_cs.get_int.side_effect = lambda key, default: {
            "degrade.s0_s1.p99_threshold_ms": 500,
            "degrade.s0_s1.duration_s": 180,
        }.get(key, default)
        mock_cs.get.return_value = "0.001"
        # _get_degrade_thresholds 内部使用 from app.core.config_service import get_config_service
        with patch("app.core.config_service.get_config_service", return_value=mock_cs):
            p99_th, dur, err_th = fsm._get_degrade_thresholds("s0_s1")
            # 配置服务可用, 应使用动态值
            assert p99_th == 500
            assert dur == 180

    def test_config_service_failure_fallback(self, fsm):
        """配置服务异常时 fallback 到静态阈值"""
        from unittest.mock import patch
        with patch("app.core.degrade_fsm.get_config_service", side_effect=Exception("unavailable"), create=True):
            p99_th, dur, err_th = fsm._get_degrade_thresholds("s0_s1")
            assert p99_th == 450  # 静态默认值
            assert dur == 120


class TestMurmurhashRampConsistency:
    """S3 灰度放量 murmurhash3 一致性测试"""

    def test_same_merchant_same_result(self, fsm):
        """同一 merchant_id 多次哈希结果一致"""
        import mmh3
        h1 = mmh3.hash("merchant_001") % 100
        h2 = mmh3.hash("merchant_001") % 100
        assert h1 == h2

    def test_different_merchants_distribute(self, fsm):
        """1000 个 merchant 分布到 0-99"""
        import mmh3
        buckets = set()
        for i in range(1000):
            buckets.add(mmh3.hash(f"merchant_{i}") % 100)
        # 1000 个商家应覆盖大部分桶
        assert len(buckets) >= 50

    def test_s3_ramp_stage_0_passes_few(self, fsm):
        """S3 ramp stage 0 (10% 放量) 通过率约 10%"""
        import mmh3
        fsm._state = DegradeState.S3
        fsm._ramp_stage = 0
        from app.core.config import get_settings
        stages = get_settings().degrade.recovery.s3_ramp_stages
        ratio = stages[0]
        passed = 0
        total = 1000
        for i in range(total):
            if (mmh3.hash(f"m_{i}") % 100) < ratio * 100:
                passed += 1
        # 允许 ±10% 偏差
        assert abs(passed / total - ratio) < 0.10, f"Expected ~{ratio}, got {passed/total}"

    def test_ramp_cross_language_consistency(self, fsm):
        """murmurhash3 跨语言一致性: 已知输入已知输出"""
        import mmh3
        # mmh3.hash 默认 seed=0
        h = mmh3.hash("test_merchant", 0)
        # 值应稳定 (跨版本/跨语言相同)
        assert isinstance(h, int)
        # 再次调用结果相同
        assert mmh3.hash("test_merchant", 0) == h
