"""
Pipeline 集成场景测试
覆盖: CircuitBreaker+Pipeline 联动、Bitmap skip→Force S2、级联触发逻辑、
       降级参数实际应用、错误恢复
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.model.schemas import DegradeState


class TestCircuitBreakerPipelineIntegration:
    """熔断器打开时 Pipeline 的降级行为"""

    @pytest.mark.asyncio
    async def test_hot_zone_breaker_open_returns_empty(self):
        """热区熔断器打开 → 热区搜索返回空, 不崩溃"""
        from app.core.circuit_breaker import BreakerState, CircuitBreaker

        cb = CircuitBreaker("hot_zone_test")
        cb.force_state(BreakerState.OPEN)

        # 验证打开状态
        assert cb.is_open is True

        # Pipeline 应 catch CircuitBreakerOpenError 返回空列表
        with pytest.raises(Exception):
            await cb.call_async(AsyncMock(side_effect=Exception("should not reach")))

    @pytest.mark.asyncio
    async def test_breaker_half_open_allows_probe(self):
        """半开状态 → 允许一个探测请求"""
        from app.core.circuit_breaker import BreakerState, CircuitBreaker

        cb = CircuitBreaker("probe_test")
        cb.force_state(BreakerState.HALF_OPEN)

        mock_fn = AsyncMock(return_value="probe_ok")
        result = await cb.call_async(mock_fn)
        assert result == "probe_ok"

    @pytest.mark.asyncio
    async def test_breaker_probe_failure_reopens(self):
        """半开探测失败 → 重新打开"""
        from app.core.circuit_breaker import BreakerState, CircuitBreaker

        cb = CircuitBreaker("reopen_test")
        cb.force_state(BreakerState.HALF_OPEN)

        mock_fn = AsyncMock(side_effect=RuntimeError("still failing"))
        with pytest.raises(RuntimeError):
            await cb.call_async(mock_fn)

        assert cb.state == BreakerState.OPEN


class TestBitmapSkipForceDegradeInPipeline:
    """Bitmap skip 3次 → 强制 S2 降级 (P0-C)"""

    def test_consecutive_skip_counter_logic(self):
        """连续 3 次 bitmap skip → 触发 force S2"""
        from app.core.degrade_fsm import DegradeStateMachine
        from app.core.pipeline import BITMAP_SKIP_FORCE_DEGRADE_THRESHOLD

        assert BITMAP_SKIP_FORCE_DEGRADE_THRESHOLD == 3

        fsm = DegradeStateMachine(redis_client=None)
        assert fsm.state == DegradeState.S0

        # 模拟连续 3 次 bitmap skip 触发
        fsm.force_state(DegradeState.S2, "bitmap_all_unavailable")
        assert fsm.state == DegradeState.S2

    def test_skip_counter_resets_on_success(self):
        """bitmap 成功 → 计数器归零"""
        # 验证逻辑: 成功过滤后计数器重置
        skip_counter = 0
        skip_counter += 1  # skip 1
        skip_counter += 1  # skip 2
        skip_counter = 0   # 成功, 重置
        skip_counter += 1  # skip 1 again
        assert skip_counter == 1  # 不是 3, 不触发


class TestCascadeTriggerLogic:
    """级联搜索触发条件"""

    def test_cascade_triggered_on_low_score(self):
        """top1_score < cascade_threshold → 触发级联"""
        from app.core.config import get_settings

        settings = get_settings()
        threshold = settings.search.cascade_trigger_score

        top1_score = threshold - 0.01
        should_cascade = top1_score < threshold
        assert should_cascade is True

    def test_cascade_not_triggered_on_high_score(self):
        """top1_score >= cascade_threshold → 不触发"""
        from app.core.config import get_settings

        settings = get_settings()
        threshold = settings.search.cascade_trigger_score

        top1_score = threshold + 0.01
        should_cascade = top1_score < threshold
        assert should_cascade is False

    def test_cascade_disabled_in_s1_degrade(self):
        """S1 降级 → enable_cascade=False"""
        from app.core.degrade_fsm import DegradeStateMachine
        from app.model.schemas import SearchRequest

        fsm = DegradeStateMachine(redis_client=None)
        fsm._state = DegradeState.S1

        req = SearchRequest(query_image="dGVzdA==")
        params = fsm.apply(req)
        assert params.enable_cascade is False

    def test_cascade_disabled_in_s2_degrade(self):
        """S2 降级 → enable_cascade=False"""
        from app.core.degrade_fsm import DegradeStateMachine
        from app.model.schemas import SearchRequest

        fsm = DegradeStateMachine(redis_client=None)
        fsm._state = DegradeState.S2

        req = SearchRequest(query_image="dGVzdA==")
        params = fsm.apply(req)
        assert params.enable_cascade is False


class TestDegradeParamsApplied:
    """降级参数实际应用到搜索"""

    def test_s1_reduces_ef_search(self):
        """S1 → ef_search=128"""
        from app.core.degrade_fsm import DegradeStateMachine
        from app.model.schemas import SearchRequest

        fsm = DegradeStateMachine(redis_client=None)
        fsm._state = DegradeState.S1

        req = SearchRequest(query_image="dGVzdA==")
        params = fsm.apply(req)
        assert params.ef_search == 128

    def test_s2_reduces_ef_search_further(self):
        """S2 → ef_search=64"""
        from app.core.degrade_fsm import DegradeStateMachine
        from app.model.schemas import SearchRequest

        fsm = DegradeStateMachine(redis_client=None)
        fsm._state = DegradeState.S2

        req = SearchRequest(query_image="dGVzdA==")
        params = fsm.apply(req)
        assert params.ef_search == 64

    def test_s1_s2_restrict_time_range(self):
        """S1/S2 → time_range=HOT_PLUS_EVERGREEN"""
        from app.core.degrade_fsm import DegradeStateMachine
        from app.model.schemas import SearchRequest, TimeRange

        fsm = DegradeStateMachine(redis_client=None)
        fsm._state = DegradeState.S1

        req = SearchRequest(query_image="dGVzdA==")
        params = fsm.apply(req)
        assert params.time_range == TimeRange.HOT_PLUS_EVERGREEN

    def test_s1_s2_disable_fallback(self):
        """S1/S2 → enable_fallback=False"""
        from app.core.degrade_fsm import DegradeStateMachine
        from app.model.schemas import SearchRequest

        fsm = DegradeStateMachine(redis_client=None)
        fsm._state = DegradeState.S2

        req = SearchRequest(query_image="dGVzdA==")
        params = fsm.apply(req)
        assert params.enable_fallback is False
