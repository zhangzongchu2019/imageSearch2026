"""
熔断器生命周期测试 — CLOSED→OPEN→HALF_OPEN→CLOSED
"""
import time
from unittest.mock import patch

import pytest
import sys
sys.path.insert(0, ".")

from app.core.circuit_breaker import (
    BreakerConfig,
    BreakerState,
    CircuitBreaker,
    CircuitBreakerOpenError,
    get_breaker,
)


@pytest.fixture
def breaker():
    return CircuitBreaker(
        "test_breaker",
        BreakerConfig(fail_max=3, reset_timeout_s=0.5),
    )


class TestCircuitBreaker:
    def test_initial_closed(self, breaker):
        """初始状态为 CLOSED"""
        assert breaker.state == BreakerState.CLOSED
        assert breaker._fail_count == 0

    @pytest.mark.asyncio
    async def test_5_fails_opens(self):
        """连续 fail_max 次失败 → OPEN"""
        b = CircuitBreaker("test_open", BreakerConfig(fail_max=5, reset_timeout_s=10))
        for i in range(5):
            try:
                async def _fail():
                    raise RuntimeError("test")
                await b.call_async(_fail())
            except RuntimeError:
                pass
        assert b.state == BreakerState.OPEN

    @pytest.mark.asyncio
    async def test_open_rejects(self, breaker):
        """OPEN 状态拒绝调用"""
        # 触发 OPEN
        for _ in range(3):
            try:
                async def _fail():
                    raise RuntimeError("err")
                await breaker.call_async(_fail())
            except RuntimeError:
                pass
        assert breaker.state == BreakerState.OPEN
        with pytest.raises(CircuitBreakerOpenError):
            async def _call():
                return "ok"
            await breaker.call_async(_call())

    @pytest.mark.asyncio
    async def test_timeout_half_open(self):
        """OPEN → reset_timeout 后 → HALF_OPEN"""
        b = CircuitBreaker("test_half", BreakerConfig(fail_max=2, reset_timeout_s=0.1))
        for _ in range(2):
            try:
                async def _fail():
                    raise RuntimeError("err")
                await b.call_async(_fail())
            except RuntimeError:
                pass
        assert b._state == BreakerState.OPEN
        import asyncio
        await asyncio.sleep(0.15)
        assert b.state == BreakerState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_probe_success_closes(self):
        """HALF_OPEN 探测成功 → CLOSED"""
        b = CircuitBreaker("test_close", BreakerConfig(fail_max=2, reset_timeout_s=0.1))
        for _ in range(2):
            try:
                async def _fail():
                    raise RuntimeError("err")
                await b.call_async(_fail())
            except RuntimeError:
                pass
        import asyncio
        await asyncio.sleep(0.15)
        assert b.state == BreakerState.HALF_OPEN

        async def _ok():
            return "success"
        result = await b.call_async(_ok())
        assert result == "success"
        assert b.state == BreakerState.CLOSED

    @pytest.mark.asyncio
    async def test_probe_fail_reopens(self):
        """HALF_OPEN 探测失败 → 重新 OPEN"""
        b = CircuitBreaker("test_reopen", BreakerConfig(fail_max=2, reset_timeout_s=0.1))
        for _ in range(2):
            try:
                async def _fail():
                    raise RuntimeError("err")
                await b.call_async(_fail())
            except RuntimeError:
                pass
        import asyncio
        await asyncio.sleep(0.15)
        assert b.state == BreakerState.HALF_OPEN

        try:
            async def _fail2():
                raise RuntimeError("probe fail")
            await b.call_async(_fail2())
        except RuntimeError:
            pass
        assert b._state == BreakerState.OPEN

    def test_force_state(self, breaker):
        """手动覆盖状态"""
        breaker.force_state(BreakerState.OPEN, "test override")
        assert breaker.state == BreakerState.OPEN
        breaker.force_state(BreakerState.CLOSED, "restore")
        assert breaker.state == BreakerState.CLOSED

    @pytest.mark.asyncio
    async def test_exclude_exceptions(self):
        """排除的异常不计入失败"""
        b = CircuitBreaker(
            "test_exclude",
            BreakerConfig(fail_max=2, exclude_exceptions=(ValueError,)),
        )
        for _ in range(5):
            try:
                async def _val_err():
                    raise ValueError("excluded")
                await b.call_async(_val_err())
            except ValueError:
                pass
        # Should still be CLOSED (ValueError excluded)
        assert b.state == BreakerState.CLOSED
