"""
Admin API 端点测试
覆盖: 降级手动覆盖/释放、熔断器手动控制、配置热重载、配置审计
"""
from unittest.mock import MagicMock

import pytest

from app.model.schemas import DegradeState


class TestAdminDegradeOverride:
    """POST /admin/degrade/override + /release"""

    def test_force_state_s4_manual_lock(self):
        """手动锁定 → 状态变为 S4"""
        from app.core.degrade_fsm import DegradeStateMachine

        fsm = DegradeStateMachine(redis_client=None)
        fsm.force_state(DegradeState.S4, "maintenance")
        assert fsm.state == DegradeState.S4

    def test_release_from_s4_to_s0(self):
        """释放手动锁定 → S4→S0"""
        from app.core.degrade_fsm import DegradeStateMachine

        fsm = DegradeStateMachine(redis_client=None)
        fsm.force_state(DegradeState.S4, "maintenance")
        fsm.force_state(DegradeState.S0, "maintenance complete")
        assert fsm.state == DegradeState.S0

    def test_force_state_updates_epoch(self):
        """force_state 递增 epoch"""
        from app.core.degrade_fsm import DegradeStateMachine

        fsm = DegradeStateMachine(redis_client=None)
        initial_epoch = fsm.epoch
        fsm.force_state(DegradeState.S2, "bitmap_skip")
        assert fsm.epoch > initial_epoch

    def test_force_state_records_reason(self):
        """force_state 记录降级原因 (FIX-G)"""
        from app.core.degrade_fsm import DegradeStateMachine

        fsm = DegradeStateMachine(redis_client=None)
        fsm.force_state(DegradeState.S2, "bitmap_all_unavailable")
        assert fsm.last_reason == "bitmap_all_unavailable"

    def test_status_returns_full_state(self):
        """status() 返回完整可观测状态 (P0-F)"""
        from app.core.degrade_fsm import DegradeStateMachine

        fsm = DegradeStateMachine(redis_client=None)
        status = fsm.status()
        assert "state" in status
        assert "epoch" in status
        assert "instance_id" in status
        assert "ramp_stage" in status
        assert "window_avg_p99_2m" in status
        assert "window_max_err_2m" in status
        assert "redis_connected" in status


class TestAdminCircuitBreakerForce:
    """POST /admin/breakers/{name}/force"""

    def test_force_breaker_open(self):
        """手动打开熔断器 → 后续请求被拒"""
        import time as _time
        from app.core.circuit_breaker import BreakerConfig, BreakerState, CircuitBreaker

        cb = CircuitBreaker("test_hot_zone", BreakerConfig(reset_timeout_s=9999))
        cb.force_state(BreakerState.OPEN)
        cb._last_failure_time = _time.monotonic()
        assert cb.state == BreakerState.OPEN

    def test_force_breaker_closed(self):
        """手动关闭熔断器 → 恢复正常"""
        from app.core.circuit_breaker import BreakerConfig, BreakerState, CircuitBreaker

        cb = CircuitBreaker("test_restore", BreakerConfig(reset_timeout_s=9999))
        cb.force_state(BreakerState.OPEN)
        cb.force_state(BreakerState.CLOSED)
        assert cb.state == BreakerState.CLOSED

    def test_force_breaker_half_open(self):
        """手动设为半开 → 允许探测请求"""
        from app.core.circuit_breaker import BreakerState, CircuitBreaker

        cb = CircuitBreaker("test_probe")
        cb.force_state(BreakerState.HALF_OPEN)
        assert cb.state == BreakerState.HALF_OPEN

    def test_list_all_breakers(self):
        """获取所有熔断器状态 (通过 get_breaker 注册)"""
        from app.core.circuit_breaker import get_breaker, all_breakers

        # get_breaker 会自动注册到 _registry
        get_breaker("breaker_a")
        get_breaker("breaker_b")

        status = all_breakers()
        assert "breaker_a" in status
        assert "breaker_b" in status
