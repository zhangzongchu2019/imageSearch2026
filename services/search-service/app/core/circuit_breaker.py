"""
熔断器模块 — 基于 pybreaker, 封装统一接口 + Prometheus 可观测
v1.4 P0-A: 生产加固

使用方式:
    breaker = get_breaker("milvus_hot")
    result = await breaker.call_async(milvus_search_coro())

熔断策略:
    - fail_max 次连续失败 → OPEN (拒绝请求)
    - reset_timeout 后 → HALF_OPEN (放行 1 次探测)
    - 探测成功 → CLOSED; 探测失败 → 回到 OPEN

可观测:
    - Prometheus: circuit_breaker_state{name}
    - Prometheus: circuit_breaker_failures_total{name}
    - Prometheus: circuit_breaker_calls_total{name, result}
    - API: /admin/breakers → 所有 breaker 状态
"""
from __future__ import annotations

import asyncio
import time
import threading
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional

import structlog
from prometheus_client import Counter, Gauge

logger = structlog.get_logger(__name__)


# ── Prometheus 指标 ──

BREAKER_STATE = Gauge(
    "circuit_breaker_state",
    "Circuit breaker state: 0=closed, 1=half_open, 2=open",
    ["name"],
)
BREAKER_FAILURES = Counter(
    "circuit_breaker_failures_total",
    "Total circuit breaker recorded failures",
    ["name"],
)
BREAKER_CALLS = Counter(
    "circuit_breaker_calls_total",
    "Total circuit breaker calls",
    ["name", "result"],  # result: success | failure | rejected
)


class BreakerState(Enum):
    CLOSED = 0
    HALF_OPEN = 1
    OPEN = 2


@dataclass
class BreakerConfig:
    """熔断器配置"""
    fail_max: int = 5           # 连续失败次数阈值
    reset_timeout_s: float = 30.0  # OPEN → HALF_OPEN 的等待时间
    half_open_max: int = 1      # HALF_OPEN 允许通过的探测数
    exclude_exceptions: tuple = ()  # 不计入失败的异常类型


class CircuitBreaker:
    """轻量级熔断器 — 线程安全, 支持 async

    比 pybreaker 更贴合我们场景:
    - 直接暴露 Prometheus
    - 支持 async call
    - 异常类型可过滤
    """

    def __init__(self, name: str, config: Optional[BreakerConfig] = None):
        self.name = name
        self._cfg = config or BreakerConfig()
        self._state = BreakerState.CLOSED
        self._fail_count = 0
        self._last_failure_time = 0.0
        self._half_open_count = 0
        self._lock = threading.Lock()

        # 初始化 Prometheus
        BREAKER_STATE.labels(name=name).set(0)

    @property
    def state(self) -> BreakerState:
        with self._lock:
            if self._state == BreakerState.OPEN:
                # 检查是否该进入 HALF_OPEN
                if time.monotonic() - self._last_failure_time >= self._cfg.reset_timeout_s:
                    self._transition(BreakerState.HALF_OPEN)
            return self._state

    @property
    def is_open(self) -> bool:
        return self.state == BreakerState.OPEN

    async def call_async(self, coro):
        """执行异步调用, 受熔断器保护

        Raises:
            CircuitBreakerOpenError: 熔断器打开时拒绝调用
        """
        current = self.state

        if current == BreakerState.OPEN:
            BREAKER_CALLS.labels(name=self.name, result="rejected").inc()
            raise CircuitBreakerOpenError(
                f"Circuit breaker '{self.name}' is OPEN "
                f"(failures={self._fail_count}, "
                f"reset_in={self._cfg.reset_timeout_s - (time.monotonic() - self._last_failure_time):.1f}s)"
            )

        if current == BreakerState.HALF_OPEN:
            with self._lock:
                if self._half_open_count >= self._cfg.half_open_max:
                    BREAKER_CALLS.labels(name=self.name, result="rejected").inc()
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker '{self.name}' HALF_OPEN probe limit reached"
                    )
                self._half_open_count += 1

        try:
            result = await coro
            self._on_success()
            return result
        except Exception as e:
            if isinstance(e, self._cfg.exclude_exceptions):
                self._on_success()  # 不计入失败
                raise
            self._on_failure()
            raise

    def call_sync(self, func: Callable, *args, **kwargs):
        """执行同步调用, 受熔断器保护"""
        current = self.state

        if current == BreakerState.OPEN:
            BREAKER_CALLS.labels(name=self.name, result="rejected").inc()
            raise CircuitBreakerOpenError(f"Circuit breaker '{self.name}' is OPEN")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            if isinstance(e, self._cfg.exclude_exceptions):
                self._on_success()
                raise
            self._on_failure()
            raise

    def _on_success(self):
        with self._lock:
            BREAKER_CALLS.labels(name=self.name, result="success").inc()
            if self._state == BreakerState.HALF_OPEN:
                # 探测成功 → 关闭
                self._transition(BreakerState.CLOSED)
            self._fail_count = 0

    def _on_failure(self):
        with self._lock:
            self._fail_count += 1
            self._last_failure_time = time.monotonic()
            BREAKER_FAILURES.labels(name=self.name).inc()
            BREAKER_CALLS.labels(name=self.name, result="failure").inc()

            if self._state == BreakerState.HALF_OPEN:
                # 探测失败 → 重新打开
                self._transition(BreakerState.OPEN)
            elif self._fail_count >= self._cfg.fail_max:
                self._transition(BreakerState.OPEN)

    def _transition(self, new_state: BreakerState):
        """状态转移 (调用方须持有 _lock)"""
        old = self._state
        self._state = new_state
        BREAKER_STATE.labels(name=self.name).set(new_state.value)

        if new_state == BreakerState.HALF_OPEN:
            self._half_open_count = 0

        if new_state == BreakerState.CLOSED:
            self._fail_count = 0

        logger.warning(
            "circuit_breaker_transition",
            breaker=self.name,
            old=old.name,
            new=new_state.name,
            fail_count=self._fail_count,
        )

    def force_state(self, state: BreakerState, reason: str = ""):
        """手动覆盖状态 (管理接口 / 故障注入)"""
        with self._lock:
            self._transition(state)
            if reason:
                logger.warning("circuit_breaker_forced", breaker=self.name, state=state.name, reason=reason)

    def status(self) -> dict:
        """返回当前状态 (用于 /admin/breakers)"""
        return {
            "name": self.name,
            "state": self.state.name,
            "fail_count": self._fail_count,
            "config": {
                "fail_max": self._cfg.fail_max,
                "reset_timeout_s": self._cfg.reset_timeout_s,
            },
        }


class CircuitBreakerOpenError(Exception):
    """熔断器打开时抛出"""
    pass


# ── 全局注册表 ──

_registry: Dict[str, CircuitBreaker] = {}
_registry_lock = threading.Lock()


def get_breaker(name: str, config: Optional[BreakerConfig] = None) -> CircuitBreaker:
    """获取或创建命名熔断器 (全局单例)"""
    with _registry_lock:
        if name not in _registry:
            _registry[name] = CircuitBreaker(name, config)
        return _registry[name]


def all_breakers() -> Dict[str, dict]:
    """返回所有熔断器状态 (用于管理 API)"""
    with _registry_lock:
        return {name: b.status() for name, b in _registry.items()}
