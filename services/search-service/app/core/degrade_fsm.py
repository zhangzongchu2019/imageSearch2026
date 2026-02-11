"""
降级状态机 — S0→S4 五态 + 自动恢复 + 灰度放量
v1.3 加固:
  - FIX #13: 阈值从配置服务动态读取, 支持不重启调整
  - v4.0 默认阈值: S0→S1 P99>450ms/2min, S0→S2 P99>800ms/30s
"""
from __future__ import annotations

import enum
import json
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

import mmh3
import structlog

from app.core.config import get_settings
from app.model.schemas import (
    DataScope,
    DegradeState,
    EffectiveParams,
    SearchRequest,
    TimeRange,
)

logger = structlog.get_logger(__name__)
settings = get_settings()


@dataclass
class MetricSample:
    timestamp: float
    p99_ms: float
    error_rate: float


class RollingWindow:
    """滑动窗口指标聚合"""

    def __init__(self, window_s: int = 120):
        self._window_s = window_s
        self._samples: deque[MetricSample] = deque(maxlen=600)
        self._lock = threading.Lock()

    def push(self, p99_ms: float, error_rate: float):
        with self._lock:
            self._samples.append(
                MetricSample(time.monotonic(), p99_ms, error_rate)
            )

    def avg_p99(self, duration_s: Optional[int] = None) -> float:
        cutoff = time.monotonic() - (duration_s or self._window_s)
        with self._lock:
            samples = [s for s in self._samples if s.timestamp >= cutoff]
        if not samples:
            return 0.0
        return sum(s.p99_ms for s in samples) / len(samples)

    def max_error_rate(self, duration_s: Optional[int] = None) -> float:
        cutoff = time.monotonic() - (duration_s or self._window_s)
        with self._lock:
            samples = [s for s in self._samples if s.timestamp >= cutoff]
        if not samples:
            return 0.0
        return max(s.error_rate for s in samples)


class DegradeStateMachine:
    """
    五态降级:
      S0 正常 → S1 预降级 → S2 强降级
      S2 → S3 恢复中 (灰度) → S0 正常
      S4 人工锁定

    v4.0 变更:
      - S0→S1 阈值: P99 > 450ms 持续 2min (原350ms)
      - S0→S2 阈值: P99 > 800ms 持续 30s
    """

    def __init__(self, redis_client=None):
        self._state = DegradeState.S0
        self._entered_at = time.monotonic()
        self._window = RollingWindow(window_s=120)
        self._ramp_stage = 0
        self._lock = threading.Lock()
        self._redis = redis_client

    @property
    def state(self) -> DegradeState:
        return self._state

    def apply(self, req: SearchRequest) -> EffectiveParams:
        """将降级状态应用到请求参数, 返回有效参数"""
        params = EffectiveParams.from_request(req)

        if self._state == DegradeState.S0:
            return params

        # S1/S2: 范围收缩到 热区 + 常青, 禁用非热区级联
        if self._state in (DegradeState.S1, DegradeState.S2):
            params.time_range = TimeRange.HOT_PLUS_EVERGREEN
            params.enable_fallback = False
            params.enable_cascade = False  # 禁用非热区 DiskANN

        if self._state == DegradeState.S1:
            params.ef_search = settings.hot_zone.ef_search_s1  # 128
            params.refine_top_k = settings.search.refine.top_k_s1  # 1000

        elif self._state == DegradeState.S2:
            params.ef_search = settings.hot_zone.ef_search_s2  # 64
            params.refine_top_k = settings.search.refine.top_k_s2  # 500

        elif self._state == DegradeState.S3:
            # 灰度放量
            ramp_stages = settings.degrade.recovery.s3_ramp_stages
            ratio = ramp_stages[min(self._ramp_stage, len(ramp_stages) - 1)]
            # murmurhash3 保证跨语言一致性
            if (mmh3.hash(params.merchant_scope[0] if params.merchant_scope else "default") % 100) < ratio * 100:
                return params  # 完整参数
            else:
                params.time_range = TimeRange.HOT_PLUS_EVERGREEN
                params.enable_fallback = False
                params.ef_search = settings.hot_zone.ef_search_s1

        elif self._state == DegradeState.S4:
            # 人工锁定: 参数由外部指定
            pass

        return params

    def tick(self, current_p99_ms: float, current_error_rate: float):
        """每秒调用, 检查状态转移"""
        self._window.push(current_p99_ms, current_error_rate)

        with self._lock:
            now = time.monotonic()
            dwell_s = now - self._entered_at

            if self._state == DegradeState.S0:
                if self._should_enter_s2():
                    self._transition(DegradeState.S2)
                elif self._should_enter_s1():
                    self._transition(DegradeState.S1)

            elif self._state == DegradeState.S1:
                if self._should_enter_s2():
                    self._transition(DegradeState.S2)
                elif (
                    dwell_s >= settings.degrade.recovery.s1_dwell_s
                    and self._recovery_ok()
                ):
                    self._transition(DegradeState.S3)

            elif self._state == DegradeState.S2:
                if (
                    dwell_s >= settings.degrade.recovery.s2_dwell_s
                    and self._recovery_ok()
                ):
                    self._transition(DegradeState.S3)

            elif self._state == DegradeState.S3:
                if self._should_enter_s2():
                    self._transition(DegradeState.S2)  # 回退
                elif dwell_s >= settings.degrade.recovery.s3_observe_s:
                    self._advance_ramp()

    def force_state(self, state: DegradeState, reason: str = ""):
        """人工覆盖 (管理接口)"""
        with self._lock:
            old = self._state
            self._state = state
            self._entered_at = time.monotonic()
            self._ramp_stage = 0
            logger.warning(
                "degrade_manual_override",
                old=old.value,
                new=state.value,
                reason=reason,
            )

    def _should_enter_s1(self) -> bool:
        """FIX #13: 阈值优先从配置服务读取, 支持运行时动态调整"""
        p99_th, dur, err_th = self._get_degrade_thresholds("s0_s1")
        return (
            self._window.avg_p99(dur) > p99_th
            or self._window.max_error_rate(dur) > err_th
        )

    def _should_enter_s2(self) -> bool:
        p99_th, dur, err_th = self._get_degrade_thresholds("s0_s2")
        return (
            self._window.avg_p99(dur) > p99_th
            or self._window.max_error_rate(dur) > err_th
        )

    def _recovery_ok(self) -> bool:
        p99 = self._window.avg_p99(60)
        err = self._window.max_error_rate(60)
        p99_th, _, _ = self._get_degrade_thresholds("s0_s1")
        return p99 < p99_th * 0.5 and err < 0.0001

    def _get_degrade_thresholds(self, level: str) -> tuple:
        """从配置服务或 settings 读取降级阈值

        配置服务 key 格式:
          degrade.s0_s1.p99_threshold_ms = 450
          degrade.s0_s1.duration_s = 120
          degrade.s0_s1.error_rate_threshold = 0.0005
        """
        try:
            from app.core.config_service import get_config_service
            cs = get_config_service()
            if cs._initialized:
                cfg_static = getattr(settings.degrade, level)
                p99_th = cs.get_int(
                    f"degrade.{level}.p99_threshold_ms",
                    cfg_static.p99_threshold_ms,
                )
                duration = cs.get_int(
                    f"degrade.{level}.duration_s",
                    cfg_static.duration_s,
                )
                err_th_raw = cs.get(
                    f"degrade.{level}.error_rate_threshold",
                    cfg_static.error_rate_threshold,
                )
                err_th = float(err_th_raw) if err_th_raw is not None else cfg_static.error_rate_threshold
                return p99_th, duration, err_th
        except Exception:
            pass
        # Fallback: 静态配置
        cfg = getattr(settings.degrade, level)
        return cfg.p99_threshold_ms, cfg.duration_s, cfg.error_rate_threshold

    def _transition(self, new_state: DegradeState):
        old = self._state
        self._state = new_state
        self._entered_at = time.monotonic()
        self._ramp_stage = 0

        logger.warning("degrade_transition", old=old.value, new=new_state.value)

        # 广播到 Redis (跨实例同步)
        if self._redis:
            try:
                self._redis.set(
                    "degrade:state",
                    json.dumps({"state": new_state.value, "since": time.time()}),
                )
                self._redis.publish("degrade_channel", new_state.value)
            except Exception:
                pass  # Redis 失败不阻塞状态机

    def _advance_ramp(self):
        stages = settings.degrade.recovery.s3_ramp_stages
        if self._ramp_stage < len(stages) - 1:
            self._ramp_stage += 1
            logger.info("degrade_ramp_advance", stage=self._ramp_stage)
        else:
            self._transition(DegradeState.S0)
            logger.info("degrade_fully_recovered")
