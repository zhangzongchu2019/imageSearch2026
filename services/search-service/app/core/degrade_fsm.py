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
    五态降级 — v1.4 分布式协调版:
      S0 正常 → S1 预降级 → S2 强降级
      S2 → S3 恢复中 (灰度) → S0 正常
      S4 人工锁定

    P0-B: 多实例一致性保障
      - Redis 存储权威状态 + epoch (单调递增版本号)
      - 状态转移必须 CAS: 只有 epoch 匹配时才允许写入
      - 每次 tick 先 sync_from_redis 获取最新状态
      - 无 Redis 时降级为本地模式 (保持可用)

    Redis Key: imgsrch:degrade:state
    Redis Value: JSON {"state": "S0", "epoch": 42, "updated_by": "pod-xxx", "updated_at": "..."}

    v4.0 变更:
      - S0→S1 阈值: P99 > 450ms 持续 2min (原350ms)
      - S0→S2 阈值: P99 > 800ms 持续 30s
    """

    REDIS_KEY = "imgsrch:degrade:state"

    def __init__(self, redis_client=None, instance_id: str = ""):
        self._state = DegradeState.S0
        self._entered_at = time.monotonic()
        self._window = RollingWindow(window_s=120)
        self._ramp_stage = 0
        self._lock = threading.Lock()
        self._redis = redis_client
        self._epoch = 0
        self._instance_id = instance_id or f"pod-{id(self):x}"

        # P0-B: 启动时从 Redis 同步状态
        self._sync_from_redis()

    @property
    def state(self) -> DegradeState:
        return self._state

    @property
    def epoch(self) -> int:
        return self._epoch

    def status(self) -> dict:
        """返回完整状态 (用于 /admin/degrade/status API — P0-F 测试可观测)"""
        return {
            "state": self._state.value,
            "epoch": self._epoch,
            "instance_id": self._instance_id,
            "entered_at_monotonic": self._entered_at,
            "ramp_stage": self._ramp_stage,
            "window_avg_p99_2m": round(self._window.avg_p99(120), 2),
            "window_max_err_2m": round(self._window.max_error_rate(120), 6),
            "redis_connected": self._redis is not None,
        }

    def _sync_from_redis(self):
        """从 Redis 读取权威状态, 本地同步"""
        if not self._redis:
            return
        try:
            raw = self._redis.get(self.REDIS_KEY)
            if raw:
                data = json.loads(raw)
                remote_state = DegradeState(data["state"])
                remote_epoch = data["epoch"]
                if remote_epoch > self._epoch:
                    with self._lock:
                        old = self._state
                        self._state = remote_state
                        self._epoch = remote_epoch
                        self._entered_at = time.monotonic()
                    if old != remote_state:
                        logger.info(
                            "degrade_synced_from_redis",
                            old=old.value,
                            new=remote_state.value,
                            epoch=remote_epoch,
                            source=data.get("updated_by", "unknown"),
                        )
        except Exception as e:
            logger.warning("degrade_redis_sync_failed", error=str(e))

    def _cas_transition(self, new_state: DegradeState, reason: str = "") -> bool:
        """CAS 写入 Redis — 只有 epoch 匹配时才成功

        Returns: True if transition accepted, False if rejected (stale epoch)
        """
        if not self._redis:
            # 无 Redis: 本地模式, 直接转移
            self._do_local_transition(new_state, reason)
            return True

        new_epoch = self._epoch + 1
        new_data = json.dumps({
            "state": new_state.value,
            "epoch": new_epoch,
            "updated_by": self._instance_id,
            "updated_at": time.time(),
            "reason": reason,
        })

        try:
            # Lua CAS: 原子比较 epoch 并写入
            lua_cas = """
            local current = redis.call('GET', KEYS[1])
            if current == false then
                redis.call('SET', KEYS[1], ARGV[1])
                return 1
            end
            local data = cjson.decode(current)
            if data.epoch == tonumber(ARGV[2]) then
                redis.call('SET', KEYS[1], ARGV[1])
                return 1
            end
            return 0
            """
            result = self._redis.eval(lua_cas, 1, self.REDIS_KEY, new_data, str(self._epoch))
            if result == 1:
                self._do_local_transition(new_state, reason, new_epoch)
                return True
            else:
                # CAS 失败: 其他实例已更新, 重新同步
                logger.info("degrade_cas_rejected", attempted=new_state.value, local_epoch=self._epoch)
                self._sync_from_redis()
                return False
        except Exception as e:
            logger.warning("degrade_cas_redis_error", error=str(e))
            # Redis 故障: 降级为本地模式
            self._do_local_transition(new_state, reason)
            return True

    def _do_local_transition(self, new_state: DegradeState, reason: str = "", new_epoch: int = 0):
        """执行本地状态转移"""
        old = self._state
        self._state = new_state
        self._entered_at = time.monotonic()
        self._epoch = new_epoch or (self._epoch + 1)
        self._last_reason = reason  # FIX-G: 保存降级原因

        if new_state in (DegradeState.S0, DegradeState.S3):
            self._ramp_stage = 0

        logger.warning(
            "degrade_state_transition",
            old=old.value,
            new=new_state.value,
            epoch=self._epoch,
            instance=self._instance_id,
            reason=reason,
        )

    @property
    def state(self) -> DegradeState:
        return self._state

    @property
    def last_reason(self) -> str:
        """FIX-G: 最近一次状态转移的原因"""
        return getattr(self, "_last_reason", "")

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
        """每秒调用, 检查状态转移 — P0-B: 先同步 Redis, 再 CAS 转移"""
        self._window.push(current_p99_ms, current_error_rate)

        # P0-B: 每次 tick 先从 Redis 同步最新状态
        self._sync_from_redis()

        with self._lock:
            now = time.monotonic()
            dwell_s = now - self._entered_at

            if self._state == DegradeState.S0:
                if self._should_enter_s2():
                    self._cas_transition(DegradeState.S2, "p99_or_error_s2_threshold")
                elif self._should_enter_s1():
                    self._cas_transition(DegradeState.S1, "p99_or_error_s1_threshold")

            elif self._state == DegradeState.S1:
                if self._should_enter_s2():
                    self._cas_transition(DegradeState.S2, "s1_escalate_to_s2")
                elif (
                    dwell_s >= settings.degrade.recovery.s1_dwell_s
                    and self._recovery_ok()
                ):
                    self._cas_transition(DegradeState.S3, "s1_recovery_start")

            elif self._state == DegradeState.S2:
                if (
                    dwell_s >= settings.degrade.recovery.s2_dwell_s
                    and self._recovery_ok()
                ):
                    self._cas_transition(DegradeState.S3, "s2_recovery_start")

            elif self._state == DegradeState.S3:
                if self._should_enter_s2():
                    self._cas_transition(DegradeState.S2, "s3_rollback_to_s2")
                elif dwell_s >= settings.degrade.recovery.s3_observe_s:
                    self._advance_ramp()

    def force_state(self, state: DegradeState, reason: str = ""):
        """人工覆盖 / 自动触发 (管理接口 / P0-C bitmap 联动)
        P0-B: 通过 CAS 写入 Redis, 所有实例同步
        """
        with self._lock:
            self._cas_transition(state, reason=reason or "force_override")

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

    def _advance_ramp(self):
        stages = settings.degrade.recovery.s3_ramp_stages
        if self._ramp_stage < len(stages) - 1:
            self._ramp_stage += 1
            logger.info("degrade_ramp_advance", stage=self._ramp_stage)
        else:
            self._cas_transition(DegradeState.S0, "s3_fully_recovered")
            logger.info("degrade_fully_recovered")
