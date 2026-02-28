"""
图片写入 API — 事务性写入 + 补偿日志 + 分布式锁
"""
from __future__ import annotations

import asyncio
import json
import os
import uuid
from typing import Any, Dict

import structlog

logger = structlog.get_logger(__name__)

# ── Flush 安全 (FIX #3) ──

FLUSH_BATCH_SIZE = int(os.getenv("IMGSRCH_FLUSH_BATCH_SIZE", "1000"))
_flush_counter = 0


async def _safe_incr_flush_counter(milvus_client: Any):
    """Flush 计数器 — 达到批量阈值后触发 flush, 异常不静默"""
    global _flush_counter
    _flush_counter += 1
    if _flush_counter >= FLUSH_BATCH_SIZE:
        _flush_counter = 0
        try:
            if milvus_client and hasattr(milvus_client, "flush"):
                await asyncio.get_event_loop().run_in_executor(
                    None, milvus_client.flush, []
                )
        except Exception as e:
            logger.error("milvus_flush_error", error=str(e))


# ── 分布式锁 Watchdog (FIX #4) ──

class _LockWatchdog:
    """Redis 分布式锁续约 watchdog — 每 ttl/3 续约一次"""

    LUA_RENEW = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('pexpire', KEYS[1], ARGV[2])
    end
    return 0
    """

    def __init__(self, redis_client, lock_key: str, instance_id: str, ttl_s: int = 30):
        self._redis = redis_client
        self._lock_key = lock_key
        self._instance_id = instance_id
        self._ttl_s = ttl_s
        self._task = None
        self._running = False

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._renew_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _renew_loop(self):
        interval = self._ttl_s / 3
        while self._running:
            try:
                await asyncio.sleep(interval)
                if not self._running:
                    break
                await self._redis.eval(
                    self.LUA_RENEW,
                    1,
                    self._lock_key,
                    self._instance_id,
                    str(self._ttl_s * 1000),
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("lock_watchdog_renew_error", error=str(e))


# ── 事务性写入补偿日志 (FIX #9) ──

async def _write_compensation_log(
    redis_client, comp_key: str, data: Dict[str, Any]
):
    """写入补偿日志到 Redis — 用于事务失败后的补偿恢复"""
    payload = json.dumps(data)
    await redis_client.set(comp_key, payload, ex=86400)


# ── Trace ID 生成 (FIX #12) ──

def _gen_trace_id() -> str:
    """生成 W3C Trace Context 兼容的 trace_id (32 hex chars)"""
    return uuid.uuid4().hex
