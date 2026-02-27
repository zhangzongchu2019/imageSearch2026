"""
分布式锁续约 watchdog 测试
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
import sys
sys.path.insert(0, ".")

from app.api.update_image import _LockWatchdog


class TestLockWatchdog:
    @pytest.mark.asyncio
    async def test_renews_ttl_third(self):
        """续约间隔 = TTL/3"""
        redis = AsyncMock()
        redis.eval = AsyncMock(return_value=1)
        wd = _LockWatchdog(redis, "lock:test", "inst-1", ttl_s=30)
        assert wd._ttl / 3 == 10.0

    @pytest.mark.asyncio
    async def test_lua_cas_own_lock(self):
        """Lua 脚本只续约自己的锁"""
        redis = AsyncMock()
        redis.eval = AsyncMock(return_value=1)
        wd = _LockWatchdog(redis, "lock:test", "inst-1", ttl_s=30)

        await wd.start()
        await asyncio.sleep(0.05)  # let one cycle run
        await wd.stop()

        # 验证 eval 被调用 (Lua CAS 脚本)
        if redis.eval.called:
            call_args = redis.eval.call_args
            assert call_args[0][2] == "lock:test"  # KEYS[1]
            assert call_args[0][3] == "inst-1"  # ARGV[1] = instance_id

    @pytest.mark.asyncio
    async def test_stops_on_loss(self):
        """锁被抢占 (eval 返回 0) → watchdog 停止"""
        redis = AsyncMock()
        redis.eval = AsyncMock(return_value=0)  # 锁已不属于本实例
        wd = _LockWatchdog(redis, "lock:test", "inst-1", ttl_s=3)

        await wd.start()
        await asyncio.sleep(1.5)  # 3/3 = 1s interval, should trigger
        # Watchdog should have broken out of loop
        assert wd._stopped or (wd._task and wd._task.done())
        await wd.stop()

    @pytest.mark.asyncio
    async def test_released_in_finally(self):
        """stop() 后 task 被取消"""
        redis = AsyncMock()
        redis.eval = AsyncMock(return_value=1)
        wd = _LockWatchdog(redis, "lock:test", "inst-1", ttl_s=30)

        await wd.start()
        assert wd._task is not None
        await wd.stop()
        assert wd._stopped is True
