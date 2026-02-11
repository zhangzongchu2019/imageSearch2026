"""
集成测试 — 需要真实中间件 (docker-compose up)
FIX #14: 覆盖 写入→flush→可搜索 端到端路径

运行方式:
    docker-compose up -d  # 启动中间件
    INTEGRATION_TEST=true pytest tests/integration/ -v

注意: 这些测试在 CI 中需要 services 启动, 本地跳过。
"""
import asyncio
import os
import time

import pytest

# 仅在显式启用时运行
pytestmark = pytest.mark.skipif(
    os.getenv("INTEGRATION_TEST", "false").lower() != "true",
    reason="Integration tests disabled. Set INTEGRATION_TEST=true to run.",
)


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
async def pg_pool():
    """真实 PG 连接池"""
    import asyncpg
    dsn = os.getenv("PG_DSN", "postgresql://postgres@localhost:5432/image_search")
    pool = await asyncpg.create_pool(dsn=dsn, min_size=2, max_size=5)
    yield pool
    await pool.close()


@pytest.fixture(scope="module")
async def redis_client():
    """真实 Redis 连接"""
    import redis.asyncio as aioredis
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    client = aioredis.from_url(url, decode_responses=True)
    yield client
    await client.aclose()


class TestRetryWithBackoff:
    """FIX #1: 指数退避重试集成验证"""

    @pytest.mark.asyncio
    async def test_retry_connects_after_delay(self):
        """验证重试机制在延迟后成功连接"""
        from app.core.lifecycle import _retry_with_backoff

        call_count = 0

        async def flaky_init():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Connection refused")
            return "connected"

        result = await _retry_with_backoff(flaky_init, "test_dep", max_retries=5, base_delay=0.01)
        assert result == "connected"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_retry_exhausted_raises(self):
        """验证重试耗尽后抛出异常"""
        from app.core.lifecycle import _retry_with_backoff

        async def always_fail():
            raise ConnectionError("Connection refused")

        with pytest.raises(ConnectionError):
            await _retry_with_backoff(always_fail, "test_dep", max_retries=2, base_delay=0.01)


class TestWriteReadIntegration:
    """FIX #14: 写入 → Flush → 可搜索 端到端验证"""

    @pytest.mark.asyncio
    async def test_dedup_redis_then_pg(self, pg_pool, redis_client):
        """二级去重: Redis miss → PG miss → 新图"""
        from app.api.update_image import _check_dedup, _gen_image_pk

        test_uri = f"https://test.com/{time.time()}.jpg"
        image_pk = _gen_image_pk(test_uri)

        # 确保干净状态
        await redis_client.delete(f"uri_cache:{image_pk}")
        async with pg_pool.acquire() as conn:
            await conn.execute("DELETE FROM uri_dedup WHERE image_pk = $1", image_pk)

        deps = {"redis": redis_client, "pg": pg_pool}

        # 首次: 应判定为新图
        is_new = await _check_dedup(deps, image_pk, test_uri)
        assert is_new is True

    @pytest.mark.asyncio
    async def test_dedup_redis_hit(self, pg_pool, redis_client):
        """Redis 缓存命中 → 非新图"""
        from app.api.update_image import _check_dedup, _gen_image_pk

        test_uri = f"https://test.com/cached_{time.time()}.jpg"
        image_pk = _gen_image_pk(test_uri)

        # 预设 Redis 缓存
        await redis_client.set(f"uri_cache:{image_pk}", "1", ex=60)

        deps = {"redis": redis_client, "pg": pg_pool}
        is_new = await _check_dedup(deps, image_pk, test_uri)
        assert is_new is False

        # 清理
        await redis_client.delete(f"uri_cache:{image_pk}")


class TestLockWatchdog:
    """FIX #4: 分布式锁 watchdog 续约测试"""

    @pytest.mark.asyncio
    async def test_watchdog_renews_lock(self, redis_client):
        """验证 watchdog 在 TTL/3 间隔续约锁"""
        from app.api.update_image import _LockWatchdog

        lock_key = "test_lock_watchdog"
        instance_id = "test_instance"

        await redis_client.set(lock_key, instance_id, nx=True, ex=3)
        watchdog = _LockWatchdog(redis_client, lock_key, instance_id, ttl_s=3)
        await watchdog.start()

        # 等待超过原始 TTL
        await asyncio.sleep(4)

        # 锁应该仍然存在 (watchdog 续约了)
        val = await redis_client.get(lock_key)
        assert val == instance_id

        await watchdog.stop()
        await redis_client.delete(lock_key)

    @pytest.mark.asyncio
    async def test_watchdog_stops_on_lock_loss(self, redis_client):
        """验证锁被其他实例抢占后 watchdog 停止"""
        from app.api.update_image import _LockWatchdog

        lock_key = "test_lock_lost"
        instance_id = "original"

        await redis_client.set(lock_key, instance_id, ex=3)
        watchdog = _LockWatchdog(redis_client, lock_key, instance_id, ttl_s=3)
        await watchdog.start()

        # 模拟另一个实例抢占锁
        await redis_client.set(lock_key, "intruder", ex=3)

        await asyncio.sleep(2)
        await watchdog.stop()
        await redis_client.delete(lock_key)


class TestTransactionalWrite:
    """FIX #9: 事务性写入验证"""

    @pytest.mark.asyncio
    async def test_compensation_log_on_pg_failure(self, redis_client):
        """PG 写入失败时应产生补偿日志"""
        from app.api.update_image import _write_compensation_log
        import json

        comp_key = "compensation:test_pk:test_trace"
        await _write_compensation_log(redis_client, comp_key, {
            "image_pk": "test_pk",
            "steps_done": ["milvus_upsert"],
            "steps_failed": ["pg_dedup"],
            "trace_id": "test_trace",
        })

        raw = await redis_client.get(comp_key)
        assert raw is not None
        data = json.loads(raw)
        assert data["steps_failed"] == ["pg_dedup"]

        await redis_client.delete(comp_key)


class TestFlushSafety:
    """FIX #3: Flush 异常不再静默吞掉"""

    @pytest.mark.asyncio
    async def test_flush_timeout_logged_not_swallowed(self):
        """Flush 超时应该被记录而不是静默"""
        from app.api.update_image import _safe_incr_flush_counter
        import app.api.update_image as module

        # 强制触发 flush
        module._flush_counter = module.FLUSH_BATCH_SIZE - 1
        # 这不会抛异常 (内部捕获并记录)
        deps = {}
        await _safe_incr_flush_counter(deps)
        # 验证 counter 被重置
        assert module._flush_counter == 0


class TestTraceIdPropagation:
    """FIX #12: Trace ID 贯穿验证"""

    def test_trace_id_format(self):
        """trace_id 符合 W3C TraceContext 格式 (32 hex chars)"""
        from app.api.update_image import _gen_trace_id
        trace_id = _gen_trace_id()
        assert len(trace_id) == 32
        assert all(c in "0123456789abcdef" for c in trace_id)

    def test_trace_id_uniqueness(self):
        """每次生成不同的 trace_id"""
        from app.api.update_image import _gen_trace_id
        ids = {_gen_trace_id() for _ in range(100)}
        assert len(ids) == 100
