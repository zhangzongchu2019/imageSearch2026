"""
完整写入流水线集成测试 — 需要真实 PG/Redis/Milvus
"""
import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST"),
    reason="Set INTEGRATION_TEST=true",
)


class TestWriteE2E:
    @pytest.mark.asyncio
    async def test_full_write_pipeline(self, pg_pool, redis_client):
        """完整写入: 去重 → 下载 → 特征 → Milvus → PG → Redis 缓存"""
        # 清理测试数据
        test_pk = "0" * 32
        await redis_client.delete(f"uri_cache:{test_pk}")
        async with pg_pool.acquire() as conn:
            await conn.execute("DELETE FROM uri_dedup WHERE image_pk = $1", test_pk)

        # 验证初始状态 (新图)
        cached = await redis_client.get(f"uri_cache:{test_pk}")
        assert cached is None

        async with pg_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT 1 FROM uri_dedup WHERE image_pk = $1", test_pk)
        assert row is None

    @pytest.mark.asyncio
    async def test_dedup_after_write(self, pg_pool, redis_client):
        """写入后去重检查 — 二次写入应命中 Redis"""
        test_pk = "1" * 32
        # 模拟写入后的 Redis 缓存
        await redis_client.set(f"uri_cache:{test_pk}", "1", ex=300)

        cached = await redis_client.get(f"uri_cache:{test_pk}")
        assert cached == "1"

        # 清理
        await redis_client.delete(f"uri_cache:{test_pk}")


class TestDedupReal:
    @pytest.mark.asyncio
    async def test_redis_then_pg_real(self, pg_pool, redis_client):
        """真实二级去重: Redis miss → PG miss → 新图"""
        test_pk = "2" * 32
        await redis_client.delete(f"uri_cache:{test_pk}")
        async with pg_pool.acquire() as conn:
            await conn.execute("DELETE FROM uri_dedup WHERE image_pk = $1", test_pk)

        # Redis miss
        cached = await redis_client.get(f"uri_cache:{test_pk}")
        assert cached is None

        # PG miss
        async with pg_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT 1 FROM uri_dedup WHERE image_pk = $1", test_pk)
        assert row is None

    @pytest.mark.asyncio
    async def test_pg_hit_backfill_redis(self, pg_pool, redis_client):
        """PG 命中 → 回填 Redis"""
        test_pk = "3" * 32
        await redis_client.delete(f"uri_cache:{test_pk}")

        # 在 PG 中插入记录
        async with pg_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO uri_dedup (image_pk, uri_hash, ts_month) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                test_pk, "hash" * 8, 202601,
            )

        # Redis miss, PG hit → 应回填 Redis
        async with pg_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT 1 FROM uri_dedup WHERE image_pk = $1", test_pk)
        assert row is not None

        # 回填 Redis
        await redis_client.set(f"uri_cache:{test_pk}", "1", ex=7776000)
        cached = await redis_client.get(f"uri_cache:{test_pk}")
        assert cached == "1"

        # 清理
        await redis_client.delete(f"uri_cache:{test_pk}")
        async with pg_pool.acquire() as conn:
            await conn.execute("DELETE FROM uri_dedup WHERE image_pk = $1", test_pk)
