"""
真实 Redis+PG 回源集成测试
"""
import json
import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST"),
    reason="Set INTEGRATION_TEST=true",
)


class TestScopeResolverReal:
    @pytest.mark.asyncio
    async def test_redis_pg_roundtrip(self, redis_client, pg_pool):
        """Redis miss → PG → Redis 回填 → Redis hit"""
        from app.infra.scope_resolver import ScopeResolver
        resolver = ScopeResolver(redis_client, pg_pool)

        scope_id = "test_scope_integ"
        cache_key = f"scope:{scope_id}"

        # 清理
        await redis_client.delete(cache_key)

        # 在 PG 中插入测试数据
        async with pg_pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO merchant_scope_registry (scope_id, merchant_ids)
                   VALUES ($1, $2) ON CONFLICT (scope_id) DO UPDATE SET merchant_ids = $2""",
                scope_id, json.dumps(["m_test_1", "m_test_2"]),
            )

        # 首次解析 → PG
        result = await resolver.resolve(scope_id)
        assert result == ["m_test_1", "m_test_2"]

        # 验证 Redis 已缓存
        cached = await redis_client.get(cache_key)
        assert cached is not None
        assert json.loads(cached) == ["m_test_1", "m_test_2"]

        # 再次解析 → Redis
        result2 = await resolver.resolve(scope_id)
        assert result2 == ["m_test_1", "m_test_2"]

        # 清理
        await redis_client.delete(cache_key)
        async with pg_pool.acquire() as conn:
            await conn.execute("DELETE FROM merchant_scope_registry WHERE scope_id = $1", scope_id)

    @pytest.mark.asyncio
    async def test_not_found(self, redis_client, pg_pool):
        """scope_id 不存在 → 400"""
        from app.infra.scope_resolver import ScopeResolver
        from fastapi import HTTPException
        resolver = ScopeResolver(redis_client, pg_pool)
        with pytest.raises(HTTPException):
            await resolver.resolve("nonexistent_scope_xxx")
