"""
Scope 解析测试 — Redis 1h 缓存 → PG 回源
"""
import json
from unittest.mock import AsyncMock, MagicMock

import pytest
import sys
sys.path.insert(0, ".")

from app.infra.scope_resolver import ScopeResolver


class TestScopeResolver:
    @pytest.fixture
    def resolver(self, mock_redis, mock_pg):
        return ScopeResolver(mock_redis, mock_pg)

    @pytest.mark.asyncio
    async def test_redis_cache_hit(self, resolver, mock_redis):
        """Redis 命中 → 直接返回"""
        mock_redis.get = AsyncMock(return_value=json.dumps(["m1", "m2"]))
        result = await resolver.resolve("scope_001")
        assert result == ["m1", "m2"]

    @pytest.mark.asyncio
    async def test_pg_fallback(self, resolver, mock_redis, mock_pg):
        """Redis miss → PG 查询"""
        mock_redis.get = AsyncMock(return_value=None)
        conn = AsyncMock()
        conn.fetchrow = AsyncMock(return_value={
            "merchant_ids": json.dumps(["m3", "m4"]),
        })
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        result = await resolver.resolve("scope_002")
        assert result == ["m3", "m4"]

    @pytest.mark.asyncio
    async def test_cache_1h_ttl(self, resolver, mock_redis, mock_pg):
        """PG 回源后缓存 TTL = 3600s (1h)"""
        mock_redis.get = AsyncMock(return_value=None)
        conn = AsyncMock()
        conn.fetchrow = AsyncMock(return_value={
            "merchant_ids": json.dumps(["m5"]),
        })
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        await resolver.resolve("scope_003")
        call_args = mock_redis.set.call_args
        assert call_args[1]["ex"] == 3600

    @pytest.mark.asyncio
    async def test_not_found_400(self, resolver, mock_redis, mock_pg):
        """scope_id 不存在 → HTTP 400"""
        mock_redis.get = AsyncMock(return_value=None)
        conn = AsyncMock()
        conn.fetchrow = AsyncMock(return_value=None)
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await resolver.resolve("nonexistent")
        assert exc_info.value.status_code == 400
