"""
Merchant Scope 解析 — scope_id → merchant_id 列表
Redis 缓存 1h + PG 持久
"""
from __future__ import annotations

import json
from typing import List

import structlog

logger = structlog.get_logger(__name__)


class ScopeResolver:
    def __init__(self, redis_client, pg_pool):
        self._redis = redis_client
        self._pg = pg_pool

    async def resolve(self, scope_id: str) -> List[str]:
        """merchant_scope_id → merchant_id 列表

        Level 1: Redis scope:{scope_id}
        Level 2: PG merchant_scope_registry
        """
        cache_key = f"scope:{scope_id}"

        # Redis
        cached = await self._redis.get(cache_key)
        if cached:
            return json.loads(cached)

        # PG
        async with self._pg.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT merchant_ids FROM merchant_scope_registry WHERE scope_id = $1",
                scope_id,
            )
        if not row:
            from fastapi import HTTPException
            raise HTTPException(
                status_code=400,
                detail={"error": {"code": "100_01_06",
                         "message": f"merchant_scope_id '{scope_id}' not found"}},
            )

        ids = json.loads(row["merchant_ids"])
        await self._redis.set(cache_key, row["merchant_ids"], ex=3600)
        return ids
