"""
URI 去重清理集成测试 — 真实 PG
"""
import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST"),
    reason="Set INTEGRATION_TEST=true",
)


class TestUriCleanupReal:
    @pytest.mark.asyncio
    async def test_uri_dedup_table_exists(self, pg_pool):
        """uri_dedup 表存在"""
        async with pg_pool.acquire() as conn:
            row = await conn.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'uri_dedup')"
            )
        assert row is True

    @pytest.mark.asyncio
    async def test_dead_tuples_readable(self, pg_pool):
        """pg_stat_user_tables dead tuple 统计可读"""
        async with pg_pool.acquire() as conn:
            dead = await conn.fetchval(
                "SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname='uri_dedup'"
            )
        # dead 可以是 None (表不存在) 或 >= 0
        assert dead is None or dead >= 0
