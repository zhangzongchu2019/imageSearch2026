"""
常青池治理集成测试 — 真实 Milvus + PG
"""
import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST"),
    reason="Set INTEGRATION_TEST=true",
)


class TestEvergreenGovernorReal:
    @pytest.mark.asyncio
    async def test_evergreen_count_query(self, milvus_collection):
        """Milvus 常青计数查询可执行"""
        result = milvus_collection.query(
            expr="is_evergreen == true",
            output_fields=["count(*)"],
        )
        count = result[0]["count(*)"] if result else 0
        assert count >= 0

    @pytest.mark.asyncio
    async def test_bitmap_table_exists(self, pg_pool):
        """image_merchant_bitmaps 表存在"""
        async with pg_pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'image_merchant_bitmaps')"
            )
        assert exists is True
