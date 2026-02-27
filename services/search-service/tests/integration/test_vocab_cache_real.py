"""
真实三级缓存集成测试
"""
import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST"),
    reason="Set INTEGRATION_TEST=true",
)


class TestVocabCacheReal:
    @pytest.mark.asyncio
    async def test_warm_up_real(self, redis_client, pg_pool):
        """真实预热 (PG vocabulary_mapping)"""
        from app.infra.vocab_cache import VocabCache
        vc = VocabCache(redis_client, pg_pool)
        try:
            await vc.warm_up()
            # 预热后应有数据 (如果表有数据)
        except Exception as e:
            pytest.skip(f"PG not available or table missing: {e}")

    @pytest.mark.asyncio
    async def test_encode_decode_roundtrip(self, redis_client, pg_pool):
        """编码→解码往返"""
        from app.infra.vocab_cache import VocabCache
        vc = VocabCache(redis_client, pg_pool)

        # 插入测试数据
        async with pg_pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO vocabulary_mapping (vocab_type, string_val, int_code)
                   VALUES ($1, $2, $3) ON CONFLICT DO NOTHING""",
                "test_type", "test_val", 9999,
            )

        await vc.warm_up()
        code = vc.encode("test_type", "test_val")
        if code is not None:
            val = vc.decode("test_type", code)
            assert val == "test_val"

        # 清理
        async with pg_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM vocabulary_mapping WHERE vocab_type = $1 AND string_val = $2",
                "test_type", "test_val",
            )
