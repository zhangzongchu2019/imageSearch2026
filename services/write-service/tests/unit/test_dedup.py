"""
两级去重测试 — Redis → PG
"""
import hashlib
from unittest.mock import AsyncMock, MagicMock

import pytest
import sys
sys.path.insert(0, ".")

from app.api.update_image import _check_dedup, _gen_image_pk


class TestGenImagePk:
    def test_pk_deterministic(self):
        """相同 URI 生成相同 PK"""
        uri = "https://cdn.example.com/shoes/001.jpg"
        pk1 = _gen_image_pk(uri)
        pk2 = _gen_image_pk(uri)
        assert pk1 == pk2
        assert len(pk1) == 32

    def test_pk_hex_format(self):
        """PK 是 32 字符 hex 串"""
        pk = _gen_image_pk("https://example.com/test.jpg")
        assert len(pk) == 32
        int(pk, 16)  # should not raise

    def test_pk_differs_for_different_uris(self):
        """不同 URI 生成不同 PK"""
        pk1 = _gen_image_pk("https://example.com/a.jpg")
        pk2 = _gen_image_pk("https://example.com/b.jpg")
        assert pk1 != pk2


class TestCheckDedup:
    @pytest.mark.asyncio
    async def test_redis_hit_returns_false(self, mock_deps):
        """Redis 命中 → 非新图 (False)"""
        mock_deps["redis"].get = AsyncMock(return_value="1")
        result = await _check_dedup(mock_deps, "a" * 32, "https://example.com/img.jpg")
        assert result is False

    @pytest.mark.asyncio
    async def test_redis_miss_pg_hit_returns_false(self, mock_deps):
        """Redis 未命中, PG 命中 → 非新图 + 回填 Redis"""
        mock_deps["redis"].get = AsyncMock(return_value=None)
        conn = AsyncMock()
        conn.fetchrow = AsyncMock(return_value={"1": 1})
        mock_deps["pg"].acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        result = await _check_dedup(mock_deps, "a" * 32, "https://example.com/img.jpg")
        assert result is False
        # 验证回填 Redis
        mock_deps["redis"].set.assert_called_once()
        call_args = mock_deps["redis"].set.call_args
        assert call_args[0][0].startswith("uri_cache:")
        assert call_args[1]["ex"] == 7776000

    @pytest.mark.asyncio
    async def test_redis_miss_pg_miss_returns_true(self, mock_deps):
        """Redis + PG 均未命中 → 新图 (True)"""
        mock_deps["redis"].get = AsyncMock(return_value=None)
        conn = AsyncMock()
        conn.fetchrow = AsyncMock(return_value=None)
        mock_deps["pg"].acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        result = await _check_dedup(mock_deps, "a" * 32, "https://example.com/img.jpg")
        assert result is True
