"""
三级词表缓存测试 (local→Redis→PG)
"""
from unittest.mock import AsyncMock, MagicMock

import pytest
import sys
sys.path.insert(0, ".")

from app.infra.vocab_cache import VocabCache


class TestVocabCache:
    @pytest.fixture
    def cache(self, mock_redis, mock_pg):
        vc = VocabCache(mock_redis, mock_pg)
        vc._code_to_str = {
            "category_l1": {42: "shoes", 99: "bags"},
            "tag": {10: "leather", 20: "red"},
        }
        vc._str_to_code = {
            "category_l1": {"shoes": 42, "bags": 99},
            "tag": {"leather": 10, "red": 20},
        }
        return vc

    def test_decode_local_hit(self, cache):
        """本地缓存命中"""
        assert cache.decode("category_l1", 42) == "shoes"
        assert cache.decode("tag", 10) == "leather"

    def test_decode_miss(self, cache):
        """本地缓存未命中 → None"""
        assert cache.decode("category_l1", 999) is None

    def test_decode_none_input(self, cache):
        """None 输入 → None"""
        assert cache.decode("category_l1", None) is None

    def test_encode_local_hit(self, cache):
        """编码本地命中"""
        assert cache.encode("category_l1", "shoes") == 42
        assert cache.encode("tag", "red") == 20

    def test_encode_miss(self, cache):
        """编码未命中 → None"""
        assert cache.encode("category_l1", "unknown") is None

    def test_encode_none_input(self, cache):
        """None 输入 → None"""
        assert cache.encode("category_l1", None) is None

    @pytest.mark.asyncio
    async def test_warm_up(self, mock_redis, mock_pg):
        """启动预热"""
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[
            {"vocab_type": "category_l1", "string_val": "shoes", "int_code": 42},
            {"vocab_type": "tag", "string_val": "leather", "int_code": 10},
        ])
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        vc = VocabCache(mock_redis, mock_pg)
        await vc.warm_up()
        assert vc.decode("category_l1", 42) == "shoes"
        assert vc.encode("tag", "leather") == 10

    @pytest.mark.asyncio
    async def test_refresh_reloads(self, mock_redis, mock_pg):
        """refresh 重新加载"""
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[
            {"vocab_type": "category_l1", "string_val": "hats", "int_code": 55},
        ])
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        vc = VocabCache(mock_redis, mock_pg)
        await vc.refresh()
        assert vc.decode("category_l1", 55) == "hats"
