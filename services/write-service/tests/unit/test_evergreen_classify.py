"""
常青分类测试
"""
import json
from unittest.mock import AsyncMock, MagicMock

import pytest
import sys
sys.path.insert(0, ".")

from app.api.update_image import _check_evergreen


class TestCheckEvergreen:
    @pytest.mark.asyncio
    async def test_evergreen_category_true(self, mock_deps):
        """常青类目 → True"""
        mock_deps["redis"].get = AsyncMock(return_value=json.dumps(["shoes", "bags"]))
        result = await _check_evergreen(mock_deps, "shoes")
        assert result is True

    @pytest.mark.asyncio
    async def test_non_evergreen_false(self, mock_deps):
        """非常青类目 → False"""
        mock_deps["redis"].get = AsyncMock(return_value=json.dumps(["shoes", "bags"]))
        result = await _check_evergreen(mock_deps, "electronics")
        assert result is False

    @pytest.mark.asyncio
    async def test_redis_cache_hit(self, mock_deps):
        """Redis 缓存命中, 不查 PG"""
        mock_deps["redis"].get = AsyncMock(return_value=json.dumps(["shoes"]))
        conn = AsyncMock()
        mock_deps["pg"].acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        await _check_evergreen(mock_deps, "shoes")
        conn.fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_pg_fallback(self, mock_deps):
        """Redis 未命中 → PG 查询 + 缓存回填"""
        mock_deps["redis"].get = AsyncMock(return_value=None)
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[
            {"category_l1_name": "shoes"},
            {"category_l1_name": "bags"},
        ])
        mock_deps["pg"].acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        result = await _check_evergreen(mock_deps, "shoes")
        assert result is True
        # 验证 Redis 回填
        mock_deps["redis"].set.assert_called_once()
        call_args = mock_deps["redis"].set.call_args
        assert call_args[1]["ex"] == 300
