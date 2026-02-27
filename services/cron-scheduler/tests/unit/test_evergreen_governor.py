"""
常青池治理测试 (GREEN/YELLOW/RED)
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sys
sys.path.insert(0, ".")

from app.main import (
    evergreen_pool_check,
    _evergreen_cleanup,
    EVERGREEN_GREEN,
    EVERGREEN_YELLOW,
    EVERGREEN_RED,
)


class TestEvergreenGovernor:
    def test_thresholds(self):
        """阈值常量"""
        assert EVERGREEN_GREEN == 250_000_000
        assert EVERGREEN_YELLOW == 280_000_000
        assert EVERGREEN_RED == 300_000_000

    @pytest.mark.asyncio
    async def test_green_no_action(self):
        """绿区 (<2.5亿) → 无操作"""
        mock_coll = MagicMock()
        mock_coll.query = MagicMock(return_value=[{"count(*)": 100_000_000}])

        with patch("app.main._get_milvus", return_value=mock_coll):
            await evergreen_pool_check()
        # No cleanup should be triggered

    @pytest.mark.asyncio
    async def test_yellow_5year_cleanup(self):
        """黄区 (2.5-2.8亿) → 清理 >5年"""
        mock_coll = MagicMock()
        mock_coll.query = MagicMock(side_effect=[
            [{"count(*)": 260_000_000}],  # initial check
            [],  # cleanup query returns empty
        ])

        mock_pg = AsyncMock()
        mock_pg.close = AsyncMock()

        with patch("app.main._get_milvus", return_value=mock_coll), \
             patch("app.main._get_pg", new_callable=AsyncMock, return_value=mock_pg):
            await evergreen_pool_check()

    @pytest.mark.asyncio
    async def test_red_2year_cleanup(self):
        """红区 (>3亿) → 紧急清理 >2年"""
        mock_coll = MagicMock()
        mock_coll.query = MagicMock(side_effect=[
            [{"count(*)": 310_000_000}],  # initial check
            [],  # cleanup query returns empty
        ])

        mock_pg = AsyncMock()
        mock_pg.close = AsyncMock()

        with patch("app.main._get_milvus", return_value=mock_coll), \
             patch("app.main._get_pg", new_callable=AsyncMock, return_value=mock_pg):
            await evergreen_pool_check()

    @pytest.mark.asyncio
    async def test_batch_1000(self):
        """每批清理 1000 条"""
        mock_coll = MagicMock()
        # First batch returns 1000, second returns 0
        pks_batch = [{"image_pk": f"pk{i:028d}"} for i in range(1000)]
        mock_coll.query = MagicMock(side_effect=[pks_batch, []])
        mock_coll.delete = MagicMock()

        conn = AsyncMock()
        conn.execute = AsyncMock()
        mock_pg = AsyncMock()
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))

        await _evergreen_cleanup(mock_coll, mock_pg, EVERGREEN_GREEN, min_age_years=5)

        mock_coll.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_milvus_and_pg_both(self):
        """Milvus + PG 双端清理"""
        mock_coll = MagicMock()
        pks = [{"image_pk": f"pk{i:028d}"} for i in range(3)]
        mock_coll.query = MagicMock(side_effect=[pks, []])
        mock_coll.delete = MagicMock()

        conn = AsyncMock()
        conn.execute = AsyncMock()
        mock_pg = AsyncMock()
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))

        await _evergreen_cleanup(mock_coll, mock_pg, EVERGREEN_GREEN, min_age_years=2)

        mock_coll.delete.assert_called_once()
        conn.execute.assert_called_once()
        # PG DELETE contains image_pk
        sql = conn.execute.call_args[0][0]
        assert "DELETE" in sql
