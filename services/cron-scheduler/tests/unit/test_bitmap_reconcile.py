"""
Bitmap 对账测试 (PG vs RocksDB)
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sys
sys.path.insert(0, ".")

from app.main import bitmap_reconciliation


class TestBitmapReconcile:
    @pytest.mark.asyncio
    async def test_pg_batch_scan(self):
        """PG 分批扫描"""
        conn = AsyncMock()
        conn.fetchval = AsyncMock(side_effect=[
            500,  # total count
            100,  # empty count
        ])
        mock_pg = AsyncMock()
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        mock_pg.close = AsyncMock()

        with patch("app.main._get_pg", new_callable=AsyncMock, return_value=mock_pg):
            await bitmap_reconciliation()

        conn.fetchval.assert_called()

    @pytest.mark.asyncio
    async def test_zero_bitmap_count(self):
        """统计零基数 bitmap 数量"""
        conn = AsyncMock()
        conn.fetchval = AsyncMock(side_effect=[1000, 50])
        mock_pg = AsyncMock()
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        mock_pg.close = AsyncMock()

        with patch("app.main._get_pg", new_callable=AsyncMock, return_value=mock_pg):
            await bitmap_reconciliation()

    @pytest.mark.asyncio
    async def test_alert_high_zero(self):
        """零 bitmap >1000 → 告警"""
        conn = AsyncMock()
        conn.fetchval = AsyncMock(side_effect=[10000, 2000])
        mock_pg = AsyncMock()
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        mock_pg.close = AsyncMock()

        with patch("app.main._get_pg", new_callable=AsyncMock, return_value=mock_pg):
            # Should log error (diff > 1000)
            await bitmap_reconciliation()

    @pytest.mark.asyncio
    async def test_ok_low_diff(self):
        """差异小 → 正常"""
        conn = AsyncMock()
        conn.fetchval = AsyncMock(side_effect=[10000, 5])
        mock_pg = AsyncMock()
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        mock_pg.close = AsyncMock()

        with patch("app.main._get_pg", new_callable=AsyncMock, return_value=mock_pg):
            await bitmap_reconciliation()

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """PG 连接失败 → 不崩溃"""
        with patch("app.main._get_pg", new_callable=AsyncMock, side_effect=Exception("pg down")):
            # Should not raise
            await bitmap_reconciliation()
