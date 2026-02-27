"""
URI 去重清理 + VACUUM 测试
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sys
sys.path.insert(0, ".")

from app.main import uri_dedup_cleanup, _yyyymm_subtract


class TestUriCleanup:
    @pytest.mark.asyncio
    async def test_delete_beyond_18m(self):
        """删除 >18 个月的记录"""
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="DELETE 5000")
        conn.fetchval = AsyncMock(return_value=1000)  # low dead tuples
        mock_pg = AsyncMock()
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        mock_pg.close = AsyncMock()

        with patch("app.main._get_pg", new_callable=AsyncMock, return_value=mock_pg):
            await uri_dedup_cleanup()

        conn.execute.assert_called()
        # 验证 SQL 使用了正确的 cutoff
        sql = conn.execute.call_args_list[0][0][0]
        assert "DELETE" in sql
        assert "ts_month" in sql

    @pytest.mark.asyncio
    async def test_evergreen_not_deleted(self):
        """常青记录 (ts_month=999999) 不删除"""
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="DELETE 0")
        conn.fetchval = AsyncMock(return_value=0)
        mock_pg = AsyncMock()
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        mock_pg.close = AsyncMock()

        with patch("app.main._get_pg", new_callable=AsyncMock, return_value=mock_pg):
            await uri_dedup_cleanup()

        sql = conn.execute.call_args_list[0][0][0]
        assert "999999" in sql or "!= 999999" in sql.replace(" ", "")

    @pytest.mark.asyncio
    async def test_vacuum_above_100m(self):
        """死元组 > 1亿 → 触发 VACUUM"""
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="DELETE 0")
        conn.fetchval = AsyncMock(return_value=150_000_000)
        mock_pg = AsyncMock()
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        mock_pg.close = AsyncMock()

        with patch("app.main._get_pg", new_callable=AsyncMock, return_value=mock_pg):
            await uri_dedup_cleanup()

        # VACUUM should be called
        calls = [str(c) for c in conn.execute.call_args_list]
        assert any("VACUUM" in c for c in calls)

    @pytest.mark.asyncio
    async def test_vacuum_not_below_threshold(self):
        """死元组 < 1亿 → 不触发 VACUUM"""
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="DELETE 0")
        conn.fetchval = AsyncMock(return_value=50_000_000)
        mock_pg = AsyncMock()
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        mock_pg.close = AsyncMock()

        with patch("app.main._get_pg", new_callable=AsyncMock, return_value=mock_pg):
            await uri_dedup_cleanup()

        calls = [str(c) for c in conn.execute.call_args_list]
        assert not any("VACUUM" in c for c in calls)
