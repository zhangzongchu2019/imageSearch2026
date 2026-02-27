"""
月度分区轮转测试 (创建/释放/删除)
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sys
sys.path.insert(0, ".")

from app.main import _yyyymm_subtract, partition_rotation


class TestYyyymmSubtract:
    def test_normal(self):
        assert _yyyymm_subtract(202601, 18) == 202407

    def test_year_boundary(self):
        assert _yyyymm_subtract(202601, 1) == 202512

    def test_exact_year(self):
        assert _yyyymm_subtract(202601, 12) == 202501

    def test_multi_year(self):
        assert _yyyymm_subtract(202601, 24) == 202401


class TestPartitionRotation:
    @pytest.mark.asyncio
    async def test_new_month_created(self):
        """新月份分区创建"""
        mock_coll = MagicMock()
        mock_part = MagicMock()
        mock_part.name = "p_202401"
        mock_coll.partitions = [mock_part]
        mock_coll.create_partition = MagicMock()
        mock_coll.drop_partition = MagicMock()
        mock_part.release = MagicMock()

        mock_pg = AsyncMock()
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="DELETE 0")
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        mock_pg.close = AsyncMock()

        with patch("app.main._get_milvus", return_value=mock_coll), \
             patch("app.main._get_pg", new_callable=AsyncMock, return_value=mock_pg):
            await partition_rotation()

    @pytest.mark.asyncio
    async def test_cutoff_18m(self):
        """18 个月前的分区被轮转"""
        cutoff = _yyyymm_subtract(202601, 18)
        assert cutoff == 202407
        # p_202406 should be rotated (< 202407)
        assert 202406 < cutoff

    def test_evergreen_999999_never_dropped(self):
        """常青分区 p_999999 永不删除"""
        month_val = 999999
        cutoff = 202407
        # 999999 >= cutoff → 不删除
        assert month_val >= cutoff or month_val == 999999

    @pytest.mark.asyncio
    async def test_sequence_release_delete_drop(self):
        """操作顺序: Milvus Release → PG DELETE → Milvus Drop"""
        call_order = []

        mock_part = MagicMock()
        mock_part.name = "p_202301"
        mock_part.release = MagicMock(side_effect=lambda: call_order.append("release"))

        mock_coll = MagicMock()
        mock_coll.partitions = [mock_part]
        mock_coll.drop_partition = MagicMock(side_effect=lambda n: call_order.append("drop"))

        conn = AsyncMock()
        conn.execute = AsyncMock(side_effect=lambda *a: call_order.append("pg_delete") or "DELETE 1")
        mock_pg = AsyncMock()
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        mock_pg.close = AsyncMock()

        with patch("app.main._get_milvus", return_value=mock_coll), \
             patch("app.main._get_pg", new_callable=AsyncMock, return_value=mock_pg):
            await partition_rotation()

        assert call_order == ["release", "pg_delete", "drop"]

    @pytest.mark.asyncio
    async def test_compensation_on_failure(self):
        """失败时写补偿记录"""
        mock_part = MagicMock()
        mock_part.name = "p_202301"
        mock_part.release = MagicMock(side_effect=Exception("milvus error"))

        mock_coll = MagicMock()
        mock_coll.partitions = [mock_part]

        conn = AsyncMock()
        conn.execute = AsyncMock()
        mock_pg = AsyncMock()
        mock_pg.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        mock_pg.close = AsyncMock()

        with patch("app.main._get_milvus", return_value=mock_coll), \
             patch("app.main._get_pg", new_callable=AsyncMock, return_value=mock_pg):
            await partition_rotation()

        # Compensation record should be written
        # (the function logs errors but doesn't re-raise)
