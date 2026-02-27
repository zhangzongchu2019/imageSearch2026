"""
Milvus 插入测试 (热区分区 / p_999999)
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest
import sys
sys.path.insert(0, ".")

from app.api.update_image import _milvus_upsert


class TestMilvusUpsert:
    @pytest.mark.asyncio
    async def test_hot_partition(self):
        """热区分区写入 (p_YYYYMM)"""
        mock_coll = MagicMock()
        mock_coll.has_partition = MagicMock(return_value=True)
        mock_coll.upsert = MagicMock()

        with patch("app.api.update_image.Collection", return_value=mock_coll):
            data = {"image_pk": "a" * 32, "global_vec": [0.1] * 256}
            await _milvus_upsert(MagicMock(), data, "p_202601")

        mock_coll.upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_evergreen_999999(self):
        """常青分区 p_999999 写入"""
        mock_coll = MagicMock()
        mock_coll.has_partition = MagicMock(return_value=True)
        mock_coll.upsert = MagicMock()

        with patch("app.api.update_image.Collection", return_value=mock_coll):
            data = {"image_pk": "b" * 32, "global_vec": [0.2] * 256}
            await _milvus_upsert(MagicMock(), data, "p_999999")

        mock_coll.upsert.assert_called_once()
        args = mock_coll.upsert.call_args
        assert args[1]["partition_name"] == "p_999999"

    @pytest.mark.asyncio
    async def test_partition_auto_create(self):
        """分区不存在 → 自动创建"""
        mock_coll = MagicMock()
        mock_coll.has_partition = MagicMock(return_value=False)
        mock_coll.create_partition = MagicMock()
        mock_coll.upsert = MagicMock()

        with patch("app.api.update_image.Collection", return_value=mock_coll):
            data = {"image_pk": "c" * 32, "global_vec": [0.3] * 256}
            await _milvus_upsert(MagicMock(), data, "p_202607")

        mock_coll.create_partition.assert_called_once_with("p_202607")

    @pytest.mark.asyncio
    async def test_bounded_executor(self):
        """使用注入的有界线程池"""
        from concurrent.futures import ThreadPoolExecutor
        executor = ThreadPoolExecutor(max_workers=2)
        mock_coll = MagicMock()
        mock_coll.has_partition = MagicMock(return_value=True)
        mock_coll.upsert = MagicMock()

        with patch("app.api.update_image.Collection", return_value=mock_coll):
            data = {"image_pk": "d" * 32, "global_vec": [0.4] * 256}
            await _milvus_upsert(MagicMock(), data, "p_202601", executor)

        mock_coll.upsert.assert_called_once()
        executor.shutdown(wait=True)
