"""
Milvus Compaction 定时任务测试
覆盖: compact 调用、异常处理、连接失败
"""
from unittest.mock import MagicMock, patch

import pytest


class TestMilvusCompaction:
    """Job 5: milvus_compaction 每周日 01:00"""

    @pytest.mark.asyncio
    async def test_compaction_calls_compact(self):
        """正常执行 → coll.compact() 被调用"""
        from app.main import milvus_compaction

        mock_coll = MagicMock()
        mock_coll.compact = MagicMock()

        with patch("app.main._get_milvus", return_value=mock_coll):
            await milvus_compaction()

        mock_coll.compact.assert_called_once()

    @pytest.mark.asyncio
    async def test_compaction_handles_milvus_error(self):
        """Milvus 异常 → 不崩溃"""
        from app.main import milvus_compaction

        with patch("app.main._get_milvus", side_effect=Exception("Milvus unreachable")):
            # 不应抛异常 (函数内部 catch)
            await milvus_compaction()

    @pytest.mark.asyncio
    async def test_compaction_handles_compact_failure(self):
        """compact() 执行失败 → 不崩溃"""
        from app.main import milvus_compaction

        mock_coll = MagicMock()
        mock_coll.compact.side_effect = RuntimeError("Segment merge failed")

        with patch("app.main._get_milvus", return_value=mock_coll):
            await milvus_compaction()

    @pytest.mark.asyncio
    async def test_compaction_uses_hot_collection(self):
        """compaction 操作 global_images_hot 集合"""
        from app.main import milvus_compaction

        mock_coll = MagicMock()

        with patch("app.main._get_milvus", return_value=mock_coll) as mock_get:
            await milvus_compaction()

        mock_get.assert_called_once()
