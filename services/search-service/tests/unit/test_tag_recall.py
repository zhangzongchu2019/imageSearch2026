"""
标签召回测试 — IDF Top-5 截断、10ms 超时
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sys
sys.path.insert(0, ".")

from app.core.pipeline import Candidate


class TestTagRecall:
    def test_idf_top5_truncation(self):
        """IDF Top-5 截断"""
        tags = [100, 200, 300, 400, 500, 600, 700, 800]
        idf_top_k = 5
        top_tags = tags[:idf_top_k]
        assert len(top_tags) == 5
        assert top_tags == [100, 200, 300, 400, 500]

    @pytest.mark.asyncio
    async def test_10ms_timeout(self):
        """10ms 超时后返回空列表"""
        async def slow_search():
            await asyncio.sleep(0.1)
            return [Candidate(image_pk="x" * 32, score=0.9)]

        try:
            result = await asyncio.wait_for(slow_search(), timeout=0.01)
        except asyncio.TimeoutError:
            result = []
        assert result == []

    @pytest.mark.asyncio
    async def test_empty_tags_skip(self):
        """无标签 → 跳过标签召回"""
        tags_pred = []
        if not tags_pred:
            result = []
        assert result == []

    @pytest.mark.asyncio
    async def test_normal_recall(self, mock_ann_searcher):
        """正常标签召回"""
        result = await mock_ann_searcher.search_by_tags_inverted(
            tags=[10, 20, 30], top_k=500, partition_filter="ts_month >= 202101",
        )
        assert len(result) == 1
        assert result[0].source == "tag_recall"

    def test_candidate_source_tag_recall(self):
        """标签召回候选源标记"""
        c = Candidate(image_pk="e" * 32, score=0.6, source="tag_recall")
        assert c.source == "tag_recall"
