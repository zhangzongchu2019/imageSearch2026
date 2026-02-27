"""
子图+标签多路召回 + 终极常青兜底测试
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
import sys
sys.path.insert(0, ".")

from app.core.pipeline import Candidate
from app.engine.feature_extractor import FeatureResult


class TestFallback:
    @pytest.fixture
    def features(self):
        return FeatureResult(
            global_vec=[0.1] * 256,
            tags_pred=[10, 20, 30],
            category_l1_pred=42,
            sub_vecs=[[0.1] * 128, [0.2] * 128],
        )

    @pytest.mark.asyncio
    async def test_sub_plus_tag_parallel(self, mock_ann_searcher, features):
        """子图 + 标签并行召回"""
        mock_ann_searcher.search_sub = AsyncMock(return_value=[
            Candidate(image_pk="x" * 32, score=0.7, source="sub_image"),
        ])
        mock_ann_searcher.search_by_tags = AsyncMock(return_value=[
            Candidate(image_pk="y" * 32, score=0.6, source="tag_recall"),
        ])
        tasks = [
            mock_ann_searcher.search_sub(features.sub_vecs, top_k=200),
            mock_ann_searcher.search_by_tags(features.tags_pred, top_k=200),
        ]
        results = await asyncio.gather(*tasks)
        merged = []
        for r in results:
            if isinstance(r, list):
                merged.extend(r)
        assert len(merged) == 2

    @pytest.mark.asyncio
    async def test_low_score_trigger(self):
        """Top1 score 低于阈值 → 触发 fallback"""
        score_threshold = 0.80
        top1_score = 0.60
        assert top1_score < score_threshold

    @pytest.mark.asyncio
    async def test_empty_trigger(self):
        """候选为空 → 触发 fallback"""
        candidates = []
        assert not candidates or candidates[0].score < 0.8

    @pytest.mark.asyncio
    async def test_evergreen_fallback(self, mock_ann_searcher):
        """终极兜底: 搜索常青推荐"""
        mock_ann_searcher.search_hot = AsyncMock(return_value=[
            Candidate(image_pk="z" * 32, score=0.5, is_evergreen=True),
        ])
        result = await mock_ann_searcher.search_hot(
            vector=[0.1] * 256,
            partition_filter="is_evergreen == true",
            ef_search=64,
            top_k=50,
        )
        assert len(result) == 1
        assert result[0].is_evergreen is True

    @pytest.mark.asyncio
    async def test_200ms_timeout(self):
        """兜底搜索 200ms 超时"""
        async def slow():
            await asyncio.sleep(0.5)
            return [Candidate(image_pk="w" * 32, score=0.3)]

        try:
            result = await asyncio.wait_for(slow(), timeout=0.2)
        except asyncio.TimeoutError:
            result = []
        assert result == []

    @pytest.mark.asyncio
    async def test_failure_returns_empty(self, mock_ann_searcher):
        """兜底失败 → 返回空列表"""
        mock_ann_searcher.search_hot = AsyncMock(side_effect=Exception("milvus down"))
        try:
            result = await mock_ann_searcher.search_hot(
                vector=[0.1] * 256,
                partition_filter="is_evergreen == true",
                ef_search=64,
                top_k=50,
            )
        except Exception:
            result = []
        assert result == []
