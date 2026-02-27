"""
四路融合排序测试 (global 0.4 + sub 0.3 + tag 0.2 + cat 0.1)
"""
from unittest.mock import MagicMock, patch

import pytest
import sys
sys.path.insert(0, ".")

from app.core.pipeline import Candidate
from app.engine.feature_extractor import FeatureResult


class TestFusionRanker:
    @pytest.fixture
    def features(self):
        return FeatureResult(
            global_vec=[0.1] * 256,
            tags_pred=[10, 20],
            category_l1_pred=42,
        )

    @pytest.fixture
    def ranker(self):
        with patch("app.engine.ranker.settings") as mock_settings:
            mock_settings.search.fusion.weight_global = 0.4
            mock_settings.search.fusion.weight_sub = 0.3
            mock_settings.search.fusion.weight_tag = 0.2
            mock_settings.search.fusion.weight_cat = 0.1
            from app.engine.ranker import FusionRanker
            return FusionRanker()

    def test_global_only(self, ranker, features):
        """仅全图结果"""
        global_results = [
            Candidate(image_pk="a" * 32, score=0.9, category_l1=42),
        ]
        fused = ranker.fuse(global_results, [], [], features)
        assert len(fused) == 1
        # 0.4 * 0.9 + 0.1 * 1.0 (category match) = 0.46
        assert abs(fused[0].score - 0.46) < 0.01

    def test_four_way_fusion(self, ranker, features):
        """四路融合"""
        g = [Candidate(image_pk="a" * 32, score=0.9, category_l1=42)]
        s = [Candidate(image_pk="a" * 32, score=0.8)]
        t = [Candidate(image_pk="a" * 32, score=0.7)]
        fused = ranker.fuse(g, s, t, features)
        assert len(fused) == 1
        # 0.4*0.9 + 0.3*0.8 + 0.2*0.7 + 0.1*1.0 = 0.36+0.24+0.14+0.1 = 0.84
        assert abs(fused[0].score - 0.84) < 0.01

    def test_sorted_descending(self, ranker, features):
        """结果按分数降序"""
        g = [
            Candidate(image_pk="a" * 32, score=0.5),
            Candidate(image_pk="b" * 32, score=0.9, category_l1=42),
        ]
        fused = ranker.fuse(g, [], [], features)
        assert fused[0].score > fused[1].score

    def test_dedup_by_pk(self, ranker, features):
        """相同 image_pk 去重取最高分"""
        g = [Candidate(image_pk="a" * 32, score=0.9)]
        s = [Candidate(image_pk="a" * 32, score=0.8)]
        t = [Candidate(image_pk="a" * 32, score=0.7)]
        fused = ranker.fuse(g, s, t, features)
        assert len(fused) == 1

    def test_no_category_match(self, ranker, features):
        """类目不匹配 → cat 权重为 0"""
        g = [Candidate(image_pk="a" * 32, score=0.9, category_l1=99)]
        fused = ranker.fuse(g, [], [], features)
        # 0.4*0.9 + 0.1*0.0 = 0.36
        assert abs(fused[0].score - 0.36) < 0.01

    def test_empty_all(self, ranker, features):
        """全空 → 空结果"""
        fused = ranker.fuse([], [], [], features)
        assert fused == []
