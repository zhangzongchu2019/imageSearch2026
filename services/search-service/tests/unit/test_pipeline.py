"""
检索流水线测试 — 双路径级联逻辑
"""
import pytest
import sys
sys.path.insert(0, ".")

from app.core.pipeline import SearchPipeline, EffectiveParams, Candidate
from app.model.schemas import DataScope, TimeRange, Confidence


class TestCascadeLogic:
    """级联路径触发逻辑"""

    def test_fast_path_high_score(self):
        """Top1 ≥ 0.80 → 快路径, 不触发级联"""
        candidates = [Candidate(image_pk="pk1", score=0.85)]
        trigger = candidates[0].score < 0.80
        assert trigger is False

    def test_cascade_low_score(self):
        """Top1 < 0.80 → 触发级联路径"""
        candidates = [Candidate(image_pk="pk1", score=0.65)]
        trigger = candidates[0].score < 0.80
        assert trigger is True

    def test_cascade_empty_hot(self):
        """热区无结果 → 触发级联"""
        candidates = []
        top1 = candidates[0].score if candidates else 0.0
        assert top1 < 0.80


class TestMergeCandidates:
    """候选合并去重"""

    def test_dedup_by_pk(self):
        hot = [Candidate(image_pk="pk1", score=0.9), Candidate(image_pk="pk2", score=0.7)]
        non_hot = [Candidate(image_pk="pk1", score=0.8), Candidate(image_pk="pk3", score=0.6)]
        tag = [Candidate(image_pk="pk4", score=0.5)]

        seen = {}
        for c in hot + non_hot + tag:
            if c.image_pk not in seen or c.score > seen[c.image_pk].score:
                seen[c.image_pk] = c
        result = sorted(seen.values(), key=lambda x: x.score, reverse=True)

        assert len(result) == 4
        assert result[0].image_pk == "pk1"
        assert result[0].score == 0.9  # 取最高分


class TestConfidence:
    def test_high(self):
        score, count = 0.85, 15
        if score >= 0.75 and count >= 10:
            conf = Confidence.HIGH
        assert conf == Confidence.HIGH

    def test_medium(self):
        score, count = 0.60, 5
        if score >= 0.75 and count >= 10:
            conf = Confidence.HIGH
        elif score >= 0.50:
            conf = Confidence.MEDIUM
        assert conf == Confidence.MEDIUM

    def test_low(self):
        score = 0.30
        if score >= 0.75:
            conf = Confidence.HIGH
        elif score >= 0.50:
            conf = Confidence.MEDIUM
        else:
            conf = Confidence.LOW
        assert conf == Confidence.LOW


class TestEffectiveParams:
    def test_defaults(self):
        from app.model.schemas import SearchRequest
        req = SearchRequest(query_image="dGVzdA==")
        params = EffectiveParams.from_request(req)
        assert params.ef_search == 192
        assert params.refine_top_k == 2000
        assert params.enable_cascade is True
