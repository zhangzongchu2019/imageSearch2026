"""
融合排序 — 四路加权: 全图 0.4 + 子图 0.3 + 标签 0.2 + 类目 0.1
"""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, List

from app.core.config import get_settings
from app.core.pipeline import Candidate, FeatureResult

settings = get_settings()


class FusionRanker:
    """加权融合排序 — 合并全图/子图/标签三路召回结果"""

    def fuse(
        self,
        global_results: List[Candidate],
        sub_results: List[Candidate],
        tag_results: List[Candidate],
        features: FeatureResult,
    ) -> List[Candidate]:
        cfg = settings.search.fusion
        scores: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {"global": 0.0, "sub": 0.0, "tag": 0.0, "cat": 0.0}
        )
        meta: Dict[str, Candidate] = {}

        for r in global_results:
            scores[r.image_pk]["global"] = max(scores[r.image_pk]["global"], r.score)
            if r.category_l1 == features.category_l1_pred:
                scores[r.image_pk]["cat"] = 1.0
            meta[r.image_pk] = r

        for r in sub_results:
            scores[r.image_pk]["sub"] = max(scores[r.image_pk]["sub"], r.score)
            if r.image_pk not in meta:
                meta[r.image_pk] = r

        for r in tag_results:
            scores[r.image_pk]["tag"] = max(scores[r.image_pk]["tag"], r.score)
            if r.image_pk not in meta:
                meta[r.image_pk] = r

        fused = []
        for pk, s in scores.items():
            final_score = (
                cfg.weight_global * s["global"]
                + cfg.weight_sub * s["sub"]
                + cfg.weight_tag * s["tag"]
                + cfg.weight_cat * s["cat"]
            )
            c = meta[pk]
            c.score = final_score
            fused.append(c)

        fused.sort(key=lambda x: x.score, reverse=True)
        return fused
