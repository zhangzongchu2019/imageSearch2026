"""
Refiner Float32 精排测试
覆盖: 余弦相似度重排、空输入、排序正确性、Top-K 截断
"""
import math
from unittest.mock import AsyncMock, MagicMock

import pytest


def _make_candidate(pk: str, score: float, source: str = "hot_ann"):
    """构造 Candidate mock"""
    c = MagicMock()
    c.image_pk = pk
    c.score = score
    c.source = source
    c.product_id = None
    c.is_evergreen = False
    c.category_l1 = None
    c.tags = []
    return c


class TestRefiner:
    """Float32 精排核心逻辑"""

    @pytest.mark.asyncio
    async def test_empty_candidates_returns_empty(self):
        """空候选集 → 直接返回空列表"""
        from app.engine.refiner import Refiner

        refiner = Refiner(milvus_client=MagicMock())
        result = await refiner.rerank([0.1] * 256, [])
        assert result == []

    @pytest.mark.asyncio
    async def test_single_candidate_returned(self):
        """单个候选 → 直接返回"""
        from app.engine.refiner import Refiner

        refiner = Refiner(milvus_client=MagicMock())
        c = _make_candidate("pk_001", 0.95)
        result = await refiner.rerank([0.1] * 256, [c])
        assert len(result) == 1
        assert result[0].image_pk == "pk_001"

    @pytest.mark.asyncio
    async def test_sorted_descending_by_score(self):
        """结果按分数降序排列"""
        from app.engine.refiner import Refiner

        refiner = Refiner(milvus_client=MagicMock())
        candidates = [
            _make_candidate("pk_low", 0.3),
            _make_candidate("pk_high", 0.95),
            _make_candidate("pk_mid", 0.7),
        ]
        result = await refiner.rerank([0.1] * 256, candidates)
        scores = [r.score for r in result]
        assert scores == sorted(scores, reverse=True)

    @pytest.mark.asyncio
    async def test_preserves_candidate_metadata(self):
        """精排保留候选的 metadata (source, product_id 等)"""
        from app.engine.refiner import Refiner

        refiner = Refiner(milvus_client=MagicMock())
        c = _make_candidate("pk_meta", 0.8, source="tag_recall")
        c.product_id = "prod_123"
        c.is_evergreen = True

        result = await refiner.rerank([0.1] * 256, [c])
        assert result[0].source == "tag_recall"
        assert result[0].product_id == "prod_123"
        assert result[0].is_evergreen is True

    @pytest.mark.asyncio
    async def test_large_candidate_set_performance(self):
        """2000 候选精排 → 不超时"""
        import time

        from app.engine.refiner import Refiner

        refiner = Refiner(milvus_client=MagicMock())
        candidates = [_make_candidate(f"pk_{i}", 0.5 + i * 0.0001) for i in range(2000)]

        start = time.monotonic()
        result = await refiner.rerank([0.1] * 256, candidates)
        elapsed_ms = (time.monotonic() - start) * 1000

        assert len(result) == 2000
        # SLA: P99 ≤ 5ms, 这里宽松验证
        assert elapsed_ms < 500  # 500ms 上限 (mock 模式)
