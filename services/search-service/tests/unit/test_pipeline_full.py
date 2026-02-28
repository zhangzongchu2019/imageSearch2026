"""
完整 8 Stage 流水线测试 (mock 组件)
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sys
sys.path.insert(0, ".")

from app.core.metrics import setup_metrics
setup_metrics()  # 确保 METRICS 已初始化

from app.core.pipeline import Candidate, SearchPipeline, PipelineContext
from app.model.schemas import DegradeState, TimeRange, DataScope, SearchRequest


class TestPipelineFull:
    @pytest.fixture
    def pipeline(self, mock_feature_extractor, mock_ann_searcher,
                  mock_bitmap_filter, mock_refiner, mock_ranker,
                  mock_scope_resolver, mock_vocab_cache, mock_search_logger):
        fsm = MagicMock()
        fsm.state = DegradeState.S0
        fsm.last_reason = ""

        def mock_apply(req):
            params = MagicMock()
            params.merchant_scope = None
            params.merchant_scope_id = None
            params.ef_search = 192
            params.refine_top_k = 2000
            params.enable_cascade = True
            params.enable_fallback = True
            params.time_range = TimeRange.ALL
            params.data_scope = DataScope.ALL
            params.top_k = 100
            return params

        fsm.apply = MagicMock(side_effect=mock_apply)

        # Make bitmap_filter return what it receives
        mock_bitmap_filter.filter_with_degrade = AsyncMock(
            side_effect=lambda candidate_pks=None, merchant_scope=None, candidates=None: (candidates or [], False)
        )
        # Make refiner pass through
        mock_refiner.rerank = AsyncMock(
            side_effect=lambda query_vec=None, candidates=None: candidates or []
        )

        return SearchPipeline(
            degrade_fsm=fsm,
            feature_extractor=mock_feature_extractor,
            ann_searcher=mock_ann_searcher,
            bitmap_filter=mock_bitmap_filter,
            refiner=mock_refiner,
            ranker=mock_ranker,
            scope_resolver=mock_scope_resolver,
            vocab_cache=mock_vocab_cache,
            search_logger=mock_search_logger,
        )

    @pytest.mark.asyncio
    async def test_fast_path_8stage(self, pipeline):
        """快路径: 8 个 Stage 正常执行"""
        req = SearchRequest(query_image="dGVzdA==", top_k=10)
        with patch("app.core.pipeline.settings") as ms:
            ms.feature_flags.enable_tag_recall_stage = True
            ms.feature_flags.enable_refine = True
            ms.feature_flags.enable_fallback = True
            ms.feature_flags.enable_sub_image_search = True
            ms.feature_flags.enable_tag_search = True
            ms.search.dual_path.cascade_trigger = 0.80
            ms.search.ann.hot_timeout_ms = 150
            ms.search.ann.coarse_top_k = 2000
            ms.search.tag_recall.idf_top_k = 5
            ms.search.tag_recall.timeout_ms = 10
            ms.search.tag_recall.inverted_top_k = 500
            ms.search.fallback.score_threshold = 0.80
            ms.search.fallback.sub_image_top_k = 200
            ms.search.fallback.tag_top_k = 200
            ms.search.confidence.high_score = 0.75
            ms.search.confidence.high_min_count = 10
            ms.search.confidence.medium_score = 0.50
            ms.hot_zone.months = 5
            ms.non_hot_zone.months_end = 18
            ms.non_hot_zone.search_list_size = 200
            ms.search.dual_path.cascade_timeout_ms = 250

            resp = await pipeline.execute(req, "test-req-001")

        assert resp is not None
        assert resp.meta.request_id == "test-req-001"

    def test_pipeline_context_timer(self):
        """PipelineContext timer 工作正常"""
        ctx = PipelineContext(request_id="test")
        t = ctx.timer_start("test_stage")
        import time
        time.sleep(0.01)
        ctx.timer_end("test_stage", t)
        assert "test_stage" in ctx.timings
        assert ctx.timings["test_stage"] >= 0

    def test_merge_candidates_dedup(self, pipeline):
        """合并候选去重"""
        hot = [Candidate(image_pk="a" * 32, score=0.9)]
        non_hot = [Candidate(image_pk="a" * 32, score=0.7)]
        tag = [Candidate(image_pk="b" * 32, score=0.6)]
        merged = pipeline._merge_candidates(hot, non_hot, tag)
        pks = [c.image_pk for c in merged]
        assert len(set(pks)) == len(pks)  # no duplicates
        # "a"*32 should keep higher score (0.9)
        a_cand = [c for c in merged if c.image_pk == "a" * 32][0]
        assert a_cand.score == 0.9

    def test_merge_candidates_sorted(self, pipeline):
        """合并后按分数降序"""
        hot = [
            Candidate(image_pk="a" * 32, score=0.5),
            Candidate(image_pk="b" * 32, score=0.9),
        ]
        merged = pipeline._merge_candidates(hot, [], [])
        assert merged[0].score >= merged[1].score
