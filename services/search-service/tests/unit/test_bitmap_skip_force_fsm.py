"""
连续 3 次 bitmap skip → 强制 S2 测试
"""
from unittest.mock import MagicMock

import pytest
import sys
sys.path.insert(0, ".")

from app.core.pipeline import SearchPipeline
from app.model.schemas import DegradeState


class TestBitmapSkipForceFSM:
    @pytest.fixture
    def pipeline_with_fsm(self, mock_feature_extractor, mock_ann_searcher,
                           mock_bitmap_filter, mock_refiner, mock_ranker,
                           mock_scope_resolver, mock_vocab_cache, mock_search_logger):
        fsm = MagicMock()
        fsm.state = DegradeState.S0
        fsm.force_state = MagicMock()
        fsm.last_reason = ""
        fsm.apply = MagicMock(side_effect=lambda req: MagicMock(
            merchant_scope=["m1"], ef_search=192, refine_top_k=2000,
            enable_cascade=True, enable_fallback=True,
            time_range=MagicMock(value="all"),
            data_scope=MagicMock(value="all"),
            top_k=100,
        ))

        pipeline = SearchPipeline(
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
        return pipeline, fsm

    def test_threshold_constant(self):
        """阈值常量 = 3"""
        assert SearchPipeline.BITMAP_SKIP_FORCE_DEGRADE_THRESHOLD == 3

    def test_counter_increments(self, pipeline_with_fsm):
        """连续 skip 计数器递增"""
        pipeline, fsm = pipeline_with_fsm
        pipeline._bitmap_skip_consecutive = 0

        # 模拟 filter_skipped=True
        for _ in range(2):
            pipeline._bitmap_skip_consecutive += 1
        assert pipeline._bitmap_skip_consecutive == 2

    def test_force_s2_on_3_consecutive(self, pipeline_with_fsm):
        """连续 3 次 skip → 强制 S2"""
        pipeline, fsm = pipeline_with_fsm
        pipeline._bitmap_skip_consecutive = 2
        fsm.state = DegradeState.S0

        # 第 3 次 skip
        pipeline._bitmap_skip_consecutive += 1
        if pipeline._bitmap_skip_consecutive >= pipeline.BITMAP_SKIP_FORCE_DEGRADE_THRESHOLD:
            if fsm.state in (DegradeState.S0, DegradeState.S1):
                fsm.force_state(DegradeState.S2, reason="bitmap_filter_skipped_3_consecutive")

        fsm.force_state.assert_called_once_with(
            DegradeState.S2, reason="bitmap_filter_skipped_3_consecutive"
        )

    def test_counter_resets_on_success(self, pipeline_with_fsm):
        """成功过滤后重置计数器"""
        pipeline, fsm = pipeline_with_fsm
        pipeline._bitmap_skip_consecutive = 2

        # 模拟成功过滤 (filter_skipped=False)
        pipeline._bitmap_skip_consecutive = 0
        assert pipeline._bitmap_skip_consecutive == 0

    def test_no_force_if_already_s2(self, pipeline_with_fsm):
        """已在 S2 → 不重复触发"""
        pipeline, fsm = pipeline_with_fsm
        fsm.state = DegradeState.S2
        pipeline._bitmap_skip_consecutive = 5

        if pipeline._bitmap_skip_consecutive >= pipeline.BITMAP_SKIP_FORCE_DEGRADE_THRESHOLD:
            if fsm.state in (DegradeState.S0, DegradeState.S1):
                fsm.force_state(DegradeState.S2, reason="test")

        fsm.force_state.assert_not_called()
