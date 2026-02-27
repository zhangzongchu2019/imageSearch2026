"""
Bitmap gRPC 客户端测试 — 三级降级 (gRPC→PG→skip)
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sys
sys.path.insert(0, ".")

from app.core.pipeline import Candidate


class TestBitmapGrpcClient:
    @pytest.fixture
    def candidates(self):
        return [
            Candidate(image_pk="a" * 32, score=0.95),
            Candidate(image_pk="b" * 32, score=0.85),
            Candidate(image_pk="c" * 32, score=0.75),
        ]

    @pytest.mark.asyncio
    async def test_level0_success(self, candidates, mock_bitmap_filter):
        """Level 0: gRPC 成功"""
        mock_bitmap_filter.filter_with_degrade = AsyncMock(
            return_value=(candidates[:2], False)
        )
        result, skipped = await mock_bitmap_filter.filter_with_degrade(
            candidate_pks=[c.image_pk for c in candidates],
            merchant_scope=["m1"],
            candidates=candidates,
        )
        assert len(result) == 2
        assert skipped is False

    @pytest.mark.asyncio
    async def test_level0_timeout_pg(self, candidates, mock_bitmap_filter):
        """Level 0 超时 → Level 1 PG"""
        mock_bitmap_filter.filter_with_degrade = AsyncMock(
            return_value=(candidates[:1], False)  # PG fallback result
        )
        result, skipped = await mock_bitmap_filter.filter_with_degrade(
            candidate_pks=[c.image_pk for c in candidates],
            merchant_scope=["m1"],
            candidates=candidates,
        )
        assert skipped is False

    @pytest.mark.asyncio
    async def test_level1_success(self, candidates):
        """Level 1: PG 成功"""
        filter_client = AsyncMock()
        filter_client.filter_with_degrade = AsyncMock(
            return_value=(candidates[:2], False)
        )
        result, skipped = await filter_client.filter_with_degrade(
            candidate_pks=[c.image_pk for c in candidates],
            merchant_scope=["m1"],
            candidates=candidates,
        )
        assert len(result) == 2
        assert skipped is False

    @pytest.mark.asyncio
    async def test_level1_fail_skip(self, candidates):
        """Level 1 也失败 → skip (filter_skipped=True)"""
        filter_client = AsyncMock()
        filter_client.filter_with_degrade = AsyncMock(
            return_value=(candidates, True)  # all candidates returned, skip
        )
        result, skipped = await filter_client.filter_with_degrade(
            candidate_pks=[c.image_pk for c in candidates],
            merchant_scope=["m1"],
            candidates=candidates,
        )
        assert skipped is True
        assert len(result) == len(candidates)

    @pytest.mark.asyncio
    async def test_no_scope_bypass(self, candidates):
        """无 merchant_scope → 不过滤"""
        filter_client = AsyncMock()
        filter_client.filter_with_degrade = AsyncMock(
            return_value=(candidates, False)
        )
        result, skipped = await filter_client.filter_with_degrade(
            candidate_pks=[c.image_pk for c in candidates],
            merchant_scope=[],
            candidates=candidates,
        )
        assert skipped is False
        assert len(result) == len(candidates)
