"""
search-service 共享 fixtures — mock pipeline 组件
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import sys
sys.path.insert(0, ".")


@pytest.fixture
def mock_redis():
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock(return_value=True)
    redis.eval = AsyncMock(return_value=1)
    return redis


@pytest.fixture
def mock_pg():
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchval = AsyncMock(return_value=0)
    conn.execute = AsyncMock()

    pool = AsyncMock()
    pool.acquire = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=conn),
        __aexit__=AsyncMock(),
    ))
    return pool


@pytest.fixture
def mock_feature_extractor():
    from app.engine.feature_extractor import FeatureResult
    extractor = AsyncMock()
    extractor.extract_query = AsyncMock(return_value=FeatureResult(
        global_vec=[0.1] * 256,
        tags_pred=[10, 20, 30, 40, 50],
        category_l1_pred=42,
        sub_vecs=[[0.1] * 128, [0.2] * 128],
        model_version="test-v1",
    ))
    return extractor


@pytest.fixture
def mock_ann_searcher():
    from app.core.pipeline import Candidate
    searcher = AsyncMock()
    searcher.search_hot = AsyncMock(return_value=[
        Candidate(image_pk="a" * 32, score=0.95),
        Candidate(image_pk="b" * 32, score=0.85),
    ])
    searcher.search_non_hot = AsyncMock(return_value=[
        Candidate(image_pk="c" * 32, score=0.75),
    ])
    searcher.search_by_tags_inverted = AsyncMock(return_value=[
        Candidate(image_pk="d" * 32, score=0.70, source="tag_recall"),
    ])
    searcher.search_sub = AsyncMock(return_value=[])
    searcher.search_by_tags = AsyncMock(return_value=[])
    return searcher


@pytest.fixture
def mock_bitmap_filter():
    bmp = AsyncMock()
    bmp.filter_with_degrade = AsyncMock(side_effect=lambda **kw: (kw.get("candidates", []), False))
    return bmp


@pytest.fixture
def mock_refiner():
    refiner = AsyncMock()
    refiner.rerank = AsyncMock(side_effect=lambda **kw: kw.get("candidates", []))
    return refiner


@pytest.fixture
def mock_ranker():
    ranker = MagicMock()
    ranker.fuse = MagicMock(side_effect=lambda g, s, t, f: g + s + t)
    return ranker


@pytest.fixture
def mock_scope_resolver():
    resolver = AsyncMock()
    resolver.resolve = AsyncMock(return_value=["merchant_001", "merchant_002"])
    return resolver


@pytest.fixture
def mock_vocab_cache():
    cache = MagicMock()
    cache.decode = MagicMock(side_effect=lambda vt, code: f"{vt}_{code}" if code else None)
    cache.encode = MagicMock(side_effect=lambda vt, val: hash(val) % 1000 if val else None)
    return cache


@pytest.fixture
def mock_search_logger():
    logger = AsyncMock()
    logger.emit = AsyncMock()
    return logger
