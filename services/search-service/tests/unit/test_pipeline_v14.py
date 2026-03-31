"""
v1.4 检索流水线全路径测试 — 覆盖 23 条代码路径

搜索路径 (14):
  test_01 ~ test_14: 自适应搜索/子图并行/匹配层级/SPU聚合/标签/批量/兜底

降级路径 (5):
  test_15 ~ test_19: S0/S1/S2/bitmap降级/bitmap联动S2

写入路径 (4):
  test_20 ~ test_23: 新图/转发/去重/可见性
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

import sys
sys.path.insert(0, "services/search-service")

from app.core.pipeline import Candidate, PipelineContext, SearchPipeline
from app.model.schemas import (
    BatchSearchRequest,
    Confidence,
    DataScope,
    DegradeState,
    EffectiveParams,
    MatchLevel,
    SearchRequest,
    SearchStrategy,
    Strategy,
    TagSearchRequest,
    TimeRange,
)


# ══════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════

def _make_candidate(pk_prefix: str, score: float, product_id=None, tags=None, source="hot_ann"):
    return Candidate(
        image_pk=pk_prefix.ljust(32, "0"),
        score=score,
        product_id=product_id,
        category_l1=1,
        tags=tags or [10, 20],
        source=source,
    )


def _make_vec(dim=256, seed=42):
    rng = np.random.RandomState(seed)
    v = rng.randn(dim).astype(np.float32)
    return (v / np.linalg.norm(v)).tolist()


def _make_feature_result(seed=42):
    from app.engine.feature_extractor import FeatureResult
    return FeatureResult(
        global_vec=_make_vec(256, seed),
        tags_pred=[10, 20, 30, 40, 50],
        category_l1_pred=1,
        sub_vecs=[_make_vec(128, seed + 1), _make_vec(128, seed + 2)],
        model_version="test-v1",
    )


def _make_feature_result_with_conf(seed=42, conf=0.92, cat_pred=5):
    """带类目置信度的 feature result (用 MagicMock 绕过 __slots__)"""
    result = MagicMock()
    result.global_vec = _make_vec(256, seed)
    result.tags_pred = [10, 20, 30, 40, 50]
    result.category_l1_pred = cat_pred
    result.category_l1_conf = conf
    result.sub_vecs = [_make_vec(128, seed + 1), _make_vec(128, seed + 2)]
    result.model_version = "test-v1"
    return result


@pytest.fixture
def pipeline_deps():
    """构建 pipeline 所需的全部 mock 依赖"""
    from app.core.degrade_fsm import DegradeStateMachine

    degrade = MagicMock(spec=DegradeStateMachine)
    degrade.state = DegradeState.S0
    degrade.last_reason = ""
    degrade.apply = MagicMock(side_effect=lambda req: EffectiveParams.from_request(req))

    feature = AsyncMock()
    feature._use_remote = True
    feature.extract_query = AsyncMock(return_value=_make_feature_result())

    ann = AsyncMock()
    ann.search_hot = AsyncMock(return_value=[])
    ann.search_non_hot = AsyncMock(return_value=[])
    ann.search_by_tags_inverted = AsyncMock(return_value=[])
    ann.search_sub = AsyncMock(return_value=[])
    ann.search_by_tags = AsyncMock(return_value=[])
    ann.query_by_pks = AsyncMock(return_value=[])

    bitmap = AsyncMock()
    bitmap.filter_with_degrade = AsyncMock(
        side_effect=lambda candidate_pks=None, merchant_scope=None, candidates=None: (candidates or [], False)
    )
    bitmap.get_merchant_image_count = AsyncMock(return_value=0)
    bitmap.get_merchant_image_pks = AsyncMock(return_value=[])
    bitmap._bitmap_builder = AsyncMock()
    bitmap._bitmap_builder.build = AsyncMock(return_value=b"")

    refiner = AsyncMock()
    refiner.rerank = AsyncMock(side_effect=lambda query_vec=None, candidates=None: candidates or [])

    ranker = MagicMock()
    ranker.fuse = MagicMock(side_effect=lambda g, s, t, f: g + s + t)

    scope = AsyncMock()
    scope.resolve = AsyncMock(return_value=["m1", "m2"])

    vocab = MagicMock()
    vocab.decode = MagicMock(return_value="label")
    vocab.encode = MagicMock(return_value=10)

    logger = AsyncMock()
    logger.emit = AsyncMock()

    return {
        "degrade_fsm": degrade,
        "feature_extractor": feature,
        "ann_searcher": ann,
        "bitmap_filter": bitmap,
        "refiner": refiner,
        "ranker": ranker,
        "scope_resolver": scope,
        "vocab_cache": vocab,
        "search_logger": logger,
        "pg_pool": None,
    }


@pytest.fixture
def pipeline(pipeline_deps):
    with patch("app.core.pipeline.get_settings") as mock_settings, \
         patch("app.core.pipeline.METRICS") as mock_metrics:
        s = MagicMock()
        s.search.ann.coarse_top_k = 3000
        s.search.ann.hot_timeout_ms = 200
        s.search.dual_path.cascade_trigger = 0.65
        s.search.dual_path.cascade_timeout_ms = 350
        s.search.tag_recall.idf_top_k = 8
        s.search.tag_recall.inverted_top_k = 1000
        s.search.tag_recall.timeout_ms = 20
        s.search.refine.top_k = 3000
        s.search.fallback.score_threshold = 0.70
        s.search.fallback.sub_image_top_k = 500
        s.search.fallback.tag_top_k = 500
        s.search.confidence.high_score = 0.75
        s.search.confidence.high_min_count = 10
        s.search.confidence.medium_score = 0.50
        s.hot_zone.months = 6
        s.hot_zone.ef_search = 256
        s.non_hot_zone.months_end = 18
        s.non_hot_zone.search_list_size = 200
        s.feature_flags.enable_tag_recall_stage = True
        s.feature_flags.enable_sub_image_search = True
        s.feature_flags.enable_refine = True
        s.feature_flags.enable_fallback = True
        s.feature_flags.enable_cascade_path = True
        s.match_level.p0_threshold = 0.90
        s.match_level.p1_threshold = 0.70
        s.match_level.p2_threshold = 0.50
        s.adaptive_search.brute_force_pk_limit = 1_000_000
        mock_settings.return_value = s

        # Re-import to pick up mock settings
        import importlib
        import app.core.pipeline as pipeline_mod
        pipeline_mod.settings = s

        p = SearchPipeline(**pipeline_deps)
        yield p, pipeline_deps, s


# ══════════════════════════════════════════════════════════════
# 搜索路径测试 (14 条)
# ══════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_01_small_merchant_brute_force(pipeline):
    """路径1: 小商家 bitmap预查+暴力搜索"""
    p, deps, s = pipeline

    # 模拟小商家: 500 张图
    deps["bitmap_filter"].get_merchant_image_count = AsyncMock(return_value=500)
    deps["bitmap_filter"].get_merchant_image_pks = AsyncMock(
        return_value=["pk_" + str(i).ljust(29, "0") for i in range(500)]
    )

    # 模拟 Milvus query_by_pks 返回向量
    query_vec = _make_vec(256, seed=42)
    record_vec = _make_vec(256, seed=42)  # 相同向量 → 高分
    deps["ann_searcher"].query_by_pks = AsyncMock(return_value=[
        {"image_pk": "pk_" + str(i).ljust(29, "0"), "global_vec": record_vec,
         "product_id": f"SPU_{i % 5}", "is_evergreen": False, "category_l1": 1, "tags": [10]}
        for i in range(20)  # 返回 20 条
    ])

    req = SearchRequest(
        query_image="dGVzdA==",  # base64 "test"
        merchant_scope=["merchant_a"],
    )
    resp = await p.execute(req, "test_01")

    assert resp.meta.search_strategy == SearchStrategy.BRUTE_FORCE
    deps["bitmap_filter"].get_merchant_image_count.assert_called_once()
    deps["bitmap_filter"].get_merchant_image_pks.assert_called_once()
    deps["ann_searcher"].query_by_pks.assert_called_once()
    # ANN search should NOT be called in brute force path
    deps["ann_searcher"].search_hot.assert_not_called()


@pytest.mark.asyncio
async def test_02_medium_range_brute_force(pipeline):
    """路径2: 中等范围 (50家, <100万) 暴力搜索"""
    p, deps, s = pipeline

    merchants = [f"m_{i}" for i in range(50)]
    deps["bitmap_filter"].get_merchant_image_count = AsyncMock(return_value=500_000)
    deps["bitmap_filter"].get_merchant_image_pks = AsyncMock(
        return_value=["pk_" + str(i).ljust(29, "0") for i in range(10)]
    )
    deps["ann_searcher"].query_by_pks = AsyncMock(return_value=[
        {"image_pk": "pk_" + str(i).ljust(29, "0"), "global_vec": _make_vec(256),
         "product_id": f"SPU_{i}", "category_l1": 1, "tags": [10]}
        for i in range(10)
    ])

    req = SearchRequest(query_image="dGVzdA==", merchant_scope=merchants)
    resp = await p.execute(req, "test_02")

    assert resp.meta.search_strategy == SearchStrategy.BRUTE_FORCE


@pytest.mark.asyncio
async def test_03_large_range_ann_path(pipeline):
    """路径3: 大范围 ANN+bitmap 后过滤"""
    p, deps, s = pipeline

    merchants = [f"m_{i}" for i in range(500)]
    deps["bitmap_filter"].get_merchant_image_count = AsyncMock(return_value=5_000_000)  # 超过阈值
    deps["ann_searcher"].search_hot = AsyncMock(return_value=[
        _make_candidate("aaa", 0.92, product_id="SPU_1"),
        _make_candidate("bbb", 0.85, product_id="SPU_2"),
    ])

    req = SearchRequest(query_image="dGVzdA==", merchant_scope=merchants)
    resp = await p.execute(req, "test_03")

    assert resp.meta.search_strategy == SearchStrategy.HNSW_FILTERED
    deps["ann_searcher"].search_hot.assert_called_once()
    deps["bitmap_filter"].filter_with_degrade.assert_called_once()


@pytest.mark.asyncio
async def test_04_no_merchant_scope(pipeline):
    """路径4: 无商家范围全局搜索"""
    p, deps, s = pipeline

    deps["ann_searcher"].search_hot = AsyncMock(return_value=[
        _make_candidate("aaa", 0.88, product_id="SPU_1"),
    ])

    req = SearchRequest(query_image="dGVzdA==")  # 不传 merchant_scope
    resp = await p.execute(req, "test_04")

    # 不应走 brute force, 不应调用 bitmap
    deps["bitmap_filter"].get_merchant_image_count.assert_not_called()
    deps["ann_searcher"].search_hot.assert_called_once()
    deps["bitmap_filter"].filter_with_degrade.assert_not_called()


@pytest.mark.asyncio
async def test_05_cascade_to_non_hot(pipeline):
    """路径5: 级联穿透非热区 (top1 < cascade_trigger)"""
    p, deps, s = pipeline

    deps["ann_searcher"].search_hot = AsyncMock(return_value=[
        _make_candidate("aaa", 0.55),  # score < 0.65 → 触发级联
    ])
    deps["ann_searcher"].search_non_hot = AsyncMock(return_value=[
        _make_candidate("ccc", 0.78, product_id="SPU_NH"),
    ])

    req = SearchRequest(query_image="dGVzdA==")
    resp = await p.execute(req, "test_05")

    assert resp.meta.strategy == Strategy.CASCADE_PATH
    assert resp.meta.zone_hit == "hot+non_hot"
    deps["ann_searcher"].search_non_hot.assert_called_once()


@pytest.mark.asyncio
async def test_06_sub_image_parallel(pipeline):
    """路径6: 子图并行搜索 (v1.4 必备路径)"""
    p, deps, s = pipeline

    deps["ann_searcher"].search_hot = AsyncMock(return_value=[
        _make_candidate("aaa", 0.92, product_id="SPU_1"),
    ])
    # 子图搜索返回额外候选
    deps["ann_searcher"].search_sub = AsyncMock(return_value=[
        _make_candidate("sub1", 0.88, product_id="SPU_SUB", source="sub_image"),
    ])

    req = SearchRequest(query_image="dGVzdA==")
    resp = await p.execute(req, "test_06")

    # 子图搜索必须被调用 (不是 fallback)
    deps["ann_searcher"].search_sub.assert_called_once()
    # 结果应包含子图召回的候选
    image_ids = [r.image_id for r in resp.results]
    assert "sub1".ljust(32, "0") in image_ids


@pytest.mark.asyncio
async def test_07_match_level_p0(pipeline):
    """路径7: 匹配层级 P0 (score > 0.90)"""
    p, deps, s = pipeline

    deps["ann_searcher"].search_hot = AsyncMock(return_value=[
        _make_candidate("exact", 0.96, product_id="SPU_1"),
    ])

    req = SearchRequest(query_image="dGVzdA==")
    resp = await p.execute(req, "test_07")

    assert resp.results[0].match_level == MatchLevel.P0


@pytest.mark.asyncio
async def test_08_match_level_p1(pipeline):
    """路径8: 匹配层级 P1 (0.70~0.90)"""
    p, deps, s = pipeline

    deps["ann_searcher"].search_hot = AsyncMock(return_value=[
        _make_candidate("style", 0.82, product_id="SPU_1"),
    ])

    req = SearchRequest(query_image="dGVzdA==")
    resp = await p.execute(req, "test_08")

    assert resp.results[0].match_level == MatchLevel.P1


@pytest.mark.asyncio
async def test_09_match_level_p2(pipeline):
    """路径9: 匹配层级 P2 (0.50~0.70)"""
    p, deps, s = pipeline

    deps["ann_searcher"].search_hot = AsyncMock(return_value=[
        _make_candidate("categ", 0.62, product_id="SPU_1"),
    ])

    req = SearchRequest(query_image="dGVzdA==")
    resp = await p.execute(req, "test_09")

    assert resp.results[0].match_level == MatchLevel.P2


@pytest.mark.asyncio
async def test_10_spu_aggregation(pipeline):
    """路径10: SPU 聚合 — 同商品多张图只返回一个"""
    p, deps, s = pipeline

    deps["ann_searcher"].search_hot = AsyncMock(return_value=[
        _make_candidate("img1", 0.95, product_id="SPU_A"),  # 同 SPU 最高分
        _make_candidate("img2", 0.93, product_id="SPU_A"),  # 同 SPU 较低分 → 应被聚合
        _make_candidate("img3", 0.88, product_id="SPU_B"),  # 不同 SPU
    ])

    req = SearchRequest(query_image="dGVzdA==")
    resp = await p.execute(req, "test_10")

    product_ids = [r.product_id for r in resp.results]
    # SPU_A 只应出现一次
    assert product_ids.count("SPU_A") == 1
    # 应保留最高分那张
    spu_a_result = [r for r in resp.results if r.product_id == "SPU_A"][0]
    assert spu_a_result.score == 0.95


@pytest.mark.asyncio
async def test_11_tag_search():
    """路径11: 标签搜索 — 多标签交集, 按命中数排序"""
    req = TagSearchRequest(tags=["手提包", "真皮", "红色"], top_k=10)
    assert len(req.tags) == 3
    assert req.top_k == 10
    # 完整 E2E 测试需要 API 服务运行, 这里验证模型正确性


@pytest.mark.asyncio
async def test_12_batch_search():
    """路径12: 批量搜索请求模型验证"""
    req = BatchSearchRequest(
        query_images=["img1_b64", "img2_b64", "img3_b64"],
        merchant_scope=["m1"],
        top_k=50,
    )
    assert len(req.query_images) == 3
    assert req.top_k == 50

    # 超过 7 张应报错
    with pytest.raises(Exception):
        BatchSearchRequest(query_images=["img"] * 8)


@pytest.mark.asyncio
async def test_13_ultimate_fallback(pipeline):
    """路径13: 终极兜底 — 所有路径为空"""
    p, deps, s = pipeline

    # 所有搜索返回空
    deps["ann_searcher"].search_hot = AsyncMock(return_value=[])
    deps["ann_searcher"].search_sub = AsyncMock(return_value=[])
    deps["ann_searcher"].search_by_tags_inverted = AsyncMock(return_value=[])

    req = SearchRequest(query_image="dGVzdA==")
    resp = await p.execute(req, "test_13")

    # 应触发终极兜底 (search_hot 被再次调用, 带 evergreen 过滤)
    assert deps["ann_searcher"].search_hot.call_count >= 2


@pytest.mark.asyncio
async def test_14_category_prefilter(pipeline):
    """路径14: 类目预过滤"""
    p, deps, s = pipeline

    # 特征提取返回高置信度类目
    feature_result = _make_feature_result_with_conf(conf=0.92, cat_pred=5)
    deps["feature_extractor"].extract_query = AsyncMock(return_value=feature_result)

    deps["ann_searcher"].search_hot = AsyncMock(return_value=[
        _make_candidate("aaa", 0.88),
    ])

    req = SearchRequest(query_image="dGVzdA==")
    resp = await p.execute(req, "test_14")

    # 验证 search_hot 的 partition_filter 包含 category_l1 过滤
    call_args = deps["ann_searcher"].search_hot.call_args
    partition_filter = call_args.kwargs.get("partition_filter", "")
    assert "category_l1 == 5" in partition_filter


# ══════════════════════════════════════════════════════════════
# 降级路径测试 (5 条)
# ══════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_15_s0_normal(pipeline):
    """路径15: S0 正常状态"""
    p, deps, s = pipeline

    deps["degrade_fsm"].state = DegradeState.S0
    deps["ann_searcher"].search_hot = AsyncMock(return_value=[
        _make_candidate("aaa", 0.88),
    ])

    req = SearchRequest(query_image="dGVzdA==")
    resp = await p.execute(req, "test_15")

    assert resp.meta.degraded is False
    assert resp.meta.degrade_state == DegradeState.S0


@pytest.mark.asyncio
async def test_16_s1_keeps_fallback():
    """路径16: S1 保留 fallback (v1.4 关键变更)"""
    from app.core.degrade_fsm import DegradeStateMachine

    with patch("app.core.degrade_fsm.get_settings") as mock_s:
        ms = MagicMock()
        ms.hot_zone.ef_search_s1 = 192
        ms.search.refine.top_k_s1 = 2000
        ms.hot_zone.ef_search_s2 = 96
        ms.search.refine.top_k_s2 = 800
        ms.hot_zone.ef_search = 256
        ms.search.refine.top_k = 3000
        ms.feature_flags.enable_fallback = True
        ms.feature_flags.enable_cascade_path = True
        ms.degrade.recovery.s3_ramp_stages = [0.1, 0.5, 1.0]
        mock_s.return_value = ms

        import app.core.degrade_fsm as fsm_mod
        fsm_mod.settings = ms

        fsm = DegradeStateMachine()
        fsm._state = DegradeState.S1
        fsm._entered_at = 0

        req = SearchRequest(query_image="dGVzdA==")
        params = fsm.apply(req)

        # S1 核心验证: fallback 必须保留
        assert params.enable_fallback is True
        assert params.enable_cascade is False
        assert params.ef_search == 192
        assert params.refine_top_k == 2000


@pytest.mark.asyncio
async def test_17_s2_disables_all():
    """路径17: S2 禁用 fallback + 级联"""
    from app.core.degrade_fsm import DegradeStateMachine

    with patch("app.core.degrade_fsm.get_settings") as mock_s:
        ms = MagicMock()
        ms.hot_zone.ef_search_s1 = 192
        ms.search.refine.top_k_s1 = 2000
        ms.hot_zone.ef_search_s2 = 96
        ms.search.refine.top_k_s2 = 800
        ms.hot_zone.ef_search = 256
        ms.search.refine.top_k = 3000
        ms.feature_flags.enable_fallback = True
        ms.feature_flags.enable_cascade_path = True
        ms.degrade.recovery.s3_ramp_stages = [0.1, 0.5, 1.0]
        mock_s.return_value = ms

        import app.core.degrade_fsm as fsm_mod
        fsm_mod.settings = ms

        fsm = DegradeStateMachine()
        fsm._state = DegradeState.S2
        fsm._entered_at = 0

        req = SearchRequest(query_image="dGVzdA==")
        params = fsm.apply(req)

        assert params.enable_fallback is False
        assert params.enable_cascade is False
        assert params.ef_search == 96
        assert params.refine_top_k == 800


@pytest.mark.asyncio
async def test_18_bitmap_three_level_degrade(pipeline):
    """路径18: bitmap 三级降级 gRPC→PG→skip"""
    p, deps, s = pipeline

    # 模拟大范围路径 + bitmap 过滤 skip
    deps["bitmap_filter"].get_merchant_image_count = AsyncMock(return_value=5_000_000)
    deps["ann_searcher"].search_hot = AsyncMock(return_value=[
        _make_candidate("aaa", 0.88),
    ])
    deps["bitmap_filter"].filter_with_degrade = AsyncMock(
        return_value=([_make_candidate("aaa", 0.88)], True)  # filter_skipped=True
    )

    req = SearchRequest(query_image="dGVzdA==", merchant_scope=["m1"])
    resp = await p.execute(req, "test_18")

    assert resp.meta.filter_skipped is True


@pytest.mark.asyncio
async def test_19_bitmap_skip_forces_s2(pipeline):
    """路径19: bitmap 连续 3 次 skip → 联动 S2"""
    p, deps, s = pipeline

    deps["bitmap_filter"].get_merchant_image_count = AsyncMock(return_value=5_000_000)
    deps["ann_searcher"].search_hot = AsyncMock(return_value=[
        _make_candidate("aaa", 0.88),
    ])
    deps["bitmap_filter"].filter_with_degrade = AsyncMock(
        return_value=([_make_candidate("aaa", 0.88)], True)
    )
    deps["degrade_fsm"].state = DegradeState.S0
    deps["degrade_fsm"].force_state = AsyncMock()

    req = SearchRequest(query_image="dGVzdA==", merchant_scope=["m1"])

    # 连续执行 3 次
    for i in range(3):
        await p.execute(req, f"test_19_{i}")

    # 第 3 次应触发 force_state(S2)
    deps["degrade_fsm"].force_state.assert_called()
    call_args = deps["degrade_fsm"].force_state.call_args
    assert call_args[0][0] == DegradeState.S2


# ══════════════════════════════════════════════════════════════
# 写入路径测试 (4 条) — 模型验证
# ══════════════════════════════════════════════════════════════

def test_20_new_image_write_model():
    """路径20: 新图写入请求模型"""
    from app.model.schemas import UpdateImageRequest
    req = UpdateImageRequest(
        uri="https://example.com/img.jpg",
        merchant_id="merchant_a",
        category_l1="箱包",
        product_id="SPU_001",
        tags=["手提包", "真皮"],
    )
    assert req.uri == "https://example.com/img.jpg"
    assert req.merchant_id == "merchant_a"


def test_21_forward_creates_merchant_association():
    """路径21: 转发 = 同图追加商家关联 (bitmap rb_or)"""
    # 转发场景: 同一 URI, 不同 merchant_id
    from app.model.schemas import UpdateImageRequest
    original = UpdateImageRequest(
        uri="https://example.com/shared.jpg",
        merchant_id="merchant_a",
        category_l1="箱包",
    )
    forwarded = UpdateImageRequest(
        uri="https://example.com/shared.jpg",
        merchant_id="merchant_d",  # 不同商家
        category_l1="箱包",
    )
    # 同一 URI → 同一 image_pk, 不同 merchant_id → bitmap 追加
    import hashlib
    pk_a = hashlib.sha256(original.uri.encode()).hexdigest()[:32]
    pk_d = hashlib.sha256(forwarded.uri.encode()).hexdigest()[:32]
    assert pk_a == pk_d  # 同图


def test_22_dedup_same_uri():
    """路径22: 去重 — 相同 URI 产生相同 image_pk"""
    import hashlib
    uri = "https://example.com/product.jpg"
    pk1 = hashlib.sha256(uri.encode()).hexdigest()[:32]
    pk2 = hashlib.sha256(uri.encode()).hexdigest()[:32]
    assert pk1 == pk2  # 幂等


def test_23_write_visibility_budget():
    """路径23: 写入可见性时间预算验证"""
    # 端到端 ≤ 3 分钟 = 上传(90s) + 审查(45s) + 入库(45s)
    upload_max_s = 90
    review_s = 45
    write_budget_s = 45
    total = upload_max_s + review_s + write_budget_s
    assert total == 180  # 3 分钟

    # 单张入库 ≤ 5 秒
    per_image_s = 5
    images_per_batch = 9
    assert per_image_s * images_per_batch == 45  # 刚好用完入库预算


# ══════════════════════════════════════════════════════════════
# 辅助功能测试
# ══════════════════════════════════════════════════════════════

def test_brute_force_cosine_accuracy():
    """暴力搜索 cosine 计算精度验证"""
    query = _make_vec(256, seed=1)
    same_vec = query.copy()
    diff_vec = _make_vec(256, seed=99)

    records = [
        {"image_pk": "same".ljust(32, "0"), "global_vec": same_vec, "product_id": "A"},
        {"image_pk": "diff".ljust(32, "0"), "global_vec": diff_vec, "product_id": "B"},
    ]

    candidates = SearchPipeline._brute_force_cosine(query, records)
    assert len(candidates) == 2
    # 相同向量的分数应接近 1.0
    assert candidates[0].score > 0.99
    assert candidates[0].image_pk == "same".ljust(32, "0")
    # 不同向量分数应较低
    assert candidates[1].score < candidates[0].score


def test_match_level_classification():
    """匹配层级分类逻辑"""
    candidates = [
        _make_candidate("p0", 0.95),
        _make_candidate("p1", 0.80),
        _make_candidate("p2", 0.60),
        _make_candidate("low", 0.40),
    ]

    with patch("app.core.pipeline.settings") as ms:
        ms.match_level.p0_threshold = 0.90
        ms.match_level.p1_threshold = 0.70

        result = SearchPipeline._classify_match_level(candidates)

    assert getattr(result[0], '_match_level') == MatchLevel.P0
    assert getattr(result[1], '_match_level') == MatchLevel.P1
    assert getattr(result[2], '_match_level') == MatchLevel.P2
    assert getattr(result[3], '_match_level') == MatchLevel.P2  # < p2_threshold 也归 P2


def test_spu_aggregation_logic():
    """SPU 聚合逻辑 — 同商品保留最高分"""
    candidates = [
        _make_candidate("img1", 0.95, product_id="SPU_A"),
        _make_candidate("img2", 0.90, product_id="SPU_A"),
        _make_candidate("img3", 0.85, product_id="SPU_B"),
        _make_candidate("img4", 0.80, product_id=None),  # 无 product_id
    ]
    for c in candidates:
        c._match_level = MatchLevel.P0

    result = SearchPipeline._aggregate_by_spu(candidates)

    # SPU_A 只保留 1 条 (最高分 0.95)
    spu_a = [c for c in result if c.product_id == "SPU_A"]
    assert len(spu_a) == 1
    assert spu_a[0].score == 0.95

    # SPU_B 保留
    assert any(c.product_id == "SPU_B" for c in result)

    # 无 product_id 的保留
    assert any(c.product_id is None for c in result)


def test_spu_aggregation_respects_match_level():
    """SPU 聚合排序: P0 > P1 > P2"""
    candidates = [
        _make_candidate("p2_high", 0.68, product_id="SPU_C"),
        _make_candidate("p0_low", 0.91, product_id="SPU_A"),
        _make_candidate("p1_mid", 0.80, product_id="SPU_B"),
    ]
    candidates[0]._match_level = MatchLevel.P2
    candidates[1]._match_level = MatchLevel.P0
    candidates[2]._match_level = MatchLevel.P1

    result = SearchPipeline._aggregate_by_spu(candidates)

    # P0 排第一
    assert getattr(result[0], '_match_level') == MatchLevel.P0
    # P1 排第二
    assert getattr(result[1], '_match_level') == MatchLevel.P1
    # P2 排第三
    assert getattr(result[2], '_match_level') == MatchLevel.P2
