"""
级联检索流水线 — v1.4 自适应搜索 + 子图并行 + 匹配层级
Stage 0   : 降级覆盖 + merchant_scope 解析 + 商家规模判定
Stage 0.5 : bitmap 预查 pk / 预构建 (与 Stage 1 并行)
Stage 1   : 特征提取 (TensorRT FP16, GPU→CPU fallback)
            ┌── 小范围路径 ──────────── 大范围路径 ──┐
Stage 2   : Milvus pk取向量+暴力搜索    ANN+标签并行   │
Stage 2-NH: (不需要级联)               [级联] DiskANN  │
Stage 3   : (不需要bitmap)             bitmap后过滤    │
            └─────────────┬────────────────────────────┘
Stage 4   : 全局 + 子图 **双路并行** + Refine 精排
Stage 5   : 匹配层级 P0/P1/P2 + SPU 聚合
Stage 6   : 响应构建 + 异步日志

v1.4 架构原则:
  - 召回率和准确性优先, 执行时间次之
  - 能在调用前缩小范围就先缩小范围
  - 自适应搜索: 小商家 bitmap预查+暴力搜索, 大范围 ANN+bitmap后过滤
  - 子图搜索为必备并行路径, 非可选 fallback
  - 结果按匹配层级 P0>P1>P2 分层排序
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import structlog

from app.core.config import get_settings
from app.core.degrade_fsm import DegradeStateMachine
from app.core.metrics import METRICS
from app.model.schemas import (
    Confidence,
    DataScope,
    DegradeState,
    EffectiveParams,
    EffectiveParamsSnapshot,
    MatchLevel,
    SearchMeta,
    SearchRequest,
    SearchResponse,
    SearchResultItem,
    SearchStrategy,
    Strategy,
    TimeRange,
)

logger = structlog.get_logger(__name__)
settings = get_settings()


# ── 内部数据结构 ──

@dataclass
class PipelineContext:
    """流水线执行上下文, 贯穿所有 Stage"""
    request_id: str
    start_ns: int = field(default_factory=time.monotonic_ns)
    timings: Dict[str, int] = field(default_factory=dict)
    degrade_state: DegradeState = DegradeState.S0
    degrade_reason: Optional[str] = None
    degraded: bool = False
    filter_skipped: bool = False
    strategy: Strategy = Strategy.FAST_PATH
    search_strategy: SearchStrategy = SearchStrategy.BRUTE_FORCE
    zone_hit: str = "hot"

    def timer_start(self, stage: str) -> int:
        return time.monotonic_ns()

    def timer_end(self, stage: str, start: int):
        elapsed_ms = (time.monotonic_ns() - start) // 1_000_000
        self.timings[stage] = elapsed_ms
        METRICS.search_stage_latency.labels(stage=stage).observe(elapsed_ms / 1000)

    @property
    def total_ms(self) -> int:
        return (time.monotonic_ns() - self.start_ns) // 1_000_000


@dataclass
class Candidate:
    """检索候选项"""
    image_pk: str
    score: float
    product_id: Optional[str] = None
    is_evergreen: bool = False
    category_l1: Optional[int] = None
    category_l2: Optional[int] = None
    tags: Optional[List[int]] = None
    source: str = "hot_ann"  # hot_ann | non_hot_ann | tag_recall | sub_image | brute_force


from app.engine.feature_extractor import FeatureResult


# ── 核心流水线 ──

class SearchPipeline:
    """v1.4 自适应检索流水线

    根据商家范围大小自动选择最优搜索策略:
    - 小范围 (pk ≤ brute_force_pk_limit): bitmap 预查 pk → Milvus 取向量 → 暴力搜索
    - 大范围: Milvus ANN 粗召回 → bitmap 后过滤
    - 子图搜索为必备并行路径
    """

    BITMAP_SKIP_FORCE_DEGRADE_THRESHOLD = 3
    MERCHANT_SCOPE_TOP_K_MULTIPLIER = 1.5

    def __init__(
        self,
        degrade_fsm: DegradeStateMachine,
        feature_extractor,
        ann_searcher,
        bitmap_filter,
        refiner,
        ranker,
        scope_resolver,
        vocab_cache,
        search_logger,
        pg_pool=None,
    ):
        self._degrade = degrade_fsm
        self._feature = feature_extractor
        self._ann = ann_searcher
        self._bitmap = bitmap_filter
        self._refiner = refiner
        self._ranker = ranker
        self._scope = scope_resolver
        self._vocab = vocab_cache
        self._logger = search_logger
        self._pg_pool = pg_pool
        self._bitmap_skip_consecutive = 0

    # ════════════════════════════════════════════════════════════
    # 主入口
    # ════════════════════════════════════════════════════════════

    async def execute(self, req: SearchRequest, request_id: str) -> SearchResponse:
        ctx = PipelineContext(request_id=request_id)

        # ── Stage 0: 降级覆盖 + Scope 解析 ──
        params = self._degrade.apply(req)
        ctx.degrade_state = self._degrade.state
        ctx.degraded = self._degrade.state not in (DegradeState.S0, DegradeState.S3)
        ctx.degrade_reason = self._degrade.last_reason if ctx.degraded else None

        if req.merchant_scope_id and not params.merchant_scope:
            params.merchant_scope = await self._scope.resolve(req.merchant_scope_id)

        # ── Stage 0.5: 商家规模判定 + bitmap 预查 (与特征提取并行) ──
        use_brute_force = False
        prefetch_task = None
        if params.merchant_scope:
            t = ctx.timer_start("merchant_count")
            image_count = await self._bitmap.get_merchant_image_count(params.merchant_scope)
            ctx.timer_end("merchant_count", t)

            brute_force_limit = getattr(settings, 'adaptive_search', None)
            limit = brute_force_limit.brute_force_pk_limit if brute_force_limit else 1_000_000

            if image_count <= limit:
                use_brute_force = True
                ctx.search_strategy = SearchStrategy.BRUTE_FORCE
                # 并行预查 pk 列表
                prefetch_task = asyncio.create_task(
                    self._bitmap.get_merchant_image_pks(params.merchant_scope)
                )
            else:
                ctx.search_strategy = SearchStrategy.HNSW_FILTERED
                # 大范围: 并行预构建 bitmap
                if hasattr(self._bitmap, '_bitmap_builder'):
                    prefetch_task = asyncio.create_task(
                        self._bitmap._bitmap_builder.build(params.merchant_scope)
                    )

        # ── Stage 1: 特征提取 (与 bitmap 预查并行) ──
        t = ctx.timer_start("feature")
        features = await self._extract_features(req.query_image)
        ctx.timer_end("feature", t)

        # ── 分路执行 ──
        if use_brute_force:
            all_candidates = await self._execute_brute_force_path(
                ctx, features, params, prefetch_task
            )
        else:
            all_candidates = await self._execute_ann_path(
                ctx, features, params, prefetch_task
            )

        # ── Stage 4: 子图并行搜索 (v1.4: 必备路径, 非 fallback) ──
        if features.sub_vecs and settings.feature_flags.enable_sub_image_search:
            t = ctx.timer_start("sub_image")
            sub_candidates = await self._search_sub_images(features)
            ctx.timer_end("sub_image", t)
            all_candidates = self._merge_with_sub(all_candidates, sub_candidates)

        # ── Stage 4b: Refine 精排 ──
        if settings.feature_flags.enable_refine and all_candidates:
            t = ctx.timer_start("refine")
            all_candidates = await self._refiner.rerank(
                query_vec=features.global_vec,
                candidates=all_candidates[: params.refine_top_k],
            )
            ctx.timer_end("refine", t)

        # ── Stage 5: 匹配层级分类 + SPU 聚合 ──
        all_candidates = self._classify_match_level(all_candidates)
        all_candidates = self._aggregate_by_spu(all_candidates)

        # ── 终极兜底 ──
        if not all_candidates:
            all_candidates = await self._ultimate_fallback(ctx, features, params)

        # ── Stage 6: 响应构建 ──
        response = await self._build_response(ctx, all_candidates, params, features)

        asyncio.create_task(self._logger.emit(ctx, params, features, response))

        METRICS.search_total.labels(
            strategy=ctx.strategy.value,
            confidence=response.meta.confidence.value,
        ).inc()

        return response

    # ════════════════════════════════════════════════════════════
    # 小范围路径: bitmap 预查 pk → Milvus 取向量 → 暴力搜索
    # ════════════════════════════════════════════════════════════

    async def _execute_brute_force_path(
        self,
        ctx: PipelineContext,
        features: FeatureResult,
        params: EffectiveParams,
        prefetch_task: Optional[asyncio.Task],
    ) -> List[Candidate]:
        """小商家路径: 精度 100%, 延迟 < 10ms"""
        # 等待 pk 列表预查完成
        image_pks: List[str] = []
        if prefetch_task:
            t = ctx.timer_start("bitmap_prefetch")
            try:
                image_pks = await prefetch_task
            except Exception as e:
                logger.error("bitmap_prefetch_failed", error=str(e))
            ctx.timer_end("bitmap_prefetch", t)

        if not image_pks:
            return []

        # Milvus 按 pk 批量取向量
        t = ctx.timer_start("query_by_pk")
        records = await self._ann.query_by_pks(image_pks)
        ctx.timer_end("query_by_pk", t)

        if not records:
            return []

        # 进程内暴力计算 cosine distance
        t = ctx.timer_start("brute_force")
        candidates = self._brute_force_cosine(features.global_vec, records)
        ctx.timer_end("brute_force", t)

        return candidates

    @staticmethod
    def _brute_force_cosine(query_vec: List[float], records: List[dict]) -> List[Candidate]:
        """进程内暴力计算 cosine similarity"""
        q = np.array(query_vec, dtype=np.float32)
        q_norm = q / (np.linalg.norm(q) + 1e-10)

        candidates = []
        for r in records:
            vec = r.get("global_vec")
            if vec is None:
                continue
            v = np.array(vec, dtype=np.float32)
            v_norm = v / (np.linalg.norm(v) + 1e-10)
            score = float(np.dot(q_norm, v_norm))
            candidates.append(Candidate(
                image_pk=r.get("image_pk", ""),
                score=score,
                product_id=r.get("product_id"),
                is_evergreen=r.get("is_evergreen", False),
                category_l1=r.get("category_l1"),
                category_l2=r.get("category_l2"),
                tags=r.get("tags"),
                source="brute_force",
            ))

        candidates.sort(key=lambda x: x.score, reverse=True)
        return candidates

    # ════════════════════════════════════════════════════════════
    # 大范围路径: ANN + bitmap 后过滤
    # ════════════════════════════════════════════════════════════

    async def _execute_ann_path(
        self,
        ctx: PipelineContext,
        features: FeatureResult,
        params: EffectiveParams,
        prefetch_task: Optional[asyncio.Task],
    ) -> List[Candidate]:
        """大范围路径: ANN 粗召回 + bitmap 后过滤"""

        coarse_top_k = settings.search.ann.coarse_top_k
        if params.merchant_scope:
            coarse_top_k = int(coarse_top_k * self.MERCHANT_SCOPE_TOP_K_MULTIPLIER)

        category_filter = self._build_category_prefilter(features)

        # 标签召回 + 热区 ANN 并行
        tag_recall_task = None
        if settings.feature_flags.enable_tag_recall_stage and features.tags_pred:
            tag_recall_task = asyncio.create_task(
                self._tag_recall(features, params)
            )

        t = ctx.timer_start("ann_hot")
        hot_candidates = await self._search_hot_zone(features, params, coarse_top_k, category_filter)
        ctx.timer_end("ann_hot", t)

        tag_recall_candidates: List[Candidate] = []
        if tag_recall_task is not None:
            t_tag = ctx.timer_start("tag_recall")
            try:
                tag_recall_candidates = await tag_recall_task
            except Exception as e:
                logger.warning("tag_recall_parallel_error", error=str(e))
            ctx.timer_end("tag_recall", t_tag)

        # 级联判定
        top1_score = hot_candidates[0].score if hot_candidates else 0.0
        cascade_trigger = settings.search.dual_path.cascade_trigger
        non_hot_candidates: List[Candidate] = []

        if (
            top1_score < cascade_trigger
            and params.enable_cascade
            and not ctx.degraded
            and params.time_range == TimeRange.ALL
            and params.data_scope in (DataScope.ALL, DataScope.ROLLING)
        ):
            t = ctx.timer_start("ann_non_hot")
            non_hot_candidates = await self._search_non_hot_zone(features, params, coarse_top_k)
            ctx.timer_end("ann_non_hot", t)
            ctx.strategy = Strategy.CASCADE_PATH
            ctx.zone_hit = "hot+non_hot"
            METRICS.cascade_triggered_total.inc()

        all_candidates = self._merge_candidates(hot_candidates, non_hot_candidates, tag_recall_candidates)

        # bitmap 后过滤
        if params.merchant_scope:
            if prefetch_task is not None:
                try:
                    await prefetch_task
                except Exception:
                    pass

            t = ctx.timer_start("filter")
            all_candidates, filter_skipped = await self._bitmap.filter_with_degrade(
                candidate_pks=[c.image_pk for c in all_candidates],
                merchant_scope=params.merchant_scope,
                candidates=all_candidates,
            )
            ctx.filter_skipped = filter_skipped
            ctx.timer_end("filter", t)

            if filter_skipped:
                self._bitmap_skip_consecutive += 1
                METRICS.bitmap_filter_fallback.labels(level="skip", reason="all_levels_failed").inc()
                if self._bitmap_skip_consecutive >= self.BITMAP_SKIP_FORCE_DEGRADE_THRESHOLD:
                    current_state = self._degrade.state
                    if current_state in (DegradeState.S0, DegradeState.S1):
                        self._degrade.force_state(
                            DegradeState.S2,
                            reason=f"bitmap_filter_skipped_{self._bitmap_skip_consecutive}_consecutive",
                        )
                        logger.error("bitmap_skip_force_degrade", consecutive=self._bitmap_skip_consecutive)
            else:
                self._bitmap_skip_consecutive = 0

        return all_candidates

    # ════════════════════════════════════════════════════════════
    # 子图并行搜索 (v1.4: 必备路径)
    # ════════════════════════════════════════════════════════════

    async def _search_sub_images(self, features: FeatureResult) -> List[Candidate]:
        """v1.4: 子图搜索为必备并行路径, 每次搜索都执行"""
        try:
            return await asyncio.wait_for(
                self._ann.search_sub(
                    features.sub_vecs,
                    top_k=settings.search.fallback.sub_image_top_k,
                ),
                timeout=0.1,  # 100ms 超时
            )
        except Exception as e:
            logger.warning("sub_image_search_failed", error=str(e))
            return []

    @staticmethod
    def _merge_with_sub(main: List[Candidate], sub: List[Candidate]) -> List[Candidate]:
        """合并主结果和子图结果, 按 image_pk 去重取最高分"""
        seen: Dict[str, Candidate] = {}
        for c in main:
            if c.image_pk not in seen or c.score > seen[c.image_pk].score:
                seen[c.image_pk] = c
        for c in sub:
            c.source = "sub_image"
            if c.image_pk not in seen or c.score > seen[c.image_pk].score:
                seen[c.image_pk] = c
        result = list(seen.values())
        result.sort(key=lambda x: x.score, reverse=True)
        return result

    # ════════════════════════════════════════════════════════════
    # 匹配层级 + SPU 聚合 (v1.4 新增)
    # ════════════════════════════════════════════════════════════

    @staticmethod
    def _classify_match_level(candidates: List[Candidate]) -> List[Candidate]:
        """v1.4: 按相似度分数分类匹配层级 P0>P1>P2"""
        try:
            p0_th = settings.match_level.p0_threshold
            p1_th = settings.match_level.p1_threshold
        except AttributeError:
            p0_th, p1_th = 0.90, 0.70

        for c in candidates:
            if c.score >= p0_th:
                c._match_level = MatchLevel.P0
            elif c.score >= p1_th:
                c._match_level = MatchLevel.P1
            else:
                c._match_level = MatchLevel.P2
        return candidates

    @staticmethod
    def _aggregate_by_spu(candidates: List[Candidate]) -> List[Candidate]:
        """v1.4: 按 product_id (SPU) 聚合, 同商品取最高分图片"""
        if not candidates:
            return candidates

        seen_products: Dict[str, Candidate] = {}
        no_product: List[Candidate] = []

        for c in candidates:
            if not c.product_id:
                no_product.append(c)
                continue
            if c.product_id not in seen_products or c.score > seen_products[c.product_id].score:
                seen_products[c.product_id] = c

        # P0 优先, 然后 P1, 然后 P2; 同层级按分数降序
        result = list(seen_products.values()) + no_product
        level_order = {MatchLevel.P0: 0, MatchLevel.P1: 1, MatchLevel.P2: 2}
        result.sort(key=lambda c: (level_order.get(getattr(c, '_match_level', MatchLevel.P2), 2), -c.score))
        return result

    # ════════════════════════════════════════════════════════════
    # 内部方法 (保留原有)
    # ════════════════════════════════════════════════════════════

    async def _extract_features(self, query_image_b64: str) -> FeatureResult:
        if self._feature._use_remote:
            return await asyncio.wait_for(
                self._feature.extract_query(query_image_b64),
                timeout=30.0,
            )
        try:
            return await asyncio.wait_for(
                self._feature.extract_query(query_image_b64, device="gpu"),
                timeout=0.030,
            )
        except (asyncio.TimeoutError, Exception) as e:
            METRICS.gpu_fallback_total.inc()
            METRICS.error_code_total.labels(code="gpu_fallback", source="gpu").inc()
            logger.warning("gpu_fallback", error=str(e))
            return await self._feature.extract_query(query_image_b64, device="cpu")

    async def _tag_recall(self, features: FeatureResult, params: EffectiveParams) -> List[Candidate]:
        top_tags = features.tags_pred[: settings.search.tag_recall.idf_top_k]
        if not top_tags:
            return []
        timeout_s = settings.search.tag_recall.timeout_ms / 1000
        try:
            return await asyncio.wait_for(
                self._ann.search_by_tags_inverted(
                    tags=top_tags,
                    top_k=settings.search.tag_recall.inverted_top_k,
                    partition_filter=self._build_partition_filter(params),
                ),
                timeout=timeout_s,
            )
        except asyncio.TimeoutError:
            METRICS.tag_recall_timeout_total.inc()
            return []

    async def _search_hot_zone(self, features, params, coarse_top_k=0, category_filter=""):
        from app.core.circuit_breaker import CircuitBreakerOpenError
        effective_top_k = coarse_top_k or settings.search.ann.coarse_top_k
        timeout_s = settings.search.ann.hot_timeout_ms / 1000
        partition_filter = self._build_hot_partition_filter(params)
        if category_filter:
            partition_filter = f"({partition_filter}) and ({category_filter})"
        try:
            return await asyncio.wait_for(
                self._ann.search_hot(
                    vector=features.global_vec,
                    partition_filter=partition_filter,
                    ef_search=params.ef_search,
                    top_k=effective_top_k,
                ),
                timeout=timeout_s,
            )
        except (CircuitBreakerOpenError, asyncio.TimeoutError):
            METRICS.ann_timeout_total.labels(zone="hot").inc()
            return []

    async def _search_non_hot_zone(self, features, params, coarse_top_k=0):
        from app.core.circuit_breaker import CircuitBreakerOpenError
        effective_top_k = coarse_top_k or settings.search.ann.coarse_top_k
        timeout_s = settings.search.dual_path.cascade_timeout_ms / 1000
        try:
            return await asyncio.wait_for(
                self._ann.search_non_hot(
                    vector=features.global_vec,
                    partition_filter=self._build_non_hot_partition_filter(params),
                    search_list_size=settings.non_hot_zone.search_list_size,
                    top_k=effective_top_k,
                ),
                timeout=timeout_s,
            )
        except (CircuitBreakerOpenError, asyncio.TimeoutError):
            METRICS.ann_timeout_total.labels(zone="non_hot").inc()
            return []

    async def _ultimate_fallback(self, ctx, features, params):
        METRICS.error_code_total.labels(code="empty_result_fallback", source="internal").inc()
        logger.warning("empty_result_ultimate_fallback", request_id=ctx.request_id)
        try:
            return await asyncio.wait_for(
                self._ann.search_hot(
                    vector=features.global_vec,
                    partition_filter="is_evergreen == true",
                    ef_search=96,
                    top_k=min(params.top_k, 50),
                ),
                timeout=0.3,
            )
        except Exception:
            return []

    def _merge_candidates(self, hot, non_hot, tag_recall):
        seen: Dict[str, Candidate] = {}
        for c in hot:
            if c.image_pk not in seen or c.score > seen[c.image_pk].score:
                seen[c.image_pk] = c
        for c in non_hot:
            c.source = "non_hot_ann"
            if c.image_pk not in seen or c.score > seen[c.image_pk].score:
                seen[c.image_pk] = c
        for c in tag_recall:
            c.source = "tag_recall"
            if c.image_pk not in seen:
                seen[c.image_pk] = c
        result = list(seen.values())
        result.sort(key=lambda x: x.score, reverse=True)
        return result

    @staticmethod
    def _build_category_prefilter(features: FeatureResult) -> str:
        cat_pred = getattr(features, "category_l1_pred", None)
        cat_conf = getattr(features, "category_l1_conf", 0.0)
        if cat_pred is not None and cat_conf >= 0.85:
            return f"category_l1 == {cat_pred}"
        return ""

    # ── 分区过滤表达式 ──

    def _build_partition_filter(self, params: EffectiveParams) -> str:
        from app.core.utils import current_yyyymm, month_subtract
        now = current_yyyymm()
        if params.data_scope == DataScope.EVERGREEN:
            return "is_evergreen == true"
        if params.time_range == TimeRange.HOT_PLUS_EVERGREEN:
            hot_start = month_subtract(now, settings.hot_zone.months)
            return f"(ts_month >= {hot_start}) or (is_evergreen == true)"
        if params.time_range == TimeRange.HOT_ONLY:
            hot_start = month_subtract(now, settings.hot_zone.months)
            if params.data_scope == DataScope.ROLLING:
                return f"ts_month >= {hot_start} and is_evergreen == false"
            return f"(ts_month >= {hot_start}) or (is_evergreen == true)"
        start = month_subtract(now, settings.non_hot_zone.months_end)
        if params.data_scope == DataScope.ROLLING:
            return f"ts_month >= {start} and is_evergreen == false"
        return f"(ts_month >= {start}) or (is_evergreen == true)"

    def _build_hot_partition_filter(self, params: EffectiveParams) -> str:
        from app.core.utils import current_yyyymm, month_subtract
        now = current_yyyymm()
        hot_start = month_subtract(now, settings.hot_zone.months)
        base = f"ts_month >= {hot_start}"
        if params.data_scope == DataScope.EVERGREEN:
            return "is_evergreen == true"
        if params.data_scope == DataScope.ROLLING:
            return f"{base} and is_evergreen == false"
        return f"({base}) or (is_evergreen == true)"

    def _build_non_hot_partition_filter(self, params: EffectiveParams) -> str:
        from app.core.utils import current_yyyymm, month_subtract
        now = current_yyyymm()
        hot_start = month_subtract(now, settings.hot_zone.months)
        non_hot_start = month_subtract(now, settings.non_hot_zone.months_end)
        return f"ts_month >= {non_hot_start} and ts_month < {hot_start} and is_evergreen == false"

    # ── URI 查询 ──

    async def _batch_fetch_uris(self, image_pks: List[str]) -> Dict[str, str]:
        if not image_pks or self._pg_pool is None:
            return {}
        try:
            async with self._pg_pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT image_pk, uri FROM uri_dedup WHERE image_pk = ANY($1::char(32)[])",
                    image_pks,
                )
            return {row["image_pk"].strip(): row["uri"] for row in rows if row["uri"]}
        except Exception as e:
            logger.warning("batch_fetch_uris_failed", error=str(e))
            return {}

    # ── 响应构建 ──

    async def _build_response(self, ctx, candidates, params, features) -> SearchResponse:
        top_k = min(params.top_k, len(candidates))
        final_candidates = candidates[:top_k]

        uri_map = await self._batch_fetch_uris([c.image_pk for c in final_candidates])

        results = []
        for i, c in enumerate(final_candidates):
            results.append(
                SearchResultItem(
                    image_id=c.image_pk,
                    score=round(c.score, 4),
                    image_url=uri_map.get(c.image_pk),
                    product_id=c.product_id,
                    position=i + 1,
                    is_evergreen=c.is_evergreen,
                    category_l1=self._vocab.decode("category_l1", c.category_l1)
                    if c.category_l1 is not None else None,
                    tags=[s for t in (c.tags or []) if t is not None
                          for s in [self._vocab.decode("tag", t)] if s is not None],
                    match_level=getattr(c, '_match_level', None),
                )
            )

        top1_score = candidates[0].score if candidates else 0.0
        confidence = self._compute_confidence(top1_score, len(results))
        scope_desc = self._build_scope_desc(params, ctx)

        meta = SearchMeta(
            request_id=ctx.request_id,
            total_results=len(results),
            strategy=ctx.strategy,
            confidence=confidence,
            degraded=ctx.degraded,
            filter_skipped=ctx.filter_skipped,
            degrade_state=ctx.degrade_state,
            degrade_reason=ctx.degrade_reason,
            search_scope_desc=scope_desc,
            latency_ms=ctx.total_ms,
            zone_hit=ctx.zone_hit,
            search_strategy=ctx.search_strategy,
            effective_params=EffectiveParamsSnapshot(
                ef_search=params.ef_search,
                search_list_size=getattr(params, "search_list_size", None),
                refine_top_k=params.refine_top_k,
                time_range=params.time_range.value if params.time_range else None,
                enable_cascade=params.enable_cascade,
                enable_sub_image=True,
                data_scope=params.data_scope.value if params.data_scope else None,
            ),
            feature_ms=ctx.timings.get("feature"),
            ann_hot_ms=ctx.timings.get("ann_hot"),
            ann_non_hot_ms=ctx.timings.get("ann_non_hot"),
            tag_recall_ms=ctx.timings.get("tag_recall"),
            filter_ms=ctx.timings.get("filter"),
            refine_ms=ctx.timings.get("refine"),
        )

        return SearchResponse(results=results, meta=meta)

    @staticmethod
    def _compute_confidence(top1_score: float, count: int) -> Confidence:
        cfg = settings.search.confidence
        if top1_score >= cfg.high_score and count >= cfg.high_min_count:
            return Confidence.HIGH
        elif top1_score >= cfg.medium_score:
            return Confidence.MEDIUM
        return Confidence.LOW

    @staticmethod
    def _build_scope_desc(params: EffectiveParams, ctx: PipelineContext) -> str:
        if ctx.filter_skipped:
            return "搜索范围已扩大，结果可能包含其他商家商品"
        if ctx.degraded:
            return "当前搜索范围已缩小（热区 + 经典款）"
        range_desc = {
            TimeRange.HOT_ONLY: "近6个月",
            TimeRange.ALL: "近18个月 + 经典款",
            TimeRange.HOT_PLUS_EVERGREEN: "热区 + 经典款",
        }.get(params.time_range, "全部")
        if params.data_scope == DataScope.EVERGREEN:
            return "经典款商品"
        if params.merchant_scope:
            return f"在 {len(params.merchant_scope)} 个商家范围内搜索（{range_desc}）"
        return f"全平台商品（{range_desc}）"
