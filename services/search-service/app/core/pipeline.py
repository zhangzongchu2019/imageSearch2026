"""
级联检索流水线 — v1.2 两区双路径架构
Stage 0   : 降级覆盖 + merchant_scope 解析
Stage 1   : 特征提取 (TensorRT FP16, GPU→CPU fallback)
Stage 1.5 : 全量标签召回 (INVERTED, IDF Top-5, 10ms 熔断)
Stage 2-H : 热区 HNSW ANN (M=24, ef=192)
Stage 2-NH: [级联] 非热区 DiskANN (MD=64, SL=200)
Stage 3   : 商家过滤 (Roaring Bitmap, 外部 gRPC)
Stage 4   : Refine 精排 (float32, 2000 候选)
Stage 5-7 : [条件] Fallback 子图+标签多路召回
Stage 8   : 响应构建 + 异步日志
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import mmh3
import structlog

from app.core.config import get_settings
from app.core.degrade_fsm import DegradeStateMachine
from app.core.metrics import METRICS
from app.model.schemas import (
    Confidence,
    DataScope,
    DegradeState,
    EffectiveParams,
    SearchMeta,
    SearchRequest,
    SearchResponse,
    SearchResultItem,
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
    degraded: bool = False
    filter_skipped: bool = False
    strategy: Strategy = Strategy.FAST_PATH
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
    source: str = "hot_ann"  # hot_ann | non_hot_ann | tag_recall | sub_image


from app.engine.feature_extractor import FeatureResult


# ── 核心流水线 ──

class SearchPipeline:
    """级联检索流水线 — 所有 Stage 均可独立降级

    v1.2 关键变更:
    - 两区架构: 热区 HNSW + 非热区 DiskANN
    - 双路径: 快路径 ≤240ms (80%) + 级联路径 ≤400ms (20%)
    - Stage 1.5: 全量标签召回 (IDF Top-5, 10ms 熔断)
    - Refine 扩池: 500→2000
    """

    def __init__(
        self,
        degrade_fsm: DegradeStateMachine,
        feature_extractor,  # FeatureExtractor
        ann_searcher,       # ANNSearcher
        bitmap_filter,      # BitmapFilterClient
        refiner,            # Refiner
        ranker,             # FusionRanker
        scope_resolver,     # ScopeResolver
        vocab_cache,        # VocabCache
        search_logger,      # SearchLogEmitter
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

    async def execute(self, req: SearchRequest, request_id: str) -> SearchResponse:
        ctx = PipelineContext(request_id=request_id)

        # ── Stage 0: 降级状态覆盖 + Scope 解析 ──
        params = self._degrade.apply(req)
        ctx.degrade_state = self._degrade.state
        ctx.degraded = self._degrade.state not in (DegradeState.S0, DegradeState.S3)

        if req.merchant_scope_id and not params.merchant_scope:
            params.merchant_scope = await self._scope.resolve(req.merchant_scope_id)

        # ── Stage 1: 特征提取 (GPU → CPU fallback) ──
        t = ctx.timer_start("feature")
        features = await self._extract_features(req.query_image)
        ctx.timer_end("feature", t)

        # ── Stage 1.5: 全量标签召回 (v4.0 新增) ──
        tag_recall_candidates: List[Candidate] = []
        if settings.feature_flags.enable_tag_recall_stage and features.tags_pred:
            t = ctx.timer_start("tag_recall")
            tag_recall_candidates = await self._tag_recall(features, params)
            ctx.timer_end("tag_recall", t)

        # ── Stage 2-H: 热区 HNSW ANN 检索 ──
        t = ctx.timer_start("ann_hot")
        hot_candidates = await self._search_hot_zone(features, params)
        ctx.timer_end("ann_hot", t)

        # ── 级联判定: Top1 score < cascade_trigger ──
        top1_score = hot_candidates[0].score if hot_candidates else 0.0
        cascade_trigger = settings.search.dual_path.cascade_trigger
        non_hot_candidates: List[Candidate] = []

        if (
            top1_score < cascade_trigger
            and params.enable_cascade
            and not ctx.degraded
            and params.time_range == TimeRange.ALL  # 仅 ALL 才触发非热区级联
            and params.data_scope in (DataScope.ALL, DataScope.ROLLING)
        ):
            # ── Stage 2-NH: 非热区 DiskANN 检索 (级联路径) ──
            t = ctx.timer_start("ann_non_hot")
            non_hot_candidates = await self._search_non_hot_zone(features, params)
            ctx.timer_end("ann_non_hot", t)
            ctx.strategy = Strategy.CASCADE_PATH
            ctx.zone_hit = "hot+non_hot"

            METRICS.cascade_triggered_total.inc()

        # ── 合并候选集 ──
        all_candidates = self._merge_candidates(
            hot_candidates, non_hot_candidates, tag_recall_candidates
        )

        # ── Stage 3: 商家过滤 ──
        if params.merchant_scope:
            t = ctx.timer_start("filter")
            all_candidates, filter_skipped = await self._bitmap.filter_with_degrade(
                candidate_pks=[c.image_pk for c in all_candidates],
                merchant_scope=params.merchant_scope,
                candidates=all_candidates,
            )
            ctx.filter_skipped = filter_skipped
            ctx.timer_end("filter", t)

        # ── Stage 4: Refine 精排 ──
        if settings.feature_flags.enable_refine and all_candidates:
            t = ctx.timer_start("refine")
            all_candidates = await self._refiner.rerank(
                query_vec=features.global_vec,
                candidates=all_candidates[: params.refine_top_k],
            )
            ctx.timer_end("refine", t)

        # ── Stage 5-7: 条件 Fallback (子图+标签多路召回) ──
        # v4.0: 基于 top1_score / confidence 触发, 不依赖 top_k
        top1_after_refine = all_candidates[0].score if all_candidates else 0.0
        if (
            params.enable_fallback
            and (
                not all_candidates
                or top1_after_refine < settings.search.fallback.score_threshold
            )
        ):
            fallback_results = await self._fallback(features, params)
            all_candidates = self._ranker.fuse(
                all_candidates, fallback_results, [], features
            )

        # ── Stage 8: 响应构建 ──
        response = self._build_response(ctx, all_candidates, params, features)

        # 异步写搜索日志
        asyncio.create_task(
            self._logger.emit(ctx, params, features, response)
        )

        METRICS.search_total.labels(
            strategy=ctx.strategy.value,
            confidence=response.meta.confidence.value,
        ).inc()

        return response

    # ── 内部方法 ──

    async def _extract_features(self, query_image_b64: str) -> FeatureResult:
        """GPU 推理, 30ms 超时后降级 CPU (10 QPS 限流)"""
        try:
            return await asyncio.wait_for(
                self._feature.extract_query(query_image_b64, device="gpu"),
                timeout=0.030,
            )
        except (asyncio.TimeoutError, Exception) as e:
            METRICS.gpu_fallback_total.inc()
            logger.warning("gpu_fallback", error=str(e))
            return await self._feature.extract_query(query_image_b64, device="cpu")

    async def _tag_recall(
        self, features: FeatureResult, params: EffectiveParams
    ) -> List[Candidate]:
        """Stage 1.5: 全量标签召回 — IDF Top-5 + 10ms 硬超时

        v4.0.1 Patch #4: 截断保护, 防止标签过多导致延迟爆炸
        """
        # IDF Top-5 截断: 只取区分度最高的 5 个标签
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
            logger.warning("tag_recall_timeout", tags_count=len(top_tags))
            return []

    async def _search_hot_zone(
        self, features: FeatureResult, params: EffectiveParams
    ) -> List[Candidate]:
        """热区 HNSW 检索 — M=24, ef=192, P99 ≤150ms"""
        timeout_s = settings.search.ann.hot_timeout_ms / 1000
        try:
            return await asyncio.wait_for(
                self._ann.search_hot(
                    vector=features.global_vec,
                    partition_filter=self._build_hot_partition_filter(params),
                    ef_search=params.ef_search,
                    top_k=settings.search.ann.coarse_top_k,
                ),
                timeout=timeout_s,
            )
        except asyncio.TimeoutError:
            METRICS.ann_timeout_total.labels(zone="hot").inc()
            logger.error("hot_zone_timeout")
            return []

    async def _search_non_hot_zone(
        self, features: FeatureResult, params: EffectiveParams
    ) -> List[Candidate]:
        """非热区 DiskANN 检索 — MD=64, SL=200, P99 ≤250ms"""
        timeout_s = settings.search.dual_path.cascade_timeout_ms / 1000
        try:
            return await asyncio.wait_for(
                self._ann.search_non_hot(
                    vector=features.global_vec,
                    partition_filter=self._build_non_hot_partition_filter(params),
                    search_list_size=settings.non_hot_zone.search_list_size,
                    top_k=settings.search.ann.coarse_top_k,
                ),
                timeout=timeout_s,
            )
        except asyncio.TimeoutError:
            METRICS.ann_timeout_total.labels(zone="non_hot").inc()
            logger.warning("non_hot_zone_timeout_cascade_skipped")
            return []

    async def _fallback(
        self, features: FeatureResult, params: EffectiveParams
    ) -> List[Candidate]:
        """子图 + 标签多路召回 Fallback"""
        tasks = []
        if features.sub_vecs and settings.feature_flags.enable_sub_image_search:
            tasks.append(
                self._ann.search_sub(
                    features.sub_vecs,
                    top_k=settings.search.fallback.sub_image_top_k,
                )
            )
        if features.tags_pred and settings.feature_flags.enable_tag_search:
            tasks.append(
                self._ann.search_by_tags(
                    features.tags_pred,
                    top_k=settings.search.fallback.tag_top_k,
                )
            )
        if not tasks:
            return []
        results = await asyncio.gather(*tasks, return_exceptions=True)
        merged = []
        for r in results:
            if isinstance(r, list):
                merged.extend(r)
        return merged

    def _merge_candidates(
        self,
        hot: List[Candidate],
        non_hot: List[Candidate],
        tag_recall: List[Candidate],
    ) -> List[Candidate]:
        """合并三路候选, 按 image_pk 去重取最高分"""
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

    # ── 分区过滤表达式构建 ──

    def _build_partition_filter(self, params: EffectiveParams) -> str:
        """通用分区过滤 — 根据 data_scope + time_range 构建 Milvus expr"""
        from app.core.utils import current_yyyymm, month_subtract
        now = current_yyyymm()

        if params.data_scope == DataScope.EVERGREEN:
            return "is_evergreen == true"

        # HOT_PLUS_EVERGREEN (降级专用): 热区 + 常青
        if params.time_range == TimeRange.HOT_PLUS_EVERGREEN:
            hot_start = month_subtract(now, settings.hot_zone.months)
            return f"(ts_month >= {hot_start}) or (is_evergreen == true)"

        # HOT_ONLY: 仅热区
        if params.time_range == TimeRange.HOT_ONLY:
            hot_start = month_subtract(now, settings.hot_zone.months)
            if params.data_scope == DataScope.ROLLING:
                return f"ts_month >= {hot_start} and is_evergreen == false"
            return f"(ts_month >= {hot_start}) or (is_evergreen == true)"

        # ALL (默认): 18 个月 + 常青
        start = month_subtract(now, settings.non_hot_zone.months_end)
        if params.data_scope == DataScope.ROLLING:
            return f"ts_month >= {start} and is_evergreen == false"
        return f"(ts_month >= {start}) or (is_evergreen == true)"

    def _build_hot_partition_filter(self, params: EffectiveParams) -> str:
        """热区分区过滤: ts_month >= hot_start"""
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
        """非热区分区过滤: non_hot_start ≤ ts_month < hot_start"""
        from app.core.utils import current_yyyymm, month_subtract
        now = current_yyyymm()
        hot_start = month_subtract(now, settings.hot_zone.months)
        non_hot_start = month_subtract(now, settings.non_hot_zone.months_end)
        return f"ts_month >= {non_hot_start} and ts_month < {hot_start} and is_evergreen == false"

    # ── 响应构建 ──

    def _build_response(
        self,
        ctx: PipelineContext,
        candidates: List[Candidate],
        params: EffectiveParams,
        features: FeatureResult,
    ) -> SearchResponse:
        top_k = min(params.top_k, len(candidates))
        results = []
        for i, c in enumerate(candidates[:top_k]):
            results.append(
                SearchResultItem(
                    image_id=c.image_pk,
                    score=round(c.score, 4),
                    product_id=c.product_id,
                    position=i + 1,
                    is_evergreen=c.is_evergreen,
                    category_l1=self._vocab.decode("category_l1", c.category_l1)
                    if c.category_l1 is not None
                    else None,
                    tags=[self._vocab.decode("tag", t) for t in (c.tags or [])],
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
            search_scope_desc=scope_desc,
            latency_ms=ctx.total_ms,
            zone_hit=ctx.zone_hit,
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

        # 范围描述
        range_desc = {
            TimeRange.HOT_ONLY: "近5个月",
            TimeRange.ALL: "近18个月 + 经典款",
            TimeRange.HOT_PLUS_EVERGREEN: "热区 + 经典款",
        }.get(params.time_range, "全部")

        if params.data_scope == DataScope.EVERGREEN:
            return "经典款商品"
        if params.merchant_scope:
            return f"在 {len(params.merchant_scope)} 个商家范围内搜索（{range_desc}）"
        return f"全平台商品（{range_desc}）"
