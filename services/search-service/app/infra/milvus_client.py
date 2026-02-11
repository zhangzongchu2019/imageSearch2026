"""
Milvus 向量检索客户端 — 两区架构 (热区 HNSW + 非热区 DiskANN)
v1.4 加固:
  - FIX #2: 使用有界线程池, 不再用默认 ThreadPoolExecutor
  - P0-A: 熔断器保护 — 连续 5 次失败 → OPEN, 30s 后 HALF_OPEN 探测
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import structlog
from pymilvus import Collection

from app.core.config import get_settings
from app.core.circuit_breaker import (
    BreakerConfig,
    CircuitBreakerOpenError,
    get_breaker,
)
from app.core.pipeline import Candidate

logger = structlog.get_logger(__name__)
settings = get_settings()

# 熔断器配置: 连续 5 次失败 → OPEN, 30s reset
_MILVUS_BREAKER_CFG = BreakerConfig(fail_max=5, reset_timeout_s=30.0)


class MilvusSearchClient:
    """Milvus 两区检索封装 — 使用注入的有界线程池 + 熔断器"""

    def __init__(self, connections, executor: Optional[ThreadPoolExecutor] = None):
        self._conn = connections
        self._hot = Collection(settings.milvus.hot_collection)
        self._non_hot = Collection(settings.milvus.non_hot_collection)
        self._sub = Collection(settings.milvus.sub_collection)

        # FIX #2: 使用外部注入的有界线程池, 不再使用 None (默认无限池)
        self._executor = executor

        # P0-A: 各区独立熔断器
        self._breaker_hot = get_breaker("milvus_hot", _MILVUS_BREAKER_CFG)
        self._breaker_non_hot = get_breaker("milvus_non_hot", _MILVUS_BREAKER_CFG)
        self._breaker_sub = get_breaker("milvus_sub", _MILVUS_BREAKER_CFG)

        self._hot.load()

    async def search_hot(
        self,
        vector: List[float],
        partition_filter: str,
        ef_search: int,
        top_k: int,
    ) -> List[Candidate]:
        """热区 HNSW 检索 — 受熔断器保护"""
        search_params = {
            "metric_type": "COSINE",
            "params": {"ef": ef_search},
        }
        loop = asyncio.get_event_loop()

        async def _do_search():
            return await loop.run_in_executor(
                self._executor,
                lambda: self._hot.search(
                    data=[vector],
                    anns_field="global_vec",
                    param=search_params,
                    limit=top_k,
                    expr=partition_filter,
                    output_fields=[
                        "image_pk", "product_id", "is_evergreen",
                        "category_l1", "category_l2", "tags",
                    ],
                ),
            )

        results = await self._breaker_hot.call_async(_do_search())
        return self._parse_results(results[0], source="hot_ann")

    async def search_non_hot(
        self,
        vector: List[float],
        partition_filter: str,
        search_list_size: int,
        top_k: int,
    ) -> List[Candidate]:
        """非热区 DiskANN 检索 — 受熔断器保护"""
        search_params = {
            "metric_type": "COSINE",
            "params": {"search_list": search_list_size},
        }
        loop = asyncio.get_event_loop()

        async def _do_search():
            return await loop.run_in_executor(
                self._executor,
                lambda: self._non_hot.search(
                    data=[vector],
                    anns_field="global_vec",
                    param=search_params,
                    limit=top_k,
                    expr=partition_filter,
                    output_fields=[
                        "image_pk", "product_id", "is_evergreen",
                        "category_l1", "category_l2", "tags",
                    ],
                ),
            )

        results = await self._breaker_non_hot.call_async(_do_search())
        return self._parse_results(results[0], source="non_hot_ann")

    async def search_by_tags_inverted(
        self,
        tags: List[int],
        top_k: int,
        partition_filter: str,
    ) -> List[Candidate]:
        """Stage 1.5: INVERTED 索引标签召回 — 共用 hot breaker"""
        tag_expr_parts = [f"array_contains(tags, {t})" for t in tags]
        tag_expr = " or ".join(tag_expr_parts)
        full_expr = f"({partition_filter}) and ({tag_expr})"

        loop = asyncio.get_event_loop()

        async def _do_query():
            return await loop.run_in_executor(
                self._executor,
                lambda: self._hot.query(
                    expr=full_expr,
                    output_fields=["image_pk", "product_id", "is_evergreen", "category_l1", "tags"],
                    limit=top_k,
                ),
            )

        results = await self._breaker_hot.call_async(_do_query())
        return [
            Candidate(
                image_pk=r["image_pk"],
                score=0.5,
                product_id=r.get("product_id"),
                is_evergreen=r.get("is_evergreen", False),
                category_l1=r.get("category_l1"),
                tags=r.get("tags"),
                source="tag_recall",
            )
            for r in results
        ]

    async def search_sub(
        self, sub_vecs: List[List[float]], top_k: int
    ) -> List[Candidate]:
        """子图检索 — 受熔断器保护"""
        search_params = {
            "metric_type": "COSINE",
            "params": {"nprobe": 64},
        }
        all_candidates = []
        loop = asyncio.get_event_loop()
        for vec in sub_vecs[:5]:
            async def _do_search(v=vec):
                return await loop.run_in_executor(
                    self._executor,
                    lambda: self._sub.search(
                        data=[v],
                        anns_field="sub_vec",
                        param=search_params,
                        limit=top_k,
                        output_fields=["image_pk"],
                    ),
                )

            try:
                results = await self._breaker_sub.call_async(_do_search())
                for hit in results[0]:
                    all_candidates.append(
                        Candidate(
                            image_pk=hit.entity.get("image_pk"),
                            score=hit.score,
                            source="sub_image",
                        )
                    )
            except CircuitBreakerOpenError:
                logger.warning("sub_search_breaker_open")
                break

        return all_candidates

    async def search_by_tags(
        self, tags: List[int], top_k: int
    ) -> List[Candidate]:
        """标签召回 (Fallback 路径)"""
        return await self.search_by_tags_inverted(tags, top_k, "ts_month >= 0")

    @staticmethod
    def _parse_results(hits, source: str) -> List[Candidate]:
        candidates = []
        for hit in hits:
            candidates.append(
                Candidate(
                    image_pk=hit.entity.get("image_pk", hit.id),
                    score=hit.score,
                    product_id=hit.entity.get("product_id"),
                    is_evergreen=hit.entity.get("is_evergreen", False),
                    category_l1=hit.entity.get("category_l1"),
                    tags=hit.entity.get("tags"),
                    source=source,
                )
            )
        return candidates
