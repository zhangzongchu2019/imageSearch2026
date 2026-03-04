"""
Refine 精排 — float32 原始向量重排序
v4.1: 实现 float32 cosine 重排序 (替代 placeholder)
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import numpy as np
import structlog
from pymilvus import Collection

from app.core.config import get_settings
from app.core.pipeline import Candidate

logger = structlog.get_logger(__name__)
settings = get_settings()


class Refiner:
    """Refine 精排: 用 float32 原始向量重新计算精确余弦相似度

    SQ8 量化搜索后, 用 float32 补偿精度损失 (Recall +1-2%)
    P99 ≤5ms (2000 candidates × 256d cosine)
    """

    def __init__(self, milvus_client, executor: Optional[ThreadPoolExecutor] = None):
        self._milvus = milvus_client
        self._executor = executor
        self._collection = None  # lazy init on first rerank

    async def rerank(
        self, query_vec: List[float], candidates: List[Candidate]
    ) -> List[Candidate]:
        if not candidates:
            return candidates

        query = np.array(query_vec, dtype=np.float32)
        query_norm = np.linalg.norm(query)
        if query_norm == 0:
            return candidates

        query = query / query_norm

        # 批量获取候选的 float32 原始向量
        pks = [c.image_pk for c in candidates]

        loop = asyncio.get_event_loop()
        try:
            # lazy init Collection (避免在 mock 测试中失败)
            if self._collection is None:
                self._collection = Collection(settings.milvus.hot_collection)
            pk_list_str = ", ".join(f'"{pk}"' for pk in pks)
            expr = f"image_pk in [{pk_list_str}]"

            rows = await loop.run_in_executor(
                self._executor,
                lambda: self._collection.query(
                    expr=expr,
                    output_fields=["image_pk", "global_vec"],
                    limit=len(pks),
                ),
            )

            if not rows:
                candidates.sort(key=lambda c: c.score, reverse=True)
                return candidates

            # 构建向量矩阵并计算 cosine similarity
            pk_to_vec = {r["image_pk"]: np.array(r["global_vec"], dtype=np.float32) for r in rows}

            for c in candidates:
                vec = pk_to_vec.get(c.image_pk)
                if vec is not None:
                    vec_norm = np.linalg.norm(vec)
                    if vec_norm > 0:
                        c.score = float(np.dot(query, vec / vec_norm))

            candidates.sort(key=lambda c: c.score, reverse=True)
            return candidates

        except Exception as e:
            logger.warning("refiner_rerank_fallback", error=str(e))
            # 降级: 按原始 ANN score 排序
            candidates.sort(key=lambda c: c.score, reverse=True)
            return candidates
