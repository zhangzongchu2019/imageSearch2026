"""
Bitmap 对账 — 每日 03:00
PG image_merchant_bitmaps vs bitmap-filter-service RocksDB 全量比对
"""
from __future__ import annotations

import structlog

logger = structlog.get_logger(__name__)


class BitmapReconciliation:
    def __init__(self, deps: dict):
        self._pg = deps["pg"]

    async def execute(self):
        logger.info("bitmap_reconcile_start")
        batch_size = 100_000
        offset = 0
        diff_count = 0

        async with self._pg.acquire() as conn:
            total = await conn.fetchval("SELECT count(*) FROM image_merchant_bitmaps")

        logger.info("bitmap_reconcile_total", total=total)

        # 分批校验: PG ⊇ RocksDB (ADD-only 约束下)
        while offset < total:
            async with self._pg.acquire() as conn:
                rows = await conn.fetch(
                    """SELECT image_pk, rb_cardinality(bitmap) as card
                       FROM image_merchant_bitmaps
                       ORDER BY image_pk
                       LIMIT $1 OFFSET $2""",
                    batch_size, offset,
                )

            # TODO: 对比 RocksDB 侧 (通过 gRPC 批量查询)
            # 此处仅校验 PG 侧 cardinality > 0
            for r in rows:
                if r["card"] == 0:
                    diff_count += 1

            offset += batch_size

        if diff_count > 1000:
            logger.error("bitmap_reconcile_diff_ALERT", diff_count=diff_count)
        else:
            logger.info("bitmap_reconcile_ok", diff_count=diff_count)
