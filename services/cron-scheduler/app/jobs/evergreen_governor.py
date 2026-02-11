"""
常青池水位治理 — 每日 06:00
绿区 <2.5亿 | 黄区 2.5-2.8亿(清理>5年) | 红区 >3亿(紧急清理>2年)
"""
from __future__ import annotations

import asyncio
import time

import structlog

logger = structlog.get_logger(__name__)

GREEN = 250_000_000
YELLOW = 280_000_000
RED = 300_000_000


class EvergreenPoolGovernor:
    def __init__(self, deps: dict):
        self._pg = deps["pg"]
        self._milvus = deps["milvus"]

    async def daily_check(self):
        loop = asyncio.get_event_loop()
        from pymilvus import Collection
        coll = Collection("global_images_hot")

        # 查询常青总量
        result = await loop.run_in_executor(
            None,
            lambda: coll.query(
                expr="is_evergreen == true",
                output_fields=["count(*)"],
            ),
        )
        count = result[0]["count(*)"] if result else 0
        logger.info("evergreen_pool_check", count=count)

        if count < GREEN:
            logger.info("evergreen_pool_green", count=count)
            return

        if count >= RED:
            logger.critical("evergreen_pool_RED", count=count, threshold=RED)
            await self._cleanup(coll, loop, target=GREEN, min_age_years=2)
        elif count >= YELLOW:
            logger.warning("evergreen_pool_YELLOW", count=count, threshold=YELLOW)
            await self._cleanup(coll, loop, target=GREEN, min_age_years=5)

    async def _cleanup(self, coll, loop, target: int, min_age_years: int):
        cutoff_ts = int((time.time() - min_age_years * 365 * 86400) * 1000)
        total_deleted = 0

        while True:
            candidates = await loop.run_in_executor(
                None,
                lambda: coll.query(
                    expr=f"is_evergreen == true and promoted_at > 0 and promoted_at < {cutoff_ts}",
                    output_fields=["image_pk"],
                    limit=1000,
                ),
            )
            if not candidates:
                break

            pks = [r["image_pk"] for r in candidates]
            try:
                # Milvus 删除
                await loop.run_in_executor(
                    None,
                    lambda: coll.delete(expr=f'image_pk in {pks}'),
                )
                # PG bitmap 删除
                async with self._pg.acquire() as conn:
                    await conn.execute(
                        "DELETE FROM image_merchant_bitmaps WHERE image_pk = ANY($1::char(32)[])",
                        pks,
                    )
                total_deleted += len(pks)
                logger.info("evergreen_cleanup_batch", deleted=len(pks), total=total_deleted)
            except Exception as e:
                logger.error("evergreen_cleanup_error", error=str(e))
                break

        logger.info("evergreen_cleanup_done", total_deleted=total_deleted)
