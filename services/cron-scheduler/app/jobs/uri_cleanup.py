"""
URI 去重表清理 — 每月 1 日 01:00
DELETE 过期月份, SLO: ≤1h 完成
"""
from __future__ import annotations

import time

import structlog

logger = structlog.get_logger(__name__)


class UriDedupCleanup:
    def __init__(self, deps: dict):
        self._pg = deps["pg"]

    async def execute(self):
        from datetime import datetime
        now = datetime.utcnow()
        y, m = int(now.strftime("%Y")), int(now.strftime("%m"))
        total = y * 12 + (m - 1) - 18
        cutoff = (total // 12) * 100 + (total % 12) + 1

        logger.info("uri_dedup_cleanup_start", cutoff_month=cutoff)
        start = time.monotonic()

        async with self._pg.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM uri_dedup WHERE ts_month <= $1 AND ts_month != 999999",
                cutoff,
            )
            # 检查死元组
            dead = await conn.fetchval(
                "SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'uri_dedup'"
            )

        elapsed_s = time.monotonic() - start
        logger.info("uri_dedup_cleanup_done", result=result, elapsed_s=round(elapsed_s, 1))

        if dead and dead > 100_000_000:
            logger.warning("uri_dedup_vacuum_needed", dead_tuples=dead)
            async with self._pg.acquire() as conn:
                await conn.execute("VACUUM uri_dedup")
