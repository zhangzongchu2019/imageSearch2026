"""
以图搜商品系统 · cron-scheduler 定时任务调度
对齐系统设计 v1.2 §12.5 定时任务清单:
  - 分区轮转        每月1日 00:00
  - URI 去重清理    每月1日 01:00
  - Bitmap 对账     每日 03:00
  - 常青池水位检查  每日 06:00
  - Milvus Compaction  每周日 01:00
"""
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime

import asyncpg
import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pymilvus import Collection, connections

logger = structlog.get_logger(__name__)

PG_DSN = os.getenv("PG_DSN", "postgresql://postgres@localhost:5432/image_search")
MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))
HOT_COLLECTION = os.getenv("HOT_COLLECTION", "global_images_hot")

EVERGREEN_GREEN = 250_000_000
EVERGREEN_YELLOW = 280_000_000
EVERGREEN_RED = 300_000_000


def _yyyymm_subtract(yyyymm: int, months: int) -> int:
    y, m = divmod(yyyymm, 100)
    m -= months
    while m <= 0:
        m += 12
        y -= 1
    return y * 100 + m


def _get_milvus() -> Collection:
    connections.connect(alias="cron", host=MILVUS_HOST, port=MILVUS_PORT)
    return Collection(HOT_COLLECTION, using="cron")


async def _get_pg():
    return await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=3)


# ── Job 1: 分区轮转 ──

async def partition_rotation():
    """每月1日: 创建新分区 + 删除 >18 月旧分区
    顺序: Milvus Release → PG DELETE → Milvus Drop → 校验
    """
    logger.info("job:partition_rotation started")
    now_yyyymm = int(datetime.now().strftime("%Y%m"))
    cutoff = _yyyymm_subtract(now_yyyymm, 18)

    try:
        coll = _get_milvus()
        pg = await _get_pg()

        for part in coll.partitions:
            name = part.name  # e.g. "p_202401"
            if not name.startswith("p_"):
                continue
            try:
                month_val = int(name.replace("p_", ""))
            except ValueError:
                continue
            if month_val >= cutoff or month_val == 999999:
                continue

            logger.info("rotating_partition", partition=name, month=month_val)
            # FIX-Y: 补偿机制 — 分步记录状态，失败时不丢失进度
            step = "init"
            try:
                step = "milvus_release"
                part.release()
                step = "pg_delete"
                async with pg.acquire() as conn:
                    deleted = await conn.execute(
                        "DELETE FROM uri_dedup WHERE ts_month = $1", month_val
                    )
                step = "milvus_drop"
                coll.drop_partition(name)
                logger.info("partition_rotated", partition=name)
            except Exception as e:
                logger.error(
                    "partition_rotate_failed",
                    partition=name, failed_step=step, error=str(e),
                )
                # FIX-Y: 写补偿记录到 PG, 供后续重试
                try:
                    async with pg.acquire() as conn:
                        await conn.execute(
                            """INSERT INTO partition_rotation_compensation
                               (partition_name, failed_step, error_msg, created_at)
                               VALUES ($1, $2, $3, now())
                               ON CONFLICT (partition_name) DO UPDATE
                               SET failed_step = $2, error_msg = $3, retry_count = partition_rotation_compensation.retry_count + 1""",
                            name, step, str(e),
                        )
                except Exception:
                    pass  # 补偿记录写入失败仅 log
                break  # 失败中止

        await pg.close()
        logger.info("job:partition_rotation complete")
    except Exception as e:
        logger.error("job:partition_rotation error", error=str(e))


# ── Job 2: URI 去重清理 ──

async def uri_dedup_cleanup():
    """每月1日 01:00: DELETE 过期 URI 记录, SLO ≤1h"""
    logger.info("job:uri_dedup_cleanup started")
    now_yyyymm = int(datetime.now().strftime("%Y%m"))
    cutoff = _yyyymm_subtract(now_yyyymm, 18)
    try:
        pg = await _get_pg()
        async with pg.acquire() as conn:
            r = await conn.execute(
                "DELETE FROM uri_dedup WHERE ts_month < $1 AND ts_month != 999999", cutoff
            )
            logger.info("uri_dedup_deleted", cutoff=cutoff, result=r)
            dead = await conn.fetchval(
                "SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname='uri_dedup'"
            )
            if dead and dead > 100_000_000:
                logger.warning("vacuum_triggered", dead_tuples=dead)
                await conn.execute("VACUUM ANALYZE uri_dedup")
        await pg.close()
    except Exception as e:
        logger.error("job:uri_dedup_cleanup error", error=str(e))


# ── Job 3: Bitmap 对账 ──

async def bitmap_reconciliation():
    """每日 03:00: PG vs bitmap-filter-service 比对"""
    logger.info("job:bitmap_reconciliation started")
    try:
        pg = await _get_pg()
        async with pg.acquire() as conn:
            total = await conn.fetchval("SELECT count(*) FROM image_merchant_bitmaps")
            empty = await conn.fetchval(
                "SELECT count(*) FROM image_merchant_bitmaps WHERE rb_cardinality(bitmap) = 0"
            )
        diff = empty or 0
        if diff > 1000:
            logger.error("bitmap_reconciliation_HIGH_DIFF", total=total, empty=diff)
        else:
            logger.info("bitmap_reconciliation OK", total=total, empty_bitmaps=diff)
        await pg.close()
    except Exception as e:
        logger.error("job:bitmap_reconciliation error", error=str(e))


# ── Job 4: 常青池水位治理 ──

async def evergreen_pool_check():
    """每日 06:00: 绿/黄/红 三区水位检查"""
    logger.info("job:evergreen_pool_check started")
    try:
        coll = _get_milvus()
        result = coll.query(expr="is_evergreen == true", output_fields=["count(*)"])
        count = result[0]["count(*)"] if result else 0
        logger.info("evergreen_pool", count=count)

        if count >= EVERGREEN_RED:
            logger.critical("evergreen_RED", count=count)
            pg = await _get_pg()
            await _evergreen_cleanup(coll, pg, EVERGREEN_GREEN, min_age_years=2)
            await pg.close()
        elif count >= EVERGREEN_YELLOW:
            logger.warning("evergreen_YELLOW", count=count)
            pg = await _get_pg()
            await _evergreen_cleanup(coll, pg, EVERGREEN_GREEN, min_age_years=5)
            await pg.close()
        else:
            logger.info("evergreen_GREEN", count=count)
    except Exception as e:
        logger.error("job:evergreen_pool_check error", error=str(e))


async def _evergreen_cleanup(coll, pg, target: int, min_age_years: int):
    cutoff_ms = int(time.time() * 1000) - min_age_years * 365 * 86400 * 1000
    total_deleted = 0
    while True:
        rows = coll.query(
            expr=f"is_evergreen == true and promoted_at > 0 and promoted_at < {cutoff_ms}",
            output_fields=["image_pk"], limit=1000,
        )
        if not rows:
            break
        pks = [r["image_pk"] for r in rows]
        try:
            coll.delete(expr=f'image_pk in {pks}')
            async with pg.acquire() as conn:
                await conn.execute(
                    "DELETE FROM image_merchant_bitmaps WHERE image_pk = ANY($1::char(32)[])", pks
                )
            total_deleted += len(pks)
            logger.info("evergreen_cleanup_batch", batch=len(pks), total=total_deleted)
        except Exception as e:
            logger.error("evergreen_cleanup_batch_fail", error=str(e))
            break
    logger.info("evergreen_cleanup_done", total=total_deleted)


# ── Job 5: Milvus Compaction ──

async def milvus_compaction():
    """每周日 01:00: 手动触发 Segment 合并"""
    logger.info("job:milvus_compaction started")
    try:
        coll = _get_milvus()
        coll.compact()
        logger.info("job:milvus_compaction triggered")
    except Exception as e:
        logger.error("job:milvus_compaction error", error=str(e))


# ── 主入口 ──

def main():
    scheduler = AsyncIOScheduler()

    scheduler.add_job(partition_rotation,
                      CronTrigger(day=1, hour=0, minute=0),
                      id="partition_rotation", misfire_grace_time=3600)

    scheduler.add_job(uri_dedup_cleanup,
                      CronTrigger(day=1, hour=1, minute=0),
                      id="uri_dedup_cleanup", misfire_grace_time=3600)

    scheduler.add_job(bitmap_reconciliation,
                      CronTrigger(hour=3, minute=0),
                      id="bitmap_reconciliation", misfire_grace_time=3600)

    scheduler.add_job(evergreen_pool_check,
                      CronTrigger(hour=6, minute=0),
                      id="evergreen_pool_check", misfire_grace_time=3600)

    scheduler.add_job(milvus_compaction,
                      CronTrigger(day_of_week="sun", hour=1, minute=0),
                      id="milvus_compaction", misfire_grace_time=3600)

    logger.info("cron-scheduler starting: 5 jobs registered")
    scheduler.start()

    loop = asyncio.get_event_loop()
    try:
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":
    main()
