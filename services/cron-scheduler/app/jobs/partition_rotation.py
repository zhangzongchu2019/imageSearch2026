"""
分区轮转 — 每月 1 日 00:00
Milvus + PG 两端联动: 创建新分区 → 删除 >18月旧分区
执行顺序: Milvus Release → PG DELETE → Milvus Drop → 校验
"""
from __future__ import annotations

import asyncio

import structlog

logger = structlog.get_logger(__name__)


class PartitionRotation:
    def __init__(self, deps: dict):
        self._pg = deps["pg"]
        self._milvus = deps["milvus"]

    async def execute(self):
        """分区轮转主流程"""
        from datetime import datetime
        now = datetime.utcnow()
        new_month = int(now.strftime("%Y%m"))

        # 计算过期月份 (>18 个月前)
        y, m = divmod(new_month, 100)
        total = y * 12 + (m - 1) - 18
        cutoff_month = (total // 12) * 100 + (total % 12) + 1

        logger.info("partition_rotation_start",
                     new_month=new_month, cutoff_month=cutoff_month)

        try:
            # Step 1: Milvus Release 旧分区
            loop = asyncio.get_event_loop()
            from pymilvus import Collection
            for coll_name in ["global_images_hot", "global_images_non_hot"]:
                coll = Collection(coll_name)
                partitions = await loop.run_in_executor(None, coll.partitions)
                for p in partitions:
                    try:
                        part_month = int(p.name.replace("p_", ""))
                        if part_month <= cutoff_month and part_month != 999999:
                            logger.info("releasing_partition",
                                        collection=coll_name, partition=p.name)
                            await loop.run_in_executor(None, p.release)
                    except (ValueError, AttributeError):
                        continue

            # Step 2: PG DELETE 对应月份
            async with self._pg.acquire() as conn:
                deleted = await conn.execute(
                    """DELETE FROM uri_dedup
                       WHERE ts_month <= $1 AND ts_month != 999999""",
                    cutoff_month,
                )
                logger.info("pg_uri_dedup_cleaned", cutoff=cutoff_month, result=deleted)

            # Step 3: Milvus Drop 旧分区
            for coll_name in ["global_images_hot", "global_images_non_hot"]:
                coll = Collection(coll_name)
                partitions = await loop.run_in_executor(None, coll.partitions)
                for p in partitions:
                    try:
                        part_month = int(p.name.replace("p_", ""))
                        if part_month <= cutoff_month and part_month != 999999:
                            logger.info("dropping_partition",
                                        collection=coll_name, partition=p.name)
                            await loop.run_in_executor(None, p.drop)
                    except (ValueError, AttributeError):
                        continue

            # Step 4: 创建新月分区
            for coll_name in ["global_images_hot", "global_images_non_hot"]:
                coll = Collection(coll_name)
                partition_name = f"p_{new_month}"
                try:
                    await loop.run_in_executor(
                        None, lambda: coll.create_partition(partition_name)
                    )
                    logger.info("new_partition_created",
                                collection=coll_name, partition=partition_name)
                except Exception as e:
                    logger.warning("partition_already_exists", error=str(e))

            logger.info("partition_rotation_complete")

        except Exception as e:
            logger.error("partition_rotation_failed", error=str(e))
            # 失败告警, 不重试 (等下月或人工介入)
            raise
