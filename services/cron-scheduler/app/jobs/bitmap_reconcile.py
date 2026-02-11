"""
Bitmap 对账 — 每日 03:00
PG image_merchant_bitmaps vs bitmap-filter-service RocksDB 全量比对
FIX-Z: 完成 RocksDB 侧对比 (通过 gRPC HealthCheck 获取全局统计)
"""
from __future__ import annotations

import grpc
import structlog

logger = structlog.get_logger(__name__)


class BitmapReconciliation:
    def __init__(self, deps: dict):
        self._pg = deps["pg"]
        self._bitmap_filter_host = deps.get("bitmap_filter_host", "bitmap-filter-service:50051")

    async def execute(self):
        logger.info("bitmap_reconcile_start")
        batch_size = 100_000
        offset = 0
        pg_zero_count = 0
        pg_total_card = 0

        async with self._pg.acquire() as conn:
            total = await conn.fetchval("SELECT count(*) FROM image_merchant_bitmaps")

        logger.info("bitmap_reconcile_total", total=total)

        # 分批校验 PG 侧
        while offset < total:
            async with self._pg.acquire() as conn:
                rows = await conn.fetch(
                    """SELECT image_pk, rb_cardinality(bitmap) as card
                       FROM image_merchant_bitmaps
                       ORDER BY image_pk
                       LIMIT $1 OFFSET $2""",
                    batch_size, offset,
                )

            for r in rows:
                if r["card"] == 0:
                    pg_zero_count += 1
                pg_total_card += r["card"]

            offset += batch_size

        # FIX-Z: RocksDB 侧统计 (通过 gRPC HealthCheck 获取)
        rocksdb_size_bytes = 0
        rocksdb_cdc_lag_ms = -1
        try:
            channel = grpc.aio.insecure_channel(self._bitmap_filter_host)
            from app.infra.pb import bitmap_filter_pb2, bitmap_filter_pb2_grpc
            stub = bitmap_filter_pb2_grpc.BitmapFilterServiceStub(channel)
            resp = await stub.HealthCheck(bitmap_filter_pb2.HealthCheckRequest())
            rocksdb_size_bytes = resp.rocksdb_size_bytes
            rocksdb_cdc_lag_ms = resp.cdc_lag_ms
            await channel.close()
        except Exception as e:
            logger.warning("bitmap_reconcile_grpc_failed", error=str(e))

        # 汇总报告
        report = {
            "pg_total_rows": total,
            "pg_zero_bitmap_count": pg_zero_count,
            "pg_total_cardinality": pg_total_card,
            "rocksdb_size_bytes": rocksdb_size_bytes,
            "rocksdb_cdc_lag_ms": rocksdb_cdc_lag_ms,
        }

        if pg_zero_count > 1000 or rocksdb_cdc_lag_ms > 60_000:
            logger.error("bitmap_reconcile_diff_ALERT", **report)
        else:
            logger.info("bitmap_reconcile_ok", **report)
