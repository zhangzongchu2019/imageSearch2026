"""
Bitmap 过滤 gRPC 客户端 — 三级降级: gRPC → PG → 跳过

P0 修复:
  - 使用编译后的 proto stub (不再 NotImplementedError)
  - merchant_scope → Roaring Bitmap 构建 (via PG merchant_id_mapping)
  - filter_skipped=true 作为强信号输出
"""
from __future__ import annotations

import asyncio
import hashlib
import io
import time
from typing import Dict, List, Set, Tuple

import grpc
import structlog

from app.core.config import get_settings
from app.core.metrics import METRICS

logger = structlog.get_logger(__name__)
settings = get_settings()

# Proto stubs — 编译自 proto/bitmap_filter.proto
# 执行: python -m grpc_tools.protoc -Iproto --python_out=... --grpc_python_out=... proto/bitmap_filter.proto
from app.infra.pb import bitmap_filter_pb2
from app.infra.pb import bitmap_filter_pb2_grpc


class MerchantBitmapBuilder:
    """merchant_scope → Roaring Bitmap 序列化 bytes

    层级:
      L1 内存缓存 (scope_hash → bytes)
      L2 Redis 缓存 (5min TTL)
      L3 PG 查询 merchant_id_mapping → 构建 Bitmap
    """

    def __init__(self, pg_pool, redis_client=None):
        self._pg = pg_pool
        self._redis = redis_client
        self._cache: Dict[str, bytes] = {}

    async def build(self, merchant_scope: List[str]) -> bytes:
        scope_key = hashlib.md5(
            ",".join(sorted(merchant_scope)).encode()
        ).hexdigest()

        # L1: 内存
        if scope_key in self._cache:
            return self._cache[scope_key]

        # L2: Redis
        if self._redis:
            try:
                cached = await self._redis.get(f"scope_bmp:{scope_key}")
                if cached:
                    if isinstance(cached, str):
                        cached = cached.encode("latin-1")
                    self._cache[scope_key] = cached
                    return cached
            except Exception:
                pass

        # L3: PG → 构建
        bmp_bytes = await self._build_from_pg(merchant_scope)
        self._cache[scope_key] = bmp_bytes

        if self._redis:
            try:
                await self._redis.set(f"scope_bmp:{scope_key}", bmp_bytes, ex=300)
            except Exception:
                pass

        return bmp_bytes

    async def _build_from_pg(self, merchant_scope: List[str]) -> bytes:
        """PG merchant_id_mapping → bitmap_index → Roaring Bitmap"""
        async with self._pg.acquire() as conn:
            rows = await conn.fetch(
                """SELECT bitmap_index FROM merchant_id_mapping
                   WHERE merchant_str = ANY($1::varchar[])""",
                merchant_scope,
            )

        if not rows:
            return b""

        # 使用 PG 的 rb_build 直接生成序列化 bitmap (更高效)
        indices = [r["bitmap_index"] for r in rows]
        async with self._pg.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT rb_build($1::int[])::bytea AS bmp", indices
            )
        return row["bmp"] if row else b""


class BitmapFilterGrpcClient:
    """bitmap-filter-service gRPC 客户端

    三级降级 (对齐系统设计 v1.2 §6.2):
      Level 0: gRPC → bitmap-filter-service (8ms 超时)
      Level 1: PG 直查 rb_intersect (10ms 超时)
      Level 2: 跳过过滤 (兜底, filter_skipped=true 强信号)
    """

    def __init__(self, pg_pool=None, redis_client=None):
        cfg = settings.bitmap_filter
        target = f"{cfg.host}:{cfg.port}"
        self._channel = grpc.aio.insecure_channel(
            target,
            options=[
                ("grpc.max_receive_message_length", 16 * 1024 * 1024),
                ("grpc.keepalive_time_ms", 10_000),
                ("grpc.keepalive_timeout_ms", 5_000),
            ],
        )
        self._stub = bitmap_filter_pb2_grpc.BitmapFilterServiceStub(self._channel)
        self._pg_pool = pg_pool
        self._bitmap_builder = MerchantBitmapBuilder(pg_pool, redis_client)
        self._health_status = "SERVING"
        self._health_ts = 0.0

    async def close(self):
        await self._channel.close()

    # ── 主入口 ──

    async def filter_with_degrade(
        self,
        candidate_pks: List[str],
        merchant_scope: List[str],
        candidates: list,
    ) -> Tuple[list, bool]:
        """三级降级过滤, 返回 (过滤后候选, filter_skipped)"""
        if not merchant_scope:
            return candidates, False

        # 构建查询 Bitmap
        try:
            query_bitmap = await self._bitmap_builder.build(merchant_scope)
            if not query_bitmap:
                logger.warning("empty_query_bitmap", scope_size=len(merchant_scope))
                return candidates, True
        except Exception as e:
            logger.error("bitmap_build_failed", error=str(e))
            return await self._level1_pg(candidate_pks, merchant_scope, candidates)

        # Level 0: gRPC
        if self._health_status != "NOT_SERVING":
            try:
                filtered_pks = await asyncio.wait_for(
                    self._grpc_filter(candidate_pks, query_bitmap),
                    timeout=settings.bitmap_filter.timeout_ms / 1000,
                )
                filtered = [c for c in candidates if c.image_pk in filtered_pks]
                return filtered, False
            except asyncio.TimeoutError:
                METRICS.bitmap_filter_fallback.labels(level="1", reason="grpc_timeout").inc()
                logger.warning("grpc_timeout")
            except grpc.aio.AioRpcError as e:
                METRICS.bitmap_filter_fallback.labels(level="1", reason="grpc_rpc_error").inc()
                logger.warning("grpc_rpc_error", code=e.code().name)
            except Exception as e:
                METRICS.bitmap_filter_fallback.labels(level="1", reason="grpc_error").inc()
                logger.warning("grpc_error", error=str(e))

        # Level 1: PG
        return await self._level1_pg(candidate_pks, merchant_scope, candidates)

    # ── Level 0: gRPC ──

    async def _grpc_filter(
        self, candidate_pks: List[str], query_bitmap: bytes
    ) -> Set[str]:
        pk_bytes_list = [bytes.fromhex(pk) for pk in candidate_pks]

        request = bitmap_filter_pb2.BatchFilterRequest(
            candidate_pks=pk_bytes_list,
            query_bitmap=query_bitmap,
            max_results=0,
        )
        response = await self._stub.BatchFilter(
            request,
            timeout=settings.bitmap_filter.timeout_ms / 1000,
        )
        return {pk.hex() for pk in response.filtered_pks}

    # ── Level 1: PG ──

    async def _level1_pg(
        self,
        candidate_pks: List[str],
        merchant_scope: List[str],
        candidates: list,
    ) -> Tuple[list, bool]:
        if not self._pg_pool:
            logger.error("bitmap_filter_skipped_no_pg")
            return candidates, True

        try:
            filtered_pks = await asyncio.wait_for(
                self._pg_filter(candidate_pks, merchant_scope),
                timeout=settings.bitmap_filter.pg_fallback_timeout_ms / 1000,
            )
            filtered = [c for c in candidates if c.image_pk in filtered_pks]
            METRICS.bitmap_filter_fallback.labels(level="1", reason="pg_ok").inc()
            return filtered, False
        except Exception as e:
            METRICS.bitmap_filter_fallback.labels(level="2", reason="pg_error").inc()
            logger.error("bitmap_filter_ALL_LEVELS_FAILED", error=str(e))
            return candidates, True

    async def _pg_filter(
        self, candidate_pks: List[str], merchant_scope: List[str]
    ) -> Set[str]:
        async with self._pg_pool.acquire() as conn:
            indices = await conn.fetch(
                """SELECT bitmap_index FROM merchant_id_mapping
                   WHERE merchant_str = ANY($1::varchar[])""",
                merchant_scope,
            )
            if not indices:
                return set()
            index_list = [r["bitmap_index"] for r in indices]
            rows = await conn.fetch(
                """SELECT image_pk FROM image_merchant_bitmaps
                   WHERE image_pk = ANY($1::char(32)[])
                   AND rb_intersect(bitmap, rb_build($2::int[]))""",
                candidate_pks,
                index_list,
            )
            return {r["image_pk"].strip() for r in rows}

    # ── Health Check ──

    async def health_check(self) -> str:
        now = time.monotonic()
        if now - self._health_ts < 30:
            return self._health_status
        try:
            resp = await self._stub.HealthCheck(
                bitmap_filter_pb2.HealthCheckRequest(), timeout=2.0
            )
            self._health_status = {0: "SERVING", 1: "NOT_SERVING", 2: "DEGRADED"}.get(
                resp.status, "SERVING"
            )
        except Exception:
            self._health_status = "NOT_SERVING"
        self._health_ts = now
        return self._health_status
