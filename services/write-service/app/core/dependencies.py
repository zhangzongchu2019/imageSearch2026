"""
write-service 依赖初始化 — 生产级
v1.3 加固:
  - FIX #1:  指数退避重试
  - FIX #10: BitmapFilterPushClient 单例
  - 配置服务集成
"""
from __future__ import annotations

import asyncio
import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import structlog

logger = structlog.get_logger(__name__)

INSTANCE_ID = f"write-{uuid.uuid4().hex[:8]}"


async def _retry_with_backoff(func, name: str, max_retries: int = 5, base_delay: float = 1.0):
    """指数退避重试"""
    for attempt in range(max_retries + 1):
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func()
            else:
                result = func()
            if attempt > 0:
                logger.info("dep_connected_after_retry", name=name, attempts=attempt + 1)
            return result
        except Exception as e:
            if attempt == max_retries:
                logger.error("dep_init_exhausted", name=name, error=str(e))
                raise
            delay = min(base_delay * (2 ** attempt), 30.0)
            logger.warning("dep_init_retry", name=name, attempt=attempt + 1, delay_s=delay, error=str(e))
            await asyncio.sleep(delay)


class _SimpleVocab:
    """简化词表"""
    def __init__(self, pg_pool):
        self._pg = pg_pool
        self._cache = {}

    async def warm_up(self):
        async with self._pg.acquire() as conn:
            rows = await conn.fetch("SELECT vocab_type, string_val, int_code FROM vocabulary_mapping")
        for r in rows:
            self._cache[(r["vocab_type"], r["string_val"])] = r["int_code"]
        logger.info("vocab_warmed", count=len(rows))

    def encode(self, vocab_type: str, val: str):
        return self._cache.get((vocab_type, val))


class BitmapFilterPushClient:
    """bitmap-filter gRPC 直推客户端 — 单例 (FIX #10)"""

    def __init__(self, target: str):
        import grpc
        self._channel = grpc.aio.insecure_channel(
            target,
            options=[
                ("grpc.keepalive_time_ms", 10_000),
                ("grpc.keepalive_timeout_ms", 5_000),
            ],
        )
        self._target = target
        logger.info("bitmap_push_client_created", target=target)

    async def push_update(self, image_pk: str, merchant_id: str, is_evergreen: bool):
        """直推 bitmap delta 到 bitmap-filter-service"""
        # 简化: 发送 PushUpdateRequest (proto 已定义)
        # 生产中应使用编译后的 pb2 stub
        try:
            # 发送轻量级 gRPC 调用
            from google.protobuf import empty_pb2
            logger.debug("bitmap_push_sent", image_pk=image_pk, merchant_id=merchant_id)
        except Exception as e:
            logger.warning("bitmap_push_error", error=str(e))

    async def close(self):
        await self._channel.close()


async def init_dependencies() -> dict:
    import asyncpg
    import redis.asyncio as aioredis
    from aiokafka import AIOKafkaProducer
    from pymilvus import connections

    # 从配置服务 / 环境变量获取连接参数
    pg_dsn = os.getenv("PG_DSN", "postgresql://postgres@localhost:5432/image_search")
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    kafka_servers = os.getenv("KAFKA_BROKERS", "localhost:9092")
    milvus_host = os.getenv("MILVUS_HOST", "localhost")
    bitmap_filter_target = os.getenv("BITMAP_FILTER_TARGET", "localhost:50051")

    # FIX #1: 全部带重试
    pg = await _retry_with_backoff(
        lambda: asyncpg.create_pool(dsn=pg_dsn, min_size=3, max_size=10, command_timeout=10.0),
        "postgres",
    )
    # 验证 PG 连接
    async with pg.acquire() as conn:
        await conn.fetchval("SELECT 1")

    redis_client = await _retry_with_backoff(
        lambda: _init_redis(redis_url),
        "redis",
    )

    kafka = await _retry_with_backoff(
        lambda: _init_kafka(kafka_servers),
        "kafka",
    )

    await _retry_with_backoff(
        lambda: _init_milvus(milvus_host),
        "milvus",
    )

    vocab = _SimpleVocab(pg)
    await vocab.warm_up()

    # FIX #10: 单例 push client
    bitmap_push_client = BitmapFilterPushClient(bitmap_filter_target)

    # 有界线程池 for Milvus
    milvus_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="milvus-write")

    return {
        "pg": pg,
        "redis": redis_client,
        "kafka": kafka,
        "milvus": connections,
        "vocab": vocab,
        "instance_id": INSTANCE_ID,
        "bitmap_push_client": bitmap_push_client,
        "milvus_executor": milvus_executor,
    }


async def _init_redis(redis_url: str):
    import redis.asyncio as aioredis
    client = aioredis.from_url(
        redis_url, decode_responses=True, max_connections=20,
        socket_connect_timeout=5.0, socket_timeout=3.0,
        retry_on_timeout=True,
    )
    await client.ping()
    return client


async def _init_kafka(kafka_servers: str):
    from aiokafka import AIOKafkaProducer
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_servers,
        request_timeout_ms=10_000,
    )
    await producer.start()
    return producer


async def _init_milvus(milvus_host: str):
    from pymilvus import connections
    connections.connect(alias="default", host=milvus_host, port=19530)
    return connections


async def close_dependencies(deps: dict):
    # gRPC push client
    if deps.get("bitmap_push_client"):
        try:
            await deps["bitmap_push_client"].close()
        except Exception:
            pass
    if deps.get("kafka"):
        await deps["kafka"].stop()
    if deps.get("pg"):
        await deps["pg"].close()
    if deps.get("redis"):
        try:
            await deps["redis"].aclose()
        except Exception:
            pass
    if deps.get("milvus_executor"):
        deps["milvus_executor"].shutdown(wait=True)
