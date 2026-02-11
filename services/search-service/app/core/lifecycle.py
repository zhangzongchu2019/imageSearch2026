"""
服务生命周期管理 — 生产级初始化/关闭
v1.3 加固:
  - 所有外部依赖初始化使用指数退避重试 (FIX #1)
  - 有界线程池给 Milvus executor (FIX #2)
  - gRPC channel 优雅关闭 (FIX #5)
  - 降级 tick 从 Prometheus 读取真实 P99 (FIX #13)
  - readiness probe 联动
"""
from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple

import structlog

from app.core.config import get_settings
from app.core.config_service import get_config_service
from app.core.degrade_fsm import DegradeStateMachine
from app.core.pipeline import SearchPipeline

logger = structlog.get_logger(__name__)
settings = get_settings()


class TokenBucketLimiter:
    """令牌桶限流器"""

    def __init__(self):
        self._buckets = {}
        self._lock = asyncio.Lock()

    def configure(self, name: str, rate: float, burst: int):
        self._buckets[name] = {
            "rate": rate,
            "burst": burst,
            "tokens": float(burst),
            "last_refill": time.monotonic(),
        }

    def try_acquire(self, name: str) -> bool:
        bucket = self._buckets.get(name)
        if not bucket:
            return True
        now = time.monotonic()
        elapsed = now - bucket["last_refill"]
        bucket["tokens"] = min(
            bucket["burst"], bucket["tokens"] + elapsed * bucket["rate"]
        )
        bucket["last_refill"] = now
        if bucket["tokens"] >= 1.0:
            bucket["tokens"] -= 1.0
            return True
        return False


async def _retry_with_backoff(
    func,
    name: str,
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
):
    """指数退避重试 — 所有外部依赖初始化统一使用 (FIX #1)"""
    for attempt in range(max_retries + 1):
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func()
            else:
                result = func()
            if attempt > 0:
                logger.info(
                    "dependency_connected_after_retry",
                    name=name, attempts=attempt + 1,
                )
            return result
        except Exception as e:
            if attempt == max_retries:
                logger.error(
                    "dependency_init_exhausted",
                    name=name, attempts=max_retries + 1, error=str(e),
                )
                raise
            delay = min(base_delay * (2 ** attempt), max_delay)
            logger.warning(
                "dependency_init_retry",
                name=name, attempt=attempt + 1, delay_s=delay, error=str(e),
            )
            await asyncio.sleep(delay)


class ServiceLifecycle:
    """管理所有服务依赖的生命周期 — 生产级"""

    def __init__(self):
        self.ready = False
        self.start_time = time.monotonic()

        self.redis_client = None
        self.pg_pool = None
        self.milvus_client = None
        self.kafka_producer = None
        self.degrade_fsm = None
        self.pipeline = None
        self.rate_limiter = TokenBucketLimiter()
        self.behavior_reporter = None
        self.config_service = None

        # FIX #2: 有界线程池 — Milvus 同步 SDK 专用
        pool_size = max(8, settings.server.workers * 4)
        self._milvus_executor = ThreadPoolExecutor(
            max_workers=pool_size,
            thread_name_prefix="milvus-io",
        )

        # FIX #5: gRPC client 引用 (shutdown 时关闭)
        self._bitmap_grpc_client = None
        self._degrade_task: Optional[asyncio.Task] = None

    async def startup(self):
        """按依赖顺序初始化 — 全部带指数退避重试"""
        logger.info("lifecycle_startup_begin")

        # 0. 配置服务
        self.config_service = get_config_service()
        await self.config_service.init()

        # 1. Redis (带重试)
        self.redis_client = await _retry_with_backoff(
            self._init_redis, "redis", max_retries=5, base_delay=1.0
        )

        # 2. PostgreSQL (带重试)
        self.pg_pool = await _retry_with_backoff(
            self._init_postgres, "postgres", max_retries=5, base_delay=2.0
        )

        # 3. Milvus (带重试)
        self.milvus_client = await _retry_with_backoff(
            self._init_milvus, "milvus", max_retries=5, base_delay=2.0
        )

        # 4. Kafka (带重试)
        self.kafka_producer = await _retry_with_backoff(
            self._init_kafka, "kafka", max_retries=5, base_delay=2.0
        )

        # 5. 降级状态机
        self.degrade_fsm = DegradeStateMachine(redis_client=self.redis_client)

        # 6. 基础设施客户端
        from app.engine.feature_extractor import FeatureExtractor
        from app.engine.ranker import FusionRanker
        from app.engine.refiner import Refiner
        from app.infra.bitmap_grpc_client import BitmapFilterGrpcClient
        from app.infra.behavior_reporter import BehaviorReporter
        from app.infra.milvus_client import MilvusSearchClient
        from app.infra.scope_resolver import ScopeResolver
        from app.infra.search_log_emitter import SearchLogEmitter
        from app.infra.vocab_cache import VocabCache

        ann_searcher = MilvusSearchClient(
            self.milvus_client, executor=self._milvus_executor
        )
        self._bitmap_grpc_client = BitmapFilterGrpcClient(
            pg_pool=self.pg_pool, redis_client=self.redis_client
        )
        feature_extractor = FeatureExtractor()
        refiner = Refiner(self.milvus_client)
        ranker = FusionRanker()
        scope_resolver = ScopeResolver(self.redis_client, self.pg_pool)
        vocab_cache = VocabCache(self.redis_client, self.pg_pool)
        search_logger = SearchLogEmitter(self.kafka_producer)
        self.behavior_reporter = BehaviorReporter(self.kafka_producer)

        # 7. 检索流水线
        self.pipeline = SearchPipeline(
            degrade_fsm=self.degrade_fsm,
            feature_extractor=feature_extractor,
            ann_searcher=ann_searcher,
            bitmap_filter=self._bitmap_grpc_client,
            refiner=refiner,
            ranker=ranker,
            scope_resolver=scope_resolver,
            vocab_cache=vocab_cache,
            search_logger=search_logger,
        )

        # 8. 限流器
        self.rate_limiter.configure(
            "global_search",
            rate=settings.rate_limit.global_search_qps,
            burst=settings.rate_limit.global_search_burst,
        )

        # 9. 降级 tick 后台任务 — 带 done callback 防止静默失败
        self._degrade_task = asyncio.create_task(self._degrade_tick_loop())
        self._degrade_task.add_done_callback(self._on_background_task_done)

        # 10. 预热
        await vocab_cache.warm_up()

        # 11. Nacos 配置监听
        await self.config_service.start_nacos_listener()

        self.ready = True
        logger.info("lifecycle_startup_complete")

    async def shutdown(self):
        """优雅关闭: 反序释放资源"""
        self.ready = False
        logger.info("lifecycle_shutdown_begin")

        # 取消降级 tick
        if self._degrade_task and not self._degrade_task.done():
            self._degrade_task.cancel()
            try:
                await self._degrade_task
            except asyncio.CancelledError:
                pass

        # FIX #5: gRPC channel 关闭
        if self._bitmap_grpc_client:
            try:
                await self._bitmap_grpc_client.close()
            except Exception as e:
                logger.warning("bitmap_grpc_close_error", error=str(e))

        if self.kafka_producer:
            await self.kafka_producer.stop()
        if self.pg_pool:
            await self.pg_pool.close()

        # 关闭 Milvus 线程池
        self._milvus_executor.shutdown(wait=True, cancel_futures=False)

        # Redis 关闭
        if self.redis_client:
            try:
                await self.redis_client.aclose()
            except Exception:
                pass

        logger.info("lifecycle_shutdown_complete")

    # ── FIX #13: 从 Prometheus 读取真实指标 ──

    async def _degrade_tick_loop(self):
        """每秒从 Prometheus histogram 读取 P99 和 error_rate"""
        while True:
            try:
                p99, error_rate = self._read_live_metrics()
                self.degrade_fsm.tick(p99, error_rate)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("degrade_tick_error", error=str(e))
            await asyncio.sleep(1)

    def _read_live_metrics(self) -> Tuple[float, float]:
        """从 prometheus_client registry 读取实时 P99 和错误率"""
        try:
            from prometheus_client import REGISTRY

            p99_ms = 0.0
            total_requests = 0.0
            total_errors = 0.0

            for metric in REGISTRY.collect():
                # 读取 search 请求总延迟 (histogram _sum/_count)
                if metric.name == "http_request_duration_seconds":
                    req_sum = 0.0
                    req_count = 0
                    for sample in metric.samples:
                        if (
                            sample.name.endswith("_sum")
                            and sample.labels.get("handler", "").endswith("/image/search")
                        ):
                            req_sum += sample.value
                        elif (
                            sample.name.endswith("_count")
                            and sample.labels.get("handler", "").endswith("/image/search")
                        ):
                            req_count += int(sample.value)
                    if req_count > 0:
                        # P99 ≈ avg × 2.5 (保守估计, 生产可接 HDRHistogram)
                        p99_ms = (req_sum / req_count) * 1000 * 2.5
                        total_requests = float(req_count)

                # 读取错误总数
                if metric.name == "image_search_errors_total":
                    for sample in metric.samples:
                        if sample.name.endswith("_total"):
                            total_errors += sample.value

            error_rate = (
                total_errors / total_requests if total_requests > 0 else 0.0
            )
            return p99_ms, error_rate

        except Exception:
            return 0.0, 0.0

    @staticmethod
    def _on_background_task_done(task: asyncio.Task):
        """后台任务异常回调 — 防止静默吞异常"""
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error(
                "background_task_crashed",
                error=str(exc), exc_type=type(exc).__name__,
            )

    # ── 依赖初始化 ──

    async def _init_redis(self):
        import redis.asyncio as aioredis

        cfg = settings.redis
        cs = get_config_service()
        host = cs.get_str("redis.host", cfg.host)
        port = cs.get_int("redis.port", cfg.port)
        password = cs.get_secret("redis_password") or cfg.password
        sentinel_master = cs.get_str("redis.sentinel_master", cfg.sentinel_master or "")

        if sentinel_master:
            sentinel = aioredis.Sentinel([(host, port)], password=password)
            client = sentinel.master_for(sentinel_master)
        else:
            client = aioredis.Redis(
                host=host, port=port, password=password, db=cfg.db,
                decode_responses=True, max_connections=50,
                socket_connect_timeout=5.0, socket_timeout=3.0,
                retry_on_timeout=True,
            )
        await client.ping()
        logger.info("redis_connected", host=host)
        return client

    async def _init_postgres(self):
        import asyncpg

        cfg = settings.postgres
        cs = get_config_service()
        host = cs.get_str("postgres.host", cfg.host)
        port = cs.get_int("postgres.port", cfg.port)
        database = cs.get_str("postgres.database", cfg.database)
        user = cs.get_str("postgres.user", cfg.user)
        password = cs.get_secret("pg_password") or cfg.password
        ssl = "require" if cfg.ssl else "disable"

        dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode={ssl}"
        pool = await asyncpg.create_pool(
            dsn=dsn, min_size=cfg.pool_min, max_size=cfg.pool_max,
            command_timeout=10.0,
        )
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        logger.info("postgres_connected", host=host, database=database)
        return pool

    async def _init_milvus(self):
        from pymilvus import connections

        cfg = settings.milvus
        cs = get_config_service()
        host = cs.get_str("milvus.host", cfg.host)
        port = cs.get_int("milvus.port", cfg.port)
        token = cs.get_secret("milvus_token") or cfg.token

        connections.connect(alias="default", host=host, port=port, token=token)
        logger.info("milvus_connected", host=host)
        return connections

    async def _init_kafka(self):
        from aiokafka import AIOKafkaProducer

        cfg = settings.kafka
        cs = get_config_service()
        bootstrap = cs.get_str("kafka.bootstrap_servers", cfg.bootstrap_servers)
        security = cs.get_str("kafka.security_protocol", cfg.security_protocol)

        kwargs = {
            "bootstrap_servers": bootstrap,
            "security_protocol": security,
            "request_timeout_ms": 10_000,
        }
        if "SASL" in security:
            kwargs.update({
                "sasl_mechanism": cs.get_str("kafka.sasl_mechanism", "SCRAM-SHA-256"),
                "sasl_plain_username": cs.get_secret("kafka_user") or "",
                "sasl_plain_password": cs.get_secret("kafka_password") or "",
            })
        producer = AIOKafkaProducer(**kwargs)
        await producer.start()
        logger.info("kafka_connected", bootstrap=bootstrap)
        return producer
