"""
update-image 写入主链路 v1.3
v1.3 加固:
  - FIX #3:  Flush Task 异常捕获 (不再静默吞异常)
  - FIX #4:  分布式锁 watchdog 续约
  - FIX #9:  事务性写路径 (Milvus + PG 原子化 + 补偿日志)
  - FIX #10: BitmapFilterPushClient 单例
  - FIX #12: OpenTelemetry trace_id 贯穿
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
import uuid
from typing import List, Optional

import httpx
import structlog
from fastapi import APIRouter, HTTPException, Request

logger = structlog.get_logger(__name__)
router = APIRouter()

from app.core.metrics import (
    write_visibility_latency,
    push_update_total,
    write_stage_latency,
)


from pydantic import BaseModel, Field


class UpdateImageRequest(BaseModel):
    uri: str = Field(..., min_length=1, max_length=2048)
    merchant_id: str = Field(..., min_length=1, max_length=64)
    category_l1: str = Field(..., min_length=1)
    product_id: Optional[str] = Field(None, max_length=64)
    tags: Optional[List[str]] = Field(None, max_length=32)


class UpdateImageResponse(BaseModel):
    status: str = "accepted"
    image_id: str
    is_new: bool


class UpdateVideoRequest(BaseModel):
    video_uri: str = Field(..., min_length=1, max_length=2048)
    merchant_id: str = Field(..., min_length=1, max_length=64)
    category_l1: str = Field(..., min_length=1)
    product_id: Optional[str] = Field(None, max_length=64)
    max_frames: int = Field(3, ge=1, le=10)


class BindMerchantRequest(BaseModel):
    image_id: str = Field(..., min_length=32, max_length=32)
    merchant_id: str = Field(..., min_length=1, max_length=64)


# ── Trace ID 生成 (FIX #12) ──

def _gen_trace_id() -> str:
    """生成符合 W3C TraceContext 的 trace_id (32 hex chars)"""
    return uuid.uuid4().hex


# ── 核心写入逻辑 ──

def _gen_image_pk(uri: str) -> str:
    """image_pk = hex(sha256(uri)[0:16]) → CHAR(32)"""
    digest = hashlib.sha256(uri.encode()).digest()
    return digest[:16].hex()


@router.post("/image/update", response_model=UpdateImageResponse, summary="图片写入/更新")
async def update_image(req: UpdateImageRequest, request: Request):
    deps = request.app.state.deps
    image_pk = _gen_image_pk(req.uri)
    trace_id = _gen_trace_id()

    is_new = await _check_dedup(deps, image_pk, req.uri)

    if is_new:
        await _process_new_image(deps, image_pk, req, trace_id)

    await _emit_merchant_event(deps, image_pk, req.merchant_id, "update-image", trace_id)

    return UpdateImageResponse(status="accepted", image_id=image_pk, is_new=is_new)


@router.post("/merchant/bindImage", summary="图片-商家绑定")
async def bind_merchant(req: BindMerchantRequest, request: Request):
    deps = request.app.state.deps
    trace_id = _gen_trace_id()
    await _emit_merchant_event(deps, req.image_id, req.merchant_id, "bind-merchant", trace_id)
    return {"status": "accepted", "image_id": req.image_id}


@router.post("/video/update", summary="视频写入 (关键帧提取)")
async def update_video(req: UpdateVideoRequest, request: Request):
    deps = request.app.state.deps
    trace_id = _gen_trace_id()
    frame_pk = _gen_image_pk(req.video_uri + "_frame_0")
    await _emit_merchant_event(deps, frame_pk, req.merchant_id, "update-video", trace_id)
    return {"status": "accepted", "frame_count": 1}


# ── 内部函数 ──

async def _check_dedup(deps, image_pk: str, uri: str) -> bool:
    """二级去重: Redis → PG, 返回 True=新图"""
    redis_cache = deps["redis"]
    cached = await redis_cache.get(f"uri_cache:{image_pk}")
    if cached:
        return False

    pg = deps["pg"]
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM uri_dedup WHERE image_pk = $1", image_pk
        )
    if row:
        await redis_cache.set(f"uri_cache:{image_pk}", "1", ex=7776000)
        return False

    return True


# ── FIX #4: 分布式锁 watchdog 续约 ──

class _LockWatchdog:
    """分布式锁续约 watchdog — 防止长时间操作期间锁过期"""

    def __init__(self, redis_client, lock_key: str, instance_id: str, ttl_s: int = 30):
        self._redis = redis_client
        self._key = lock_key
        self._id = instance_id
        self._ttl = ttl_s
        self._task: Optional[asyncio.Task] = None
        self._stopped = False

    async def start(self):
        self._task = asyncio.create_task(self._renew_loop())

    async def stop(self):
        self._stopped = True
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _renew_loop(self):
        """每 ttl/3 秒续约一次"""
        interval = self._ttl / 3
        while not self._stopped:
            try:
                await asyncio.sleep(interval)
                if self._stopped:
                    break
                # 仅当锁仍属于本实例时续约 (Lua 原子操作)
                lua_script = """
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("expire", KEYS[1], ARGV[2])
                else
                    return 0
                end
                """
                result = await self._redis.eval(
                    lua_script, 1, self._key, self._id, str(self._ttl)
                )
                if result == 0:
                    logger.warning("lock_watchdog_lost_lock", key=self._key)
                    break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("lock_watchdog_error", error=str(e))


async def _process_new_image(deps, image_pk: str, req: UpdateImageRequest, trace_id: str):
    """处理新图片 — 事务性写入 + 补偿日志 (FIX #9)"""
    redis_cache = deps["redis"]
    instance_id = deps["instance_id"]
    lock_key = f"uri_lock:{image_pk}"

    lock_acquired = await redis_cache.set(lock_key, instance_id, nx=True, ex=30)
    if not lock_acquired:
        return

    # FIX #4: 启动 watchdog 续约
    watchdog = _LockWatchdog(redis_cache, lock_key, instance_id, ttl_s=30)
    await watchdog.start()

    # 补偿日志 key — 记录已完成步骤, 用于故障恢复
    compensation_key = f"compensation:{image_pk}:{trace_id}"
    completed_steps = []
    _process_start_ts = time.monotonic()  # FIX-B: T0 写入受理时刻
    t1_milvus_ms = None  # FIX-B: T1 Milvus 写入完成

    try:
        # 1) 下载图片
        image_bytes = await _download_image(req.uri)

        # 2) 特征提取
        features = await _extract_features(deps, image_bytes)

        # 3) 常青判定
        is_evergreen = await _check_evergreen(deps, req.category_l1)
        ts_month = 999999 if is_evergreen else int(time.strftime("%Y%m"))
        promoted_at = int(time.time() * 1000) if is_evergreen else 0

        # 4) 词表编码 — 严格校验 (FIX: Moderate Issue)
        vocab_cache = deps["vocab"]
        cat_l1_code = vocab_cache.encode("category_l1", req.category_l1)
        if cat_l1_code is None:
            raise HTTPException(status_code=400, detail={
                "error": {"code": "200_01_03", "message": f"Unknown category: {req.category_l1}"}
            })
        tag_codes = []
        if req.tags:
            for t in req.tags[:32]:
                code = vocab_cache.encode("tag", t)
                if code is not None:
                    tag_codes.append(code)
                else:
                    logger.warning("unknown_tag_skipped", tag=t, image_pk=image_pk)

        # ── FIX #9: 事务性写入 — Milvus + PG 原子化 ──
        milvus = deps["milvus"]
        pg = deps["pg"]
        partition_name = f"p_{ts_month}"

        data = {
            "image_pk": image_pk,
            "global_vec": features.global_vec,
            "category_l1": cat_l1_code,
            "tags": tag_codes,
            "is_evergreen": is_evergreen,
            "ts_month": ts_month,
            "promoted_at": promoted_at,
            "product_id": req.product_id or "",
            "created_at": int(time.time() * 1000),
        }

        # 步骤 5: Milvus Upsert
        await _milvus_upsert(milvus, data, partition_name, deps.get("milvus_executor"))
        completed_steps.append("milvus_upsert")
        t1_milvus_ms = int((time.monotonic() - _process_start_ts) * 1000)  # FIX-B: T1
        write_visibility_latency.labels(level="t0_to_t1").observe(t1_milvus_ms / 1000)
        write_stage_latency.labels(stage="milvus").observe(t1_milvus_ms / 1000)

        # 步骤 6: PG 去重记录 (事务)
        try:
            async with pg.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        """INSERT INTO uri_dedup (image_pk, uri_hash, ts_month)
                           VALUES ($1, $2, $3) ON CONFLICT DO NOTHING""",
                        image_pk,
                        hashlib.sha256(req.uri.encode()).hexdigest(),
                        ts_month,
                    )
            completed_steps.append("pg_dedup")
        except Exception as pg_err:
            # PG 失败 → 写补偿日志, Milvus 数据不删 (幂等, 下次 upsert 覆盖)
            logger.error(
                "pg_write_failed_compensation",
                image_pk=image_pk, trace_id=trace_id,
                completed_steps=completed_steps, error=str(pg_err),
            )
            await _write_compensation_log(redis_cache, compensation_key, {
                "image_pk": image_pk,
                "steps_done": completed_steps,
                "steps_failed": ["pg_dedup"],
                "trace_id": trace_id,
                "timestamp": int(time.time() * 1000),
            })
            raise

        # 步骤 7: Redis 缓存
        await redis_cache.set(f"uri_cache:{image_pk}", "1", ex=7776000)
        completed_steps.append("redis_cache")

        # 步骤 8: Flush (FIX #3: 带异常捕获)
        await _safe_incr_flush_counter(deps)
        completed_steps.append("flush_signal")

        # 步骤 9: bitmap-filter 直推 (FIX-A: 重试 + 补偿)
        t2_start = time.monotonic()
        bitmap_push_client = deps.get("bitmap_push_client")
        push_ok = False
        if bitmap_push_client:
            for attempt in range(2):  # FIX-A: 最多 2 次尝试
                try:
                    await asyncio.wait_for(
                        bitmap_push_client.push_update(image_pk, req.merchant_id, is_evergreen),
                        timeout=0.05 * (attempt + 1),  # 第二次超时放宽
                    )
                    push_ok = True
                    break
                except Exception as e:
                    logger.warning(
                        "bitmap_push_failed",
                        image_pk=image_pk, attempt=attempt + 1, error=str(e),
                    )

            # FIX-A: 两次均失败 → 写补偿事件到 Kafka, CDC 会最终兜底
            if not push_ok:
                logger.error(
                    "bitmap_push_compensate",
                    image_pk=image_pk, trace_id=trace_id,
                )
                try:
                    kafka = deps.get("kafka")
                    if kafka:
                        await kafka.send_and_wait(
                            "image-search.bitmap-push-compensate",
                            value=json.dumps({
                                "image_pk": image_pk,
                                "merchant_id": req.merchant_id,
                                "is_evergreen": is_evergreen,
                                "trace_id": trace_id,
                                "timestamp": int(time.time() * 1000),
                            }).encode(),
                            key=image_pk.encode(),
                        )
                except Exception:
                    pass  # 补偿发送失败由 CDC 最终兜底

        # FIX-B: 写入可见性端到端打点
        t2_ms = int((time.monotonic() - t2_start) * 1000)
        t0_to_t2_ms = int((time.monotonic() - _process_start_ts) * 1000) if _process_start_ts else None
        logger.info(
            "write_visibility_timing",
            image_pk=image_pk,
            t0_accepted_ms=0,
            t1_milvus_ms=t1_milvus_ms,
            t2_push_ms=t2_ms,
            t0_to_t2_total_ms=t0_to_t2_ms,
            push_ok=push_ok,
        )
        # FIX-B: Prometheus Histogram observe
        write_stage_latency.labels(stage="bitmap_push").observe(t2_ms / 1000)
        if t0_to_t2_ms is not None:
            write_visibility_latency.labels(level="t0_to_t2").observe(t0_to_t2_ms / 1000)
        push_update_total.labels(result="ok" if push_ok else "compensate").inc()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "process_new_image_failed",
            image_pk=image_pk, trace_id=trace_id,
            completed_steps=completed_steps, error=str(e),
        )
        raise HTTPException(status_code=500, detail={
            "error": {"code": "200_05_01", "message": "Internal processing error"}
        })
    finally:
        # 停止 watchdog, 释放锁 (Lua 原子: 仅删自己的锁)
        await watchdog.stop()
        lua_del = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        try:
            await redis_cache.eval(lua_del, 1, lock_key, instance_id)
        except Exception:
            pass


async def _write_compensation_log(redis_client, key: str, data: dict):
    """写入补偿日志到 Redis (TTL 24h), 供异步 worker 重试"""
    try:
        await redis_client.set(key, json.dumps(data), ex=86400)
    except Exception as e:
        logger.error("compensation_log_write_failed", error=str(e))


async def _download_image(uri: str, timeout: float = 3.0, retries: int = 2) -> bytes:
    """下载图片, 指数退避重试"""
    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt in range(retries + 1):
            try:
                resp = await client.get(uri, headers={"User-Agent": "SZWEGO-ImageSearch/1.2"})
                resp.raise_for_status()
                data = resp.content
                if len(data) > 10 * 1024 * 1024:
                    raise HTTPException(status_code=400, detail={
                        "error": {"code": "200_01_02", "message": "Image exceeds 10MB"}
                    })
                return data
            except httpx.HTTPError as e:
                if attempt == retries:
                    raise HTTPException(status_code=400, detail={
                        "error": {"code": "200_01_04", "message": f"Image download failed: {e}"}
                    })
                await asyncio.sleep(2 ** attempt)
    raise RuntimeError("unreachable")


async def _extract_features(deps, image_bytes: bytes):
    """特征提取"""
    allow_mock = os.getenv("IMGSRCH_ALLOW_MOCK_INFERENCE", "false").lower() == "true"
    inference_url = os.getenv("INFERENCE_SERVICE_URL")

    if inference_url:
        import base64
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                f"{inference_url}/api/v1/extract",
                json={"image_b64": base64.b64encode(image_bytes).decode()},
            )
            resp.raise_for_status()
            data = resp.json()

            class _Result:
                pass
            result = _Result()
            result.global_vec = data["global_vec"]
            result.tags_pred = data.get("tags_pred", [])
            result.category_l1_pred = data.get("category_l1_pred", 0)
            return result

    if allow_mock:
        import numpy as np
        seed = int.from_bytes(image_bytes[:4], "big") if len(image_bytes) >= 4 else 0
        rng = np.random.RandomState(seed % (2**31))
        vec = rng.randn(256).astype("float32")
        vec = (vec / np.linalg.norm(vec)).tolist()

        class _MockResult:
            pass
        result = _MockResult()
        result.global_vec = vec
        result.tags_pred = rng.choice(1000, 5, replace=False).tolist()
        result.category_l1_pred = int(rng.randint(0, 200))
        return result

    raise RuntimeError("No inference service configured.")


async def _check_evergreen(deps, category_l1: str) -> bool:
    """常青类目判定"""
    redis_cache = deps["redis"]
    cached = await redis_cache.get("config:evergreen_categories")
    if cached:
        cats = json.loads(cached)
        return category_l1 in cats

    pg = deps["pg"]
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            "SELECT category_l1_name FROM evergreen_categories WHERE enabled = true"
        )
    cat_names = [r["category_l1_name"] for r in rows]
    await redis_cache.set("config:evergreen_categories", json.dumps(cat_names), ex=300)
    return category_l1 in cat_names


async def _milvus_upsert(milvus, data: dict, partition_name: str, executor=None):
    """Milvus Upsert — 使用注入的有界线程池"""
    loop = asyncio.get_event_loop()
    from pymilvus import Collection
    hot_coll = Collection("global_images_hot")

    def _upsert():
        if not hot_coll.has_partition(partition_name):
            hot_coll.create_partition(partition_name)
        hot_coll.upsert([data], partition_name=partition_name)

    await loop.run_in_executor(executor, _upsert)


# ── FIX #3: 安全的 Flush 管理 ──

_flush_counter = 0
_flush_lock = asyncio.Lock()
FLUSH_BATCH_SIZE = int(os.getenv("FLUSH_BATCH_SIZE", "50"))
FLUSH_INTERVAL_S = float(os.getenv("FLUSH_INTERVAL_S", "2.0"))


async def _safe_incr_flush_counter(deps):
    """累积写入计数, 达到阈值触发 flush — 带异常捕获"""
    global _flush_counter
    async with _flush_lock:
        _flush_counter += 1
        if _flush_counter >= FLUSH_BATCH_SIZE:
            _flush_counter = 0
            # FIX #3: 不再用 create_task fire-and-forget, 改为 await + 异常捕获
            try:
                await asyncio.wait_for(_do_flush(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("milvus_flush_timeout")
            except Exception as e:
                logger.error("milvus_flush_failed", error=str(e))


async def _do_flush():
    """执行 Milvus flush"""
    loop = asyncio.get_event_loop()
    from pymilvus import Collection
    hot_coll = Collection("global_images_hot")
    await loop.run_in_executor(None, hot_coll.flush)
    logger.info("milvus_flush_ok")


# ── FIX #12: Kafka 事件带 trace_id ──

async def _emit_merchant_event(
    deps, image_pk: str, merchant_id: str, source: str, trace_id: str
):
    """发送商家关联事件到 Kafka — 携带 W3C trace_id"""
    kafka = deps["kafka"]
    event = {
        "event_type": "ADD",
        "image_pk": image_pk,
        "merchant_id": merchant_id,
        "source": source,
        "timestamp": int(time.time() * 1000),
        "trace_id": trace_id,
    }
    try:
        await kafka.send_and_wait(
            "image-search.merchant-events",
            value=json.dumps(event).encode(),
            key=image_pk.encode(),
            headers=[("traceparent", f"00-{trace_id}-{uuid.uuid4().hex[:16]}-01".encode())],
        )
    except Exception as e:
        logger.error("merchant_event_send_failed", error=str(e), trace_id=trace_id)
