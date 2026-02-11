"""
Redis 缓存封装 — URI 去重 / 字典编码 / 降级广播
"""
from __future__ import annotations

from typing import Optional

import structlog

logger = structlog.get_logger(__name__)


class RedisCache:
    """Redis 操作封装, 所有方法 fail-open (不阻塞主链路)"""

    def __init__(self, redis_client):
        self._r = redis_client

    async def get_uri_cache(self, image_pk: str) -> bool:
        """URI 去重第一级: 命中返回 True"""
        try:
            return await self._r.get(f"uri_cache:{image_pk}") is not None
        except Exception:
            return False

    async def set_uri_cache(self, image_pk: str, ttl_days: int = 90):
        try:
            await self._r.set(f"uri_cache:{image_pk}", "1", ex=ttl_days * 86400)
        except Exception as e:
            logger.warning("redis_set_uri_cache_failed", error=str(e))

    async def get_dict(self, merchant_id: str) -> Optional[int]:
        """字典编码二级: Redis"""
        try:
            val = await self._r.get(f"dict:merchant:{merchant_id}")
            return int(val) if val else None
        except Exception:
            return None

    async def set_dict(self, merchant_id: str, bitmap_index: int):
        try:
            await self._r.set(f"dict:merchant:{merchant_id}", str(bitmap_index), ex=86400)
        except Exception:
            pass

    async def acquire_lock(self, image_pk: str, instance_id: str, ttl_s: int = 30) -> bool:
        try:
            return await self._r.set(f"uri_lock:{image_pk}", instance_id, nx=True, ex=ttl_s)
        except Exception:
            return False

    async def release_lock(self, image_pk: str):
        try:
            await self._r.delete(f"uri_lock:{image_pk}")
        except Exception:
            pass
