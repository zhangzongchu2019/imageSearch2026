"""
依赖注入容器 — 单例管理
"""
from __future__ import annotations

import grpc
import structlog

from app.core.config import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()


class BitmapFilterPushClient:
    """bitmap-filter-service 推送更新客户端 (单例)"""

    def __init__(self, target: str):
        self._target = target
        self._channel = grpc.aio.insecure_channel(target)

    async def close(self):
        await self._channel.close()
