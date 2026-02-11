"""
搜索日志异步写入 Kafka — 不阻塞主链路
"""
from __future__ import annotations

import json
import time

import structlog

from app.core.config import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()


class SearchLogEmitter:
    def __init__(self, kafka_producer):
        self._producer = kafka_producer
        self._topic = settings.kafka.search_logs_topic

    async def emit(self, ctx, params, features, response):
        """异步发送搜索日志到 Kafka"""
        try:
            log_entry = {
                "request_id": ctx.request_id,
                "version": {
                    "pipeline_version": settings.system.version,
                },
                "params": {
                    "merchant_scope_size": len(params.merchant_scope) if params.merchant_scope else 0,
                    "top_k": params.top_k,
                    "data_scope": params.data_scope.value,
                    "time_range": params.time_range.value,
                },
                "results": {
                    "count": response.meta.total_results,
                    "strategy": response.meta.strategy.value,
                    "confidence": response.meta.confidence.value,
                },
                "performance": {
                    "total_ms": response.meta.latency_ms,
                    "feature_ms": response.meta.feature_ms,
                    "ann_hot_ms": response.meta.ann_hot_ms,
                    "ann_non_hot_ms": response.meta.ann_non_hot_ms,
                    "filter_ms": response.meta.filter_ms,
                    "refine_ms": response.meta.refine_ms,
                },
                "meta": {
                    "degraded": response.meta.degraded,
                    "filter_skipped": response.meta.filter_skipped,
                    "degrade_state": response.meta.degrade_state.value,
                    "zone_hit": response.meta.zone_hit,
                },
                "timestamp": int(time.time() * 1000),
            }
            await self._producer.send_and_wait(
                self._topic,
                value=json.dumps(log_entry).encode(),
                key=ctx.request_id.encode(),
            )
        except Exception as e:
            # 日志发送失败不阻塞主链路
            logger.warning("search_log_emit_failed", error=str(e))
