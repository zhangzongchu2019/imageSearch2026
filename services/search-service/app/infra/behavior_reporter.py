"""
行为上报 — 可选能力, 写入失败不阻塞主链路
"""
from __future__ import annotations

import json
import time

import structlog

from app.core.config import get_settings
from app.model.schemas import BehaviorReportRequest

logger = structlog.get_logger(__name__)
settings = get_settings()


class BehaviorReporter:
    def __init__(self, kafka_producer):
        self._producer = kafka_producer
        self._topic = "image-search.behavior-events"

    async def report(self, req: BehaviorReportRequest):
        event = {
            "event_type": req.event_type,
            "request_id": req.request_id,
            "image_id": req.image_id,
            "position": req.position,
            "timestamp": int(time.time() * 1000),
        }
        try:
            await self._producer.send_and_wait(
                self._topic,
                value=json.dumps(event).encode(),
                key=req.request_id.encode(),
            )
        except Exception as e:
            logger.warning("behavior_report_kafka_failed", error=str(e))
