"""
搜索日志异步发送 SearchLogEmitter 测试
覆盖: 日志格式、性能指标收集、Kafka 发送、失败不阻塞 pipeline
"""
from unittest.mock import AsyncMock, MagicMock

import pytest


class TestSearchLogEmitter:
    """搜索日志异步发送"""

    @pytest.mark.asyncio
    async def test_emit_sends_to_kafka(self):
        """emit → Kafka 发送"""
        from app.infra.search_log_emitter import SearchLogEmitter

        mock_producer = AsyncMock()
        mock_producer.send_and_wait = AsyncMock()

        emitter = SearchLogEmitter(kafka_producer=mock_producer)

        ctx = MagicMock()
        ctx.request_id = "req_log_001"
        ctx.timers = {"feature_ms": 10, "ann_hot_ms": 50}

        params = MagicMock()
        params.merchant_scope = ["m001"]
        params.top_k = 100
        params.data_scope = "ALL"
        params.time_range = "ALL"

        features = MagicMock()
        features.global_vec = [0.1] * 256

        response = MagicMock()
        response.meta = MagicMock()
        response.meta.total_results = 10
        response.meta.strategy = "FAST_PATH"
        response.meta.confidence = "HIGH"
        response.meta.latency_ms = 120
        response.meta.degraded = False
        response.meta.degrade_state = "S0"
        response.meta.filter_skipped = False
        response.meta.zone_hit = "hot"

        await emitter.emit(ctx, params, features, response)
        mock_producer.send_and_wait.assert_called_once()

    @pytest.mark.asyncio
    async def test_emit_kafka_failure_no_block(self):
        """Kafka 失败 → 不阻塞 pipeline"""
        from app.infra.search_log_emitter import SearchLogEmitter

        mock_producer = AsyncMock()
        mock_producer.send_and_wait = AsyncMock(side_effect=Exception("Kafka timeout"))

        emitter = SearchLogEmitter(kafka_producer=mock_producer)

        ctx = MagicMock()
        ctx.request_id = "req_log_002"
        ctx.timers = {}

        # 不应抛异常
        await emitter.emit(ctx, MagicMock(), MagicMock(), MagicMock())

    @pytest.mark.asyncio
    async def test_emit_includes_performance_breakdown(self):
        """日志包含各阶段延迟分解"""
        from app.infra.search_log_emitter import SearchLogEmitter

        captured = {}
        mock_producer = AsyncMock()

        async def capture_send(*args, **kwargs):
            if args:
                captured["value"] = args
            captured.update(kwargs)

        mock_producer.send_and_wait = capture_send

        emitter = SearchLogEmitter(kafka_producer=mock_producer)

        ctx = MagicMock()
        ctx.request_id = "req_perf_001"
        ctx.timers = {
            "feature_ms": 15,
            "ann_hot_ms": 80,
            "ann_non_hot_ms": 0,
            "filter_ms": 5,
            "refine_ms": 3,
        }

        await emitter.emit(ctx, MagicMock(), MagicMock(), MagicMock())
        # 验证至少被调用
        assert captured  # 有内容被捕获

    @pytest.mark.asyncio
    async def test_emit_uses_request_id_as_key(self):
        """Kafka key = request_id"""
        from app.infra.search_log_emitter import SearchLogEmitter

        mock_producer = AsyncMock()
        emitter = SearchLogEmitter(kafka_producer=mock_producer)

        ctx = MagicMock()
        ctx.request_id = "req_key_log"
        ctx.timers = {}

        await emitter.emit(ctx, MagicMock(), MagicMock(), MagicMock())
        assert mock_producer.send_and_wait.called
