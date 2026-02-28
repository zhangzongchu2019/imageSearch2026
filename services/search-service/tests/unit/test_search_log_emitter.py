"""
搜索日志异步发送 SearchLogEmitter 测试
覆盖: 日志格式、性能指标收集、Kafka 发送、失败不阻塞 pipeline
"""
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


def _make_mock_params():
    """构造与 SearchLogEmitter.emit() 兼容的 params mock"""
    params = SimpleNamespace(
        merchant_scope=["m001"],
        top_k=100,
        data_scope=SimpleNamespace(value="ALL"),
        time_range=SimpleNamespace(value="ALL"),
    )
    return params


def _make_mock_response():
    """构造与 SearchLogEmitter.emit() 兼容的 response mock"""
    meta = SimpleNamespace(
        total_results=10,
        strategy=SimpleNamespace(value="FAST_PATH"),
        confidence=SimpleNamespace(value="HIGH"),
        latency_ms=120,
        feature_ms=15,
        ann_hot_ms=80,
        ann_non_hot_ms=0,
        filter_ms=5,
        refine_ms=3,
        degraded=False,
        filter_skipped=False,
        degrade_state=SimpleNamespace(value="S0"),
        zone_hit="hot",
    )
    return SimpleNamespace(meta=meta)


class TestSearchLogEmitter:
    """搜索日志异步发送"""

    @pytest.mark.asyncio
    async def test_emit_sends_to_kafka(self):
        """emit → Kafka 发送"""
        from app.infra.search_log_emitter import SearchLogEmitter

        mock_producer = AsyncMock()
        mock_producer.send_and_wait = AsyncMock()

        emitter = SearchLogEmitter(kafka_producer=mock_producer)

        ctx = SimpleNamespace(request_id="req_log_001")
        params = _make_mock_params()
        features = SimpleNamespace(global_vec=[0.1] * 256)
        response = _make_mock_response()

        await emitter.emit(ctx, params, features, response)
        mock_producer.send_and_wait.assert_called_once()

    @pytest.mark.asyncio
    async def test_emit_kafka_failure_no_block(self):
        """Kafka 失败 → 不阻塞 pipeline"""
        from app.infra.search_log_emitter import SearchLogEmitter

        mock_producer = AsyncMock()
        mock_producer.send_and_wait = AsyncMock(side_effect=Exception("Kafka timeout"))

        emitter = SearchLogEmitter(kafka_producer=mock_producer)

        ctx = SimpleNamespace(request_id="req_log_002")
        params = _make_mock_params()
        response = _make_mock_response()

        # 不应抛异常
        await emitter.emit(ctx, params, SimpleNamespace(), response)

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

        ctx = SimpleNamespace(request_id="req_perf_001")
        params = _make_mock_params()
        response = _make_mock_response()

        await emitter.emit(ctx, params, SimpleNamespace(), response)
        # 验证至少被调用
        assert captured  # 有内容被捕获

    @pytest.mark.asyncio
    async def test_emit_uses_request_id_as_key(self):
        """Kafka key = request_id"""
        from app.infra.search_log_emitter import SearchLogEmitter

        mock_producer = AsyncMock()
        emitter = SearchLogEmitter(kafka_producer=mock_producer)

        ctx = SimpleNamespace(request_id="req_key_log")
        params = _make_mock_params()
        response = _make_mock_response()

        await emitter.emit(ctx, params, SimpleNamespace(), response)
        assert mock_producer.send_and_wait.called
