"""
Kafka 商家事件发送测试
"""
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sys
sys.path.insert(0, ".")

from app.api.update_image import _emit_merchant_event, _gen_trace_id


class TestKafkaEvent:
    @pytest.mark.asyncio
    async def test_event_format(self, mock_deps):
        """事件 JSON 格式: event_type, image_pk, merchant_id, source, timestamp, trace_id"""
        kafka = mock_deps["kafka"]
        trace_id = "a" * 32

        await _emit_merchant_event(mock_deps, "pk" * 16, "m001", "update-image", trace_id)

        kafka.send_and_wait.assert_called_once()
        call_args = kafka.send_and_wait.call_args
        assert call_args[0][0] == "image-search.merchant-events"
        event = json.loads(call_args[1]["value"])
        assert event["event_type"] == "ADD"
        assert event["image_pk"] == "pk" * 16
        assert event["merchant_id"] == "m001"
        assert event["source"] == "update-image"
        assert event["trace_id"] == trace_id
        assert "timestamp" in event

    @pytest.mark.asyncio
    async def test_traceparent_header(self, mock_deps):
        """W3C traceparent header 格式"""
        kafka = mock_deps["kafka"]
        trace_id = "b" * 32

        await _emit_merchant_event(mock_deps, "pk" * 16, "m002", "bind-merchant", trace_id)

        call_args = kafka.send_and_wait.call_args
        headers = call_args[1]["headers"]
        assert len(headers) == 1
        name, value = headers[0]
        assert name == "traceparent"
        assert value.decode().startswith(f"00-{trace_id}-")

    @pytest.mark.asyncio
    async def test_send_failure_no_raise(self, mock_deps):
        """Kafka 发送失败不抛异常"""
        mock_deps["kafka"].send_and_wait = AsyncMock(side_effect=Exception("kafka down"))
        # Should not raise
        await _emit_merchant_event(mock_deps, "pk" * 16, "m003", "test", "c" * 32)

    def test_trace_id_format(self):
        """trace_id 是 32 字符 hex"""
        tid = _gen_trace_id()
        assert len(tid) == 32
        int(tid, 16)  # should not raise

    @pytest.mark.asyncio
    async def test_key_is_image_pk(self, mock_deps):
        """Kafka key = image_pk (保证分区顺序)"""
        kafka = mock_deps["kafka"]
        await _emit_merchant_event(mock_deps, "pk" * 16, "m004", "test", "d" * 32)
        call_args = kafka.send_and_wait.call_args
        assert call_args[1]["key"] == ("pk" * 16).encode()
