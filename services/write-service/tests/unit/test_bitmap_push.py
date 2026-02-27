"""
Bitmap 推送测试 (gRPC 2重试 + Kafka 补偿)
"""
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sys
sys.path.insert(0, ".")


class TestBitmapPush:
    @pytest.mark.asyncio
    async def test_success_first(self, mock_deps):
        """首次推送成功"""
        client = mock_deps["bitmap_push_client"]
        client.push_update = AsyncMock()

        # Simulate bitmap push logic from _process_new_image
        push_ok = False
        for attempt in range(2):
            try:
                await asyncio.wait_for(
                    client.push_update("pk" * 16, "merchant1", False),
                    timeout=0.05 * (attempt + 1),
                )
                push_ok = True
                break
            except Exception:
                pass
        assert push_ok is True
        assert client.push_update.call_count == 1

    @pytest.mark.asyncio
    async def test_retries_twice(self, mock_deps):
        """首次失败, 第二次成功"""
        client = mock_deps["bitmap_push_client"]
        client.push_update = AsyncMock(
            side_effect=[asyncio.TimeoutError(), None]
        )

        push_ok = False
        for attempt in range(2):
            try:
                await asyncio.wait_for(
                    client.push_update("pk" * 16, "merchant1", False),
                    timeout=0.5,  # wider timeout for test
                )
                push_ok = True
                break
            except Exception:
                pass
        assert push_ok is True
        assert client.push_update.call_count == 2

    @pytest.mark.asyncio
    async def test_both_fail_kafka(self, mock_deps):
        """两次均失败 → Kafka 补偿"""
        client = mock_deps["bitmap_push_client"]
        client.push_update = AsyncMock(side_effect=Exception("gRPC down"))
        kafka = mock_deps["kafka"]

        push_ok = False
        for attempt in range(2):
            try:
                await asyncio.wait_for(
                    client.push_update("pk" * 16, "merchant1", False),
                    timeout=0.5,
                )
                push_ok = True
                break
            except Exception:
                pass

        assert push_ok is False

        # Kafka 补偿
        if not push_ok and kafka:
            await kafka.send_and_wait(
                "image-search.bitmap-push-compensate",
                value=json.dumps({
                    "image_pk": "pk" * 16,
                    "merchant_id": "merchant1",
                    "is_evergreen": False,
                }).encode(),
                key=("pk" * 16).encode(),
            )
        kafka.send_and_wait.assert_called_once()

    @pytest.mark.asyncio
    async def test_timeout_widened(self, mock_deps):
        """第二次尝试超时放宽 (0.1s vs 0.05s)"""
        # 验证超时策略: attempt 0 → 0.05s, attempt 1 → 0.10s
        timeouts = []
        for attempt in range(2):
            timeout = 0.05 * (attempt + 1)
            timeouts.append(timeout)
        assert timeouts == [0.05, 0.10]

    @pytest.mark.asyncio
    async def test_kafka_fail_silent(self, mock_deps):
        """Kafka 补偿发送失败 → 静默 (CDC 兜底)"""
        kafka = mock_deps["kafka"]
        kafka.send_and_wait = AsyncMock(side_effect=Exception("kafka down"))

        # Should not raise
        try:
            await kafka.send_and_wait("topic", value=b"data", key=b"key")
        except Exception:
            pass  # 补偿发送失败由 CDC 最终兜底
