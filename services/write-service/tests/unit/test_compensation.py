"""
补偿日志测试
"""
import json
from unittest.mock import AsyncMock

import pytest
import sys
sys.path.insert(0, ".")

from app.api.update_image import _write_compensation_log


class TestCompensationLog:
    @pytest.mark.asyncio
    async def test_log_format(self):
        """补偿日志 JSON 格式正确"""
        redis = AsyncMock()
        data = {
            "image_pk": "a" * 32,
            "steps_done": ["milvus_upsert"],
            "steps_failed": ["pg_dedup"],
            "trace_id": "abc123",
            "timestamp": 1700000000000,
        }
        await _write_compensation_log(redis, "compensation:key", data)
        stored_json = redis.set.call_args[0][1]
        parsed = json.loads(stored_json)
        assert parsed["image_pk"] == "a" * 32
        assert "steps_done" in parsed
        assert "steps_failed" in parsed
        assert "trace_id" in parsed

    @pytest.mark.asyncio
    async def test_ttl_24h(self):
        """补偿日志 TTL = 86400s (24h)"""
        redis = AsyncMock()
        await _write_compensation_log(redis, "comp:key", {"test": True})
        call_kwargs = redis.set.call_args[1]
        assert call_kwargs["ex"] == 86400

    @pytest.mark.asyncio
    async def test_write_failure_logged(self):
        """Redis 写入失败不抛异常 (仅 log)"""
        redis = AsyncMock()
        redis.set = AsyncMock(side_effect=Exception("Redis connection lost"))
        # Should not raise
        await _write_compensation_log(redis, "comp:key", {"test": True})
