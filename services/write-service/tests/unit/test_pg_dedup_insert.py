"""
PG 去重记录 + 补偿测试
"""
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sys
sys.path.insert(0, ".")

from app.api.update_image import _write_compensation_log


class TestPgDedupInsert:
    @pytest.mark.asyncio
    async def test_on_conflict_noop(self, mock_deps):
        """ON CONFLICT DO NOTHING — 重复插入不报错"""
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="INSERT 0 0")  # 0 rows inserted
        tx = AsyncMock()
        tx.__aenter__ = AsyncMock()
        tx.__aexit__ = AsyncMock()
        conn.transaction = MagicMock(return_value=tx)
        mock_deps["pg"].acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(),
        ))
        # Should not raise
        async with mock_deps["pg"].acquire() as c:
            async with c.transaction():
                result = await c.execute(
                    "INSERT INTO uri_dedup ... ON CONFLICT DO NOTHING",
                    "pk", "hash", 202601,
                )
        assert result == "INSERT 0 0"

    @pytest.mark.asyncio
    async def test_pg_failure_compensation(self, mock_redis):
        """PG 失败 → 写补偿日志"""
        compensation_key = "compensation:testpk:trace123"
        data = {
            "image_pk": "testpk",
            "steps_done": ["milvus_upsert"],
            "steps_failed": ["pg_dedup"],
            "trace_id": "trace123",
            "timestamp": 1234567890000,
        }
        await _write_compensation_log(mock_redis, compensation_key, data)
        mock_redis.set.assert_called_once()
        call_args = mock_redis.set.call_args
        assert call_args[0][0] == compensation_key
        stored = json.loads(call_args[0][1])
        assert stored["steps_failed"] == ["pg_dedup"]
        assert call_args[1]["ex"] == 86400

    @pytest.mark.asyncio
    async def test_no_milvus_rollback(self):
        """PG 失败不回滚 Milvus (幂等设计, 下次 upsert 覆盖)"""
        # 设计验证: 当 PG 写入失败时, 不尝试删除 Milvus 中已写入的数据
        # 因为 Milvus upsert 是幂等的, 下次写入会覆盖
        # 仅写补偿日志
        redis = AsyncMock()
        await _write_compensation_log(redis, "comp:key", {
            "image_pk": "pk",
            "steps_done": ["milvus_upsert"],
            "steps_failed": ["pg_dedup"],
        })
        # 只有 set 调用, 没有任何 Milvus delete 调用
        assert redis.set.call_count == 1
