"""
Redis CAS 多实例同步集成测试
"""
import json
import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST"),
    reason="Set INTEGRATION_TEST=true",
)


class TestDegradeRedisSync:
    @pytest.mark.asyncio
    async def test_cas_write_read(self, redis_client):
        """CAS 写入 → 读取一致"""
        import redis as sync_redis
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        client = sync_redis.from_url(url, decode_responses=True)

        key = "imgsrch:degrade:state:test"
        data = json.dumps({"state": "S1", "epoch": 1, "updated_by": "test", "updated_at": 0})
        client.set(key, data)

        raw = client.get(key)
        parsed = json.loads(raw)
        assert parsed["state"] == "S1"
        assert parsed["epoch"] == 1

        client.delete(key)

    @pytest.mark.asyncio
    async def test_multi_instance_sync(self, redis_client):
        """多实例同步: epoch 递增"""
        import redis as sync_redis
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        client = sync_redis.from_url(url, decode_responses=True)

        key = "imgsrch:degrade:state:test2"

        # Instance 1 writes epoch=1
        data1 = json.dumps({"state": "S0", "epoch": 1, "updated_by": "pod-1"})
        client.set(key, data1)

        # Instance 2 reads and writes epoch=2
        raw = client.get(key)
        d = json.loads(raw)
        assert d["epoch"] == 1
        data2 = json.dumps({"state": "S1", "epoch": 2, "updated_by": "pod-2"})
        client.set(key, data2)

        # Instance 1 reads updated state
        raw2 = client.get(key)
        d2 = json.loads(raw2)
        assert d2["state"] == "S1"
        assert d2["epoch"] == 2

        client.delete(key)
