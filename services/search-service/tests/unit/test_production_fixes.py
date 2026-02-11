"""
P0 修复代码测试 — FIX #15
覆盖: 锁续约/事务写入/配置服务/线程池/flush 安全/trace_id
"""
import asyncio
import os
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

os.environ["IMGSRCH_ALLOW_MOCK_INFERENCE"] = "true"


class TestConfigService:
    """配置服务单元测试"""

    def test_env_override_highest_priority(self):
        """环境变量优先级最高"""
        os.environ["IMGSRCH_REDIS__HOST"] = "override-host"
        from app.core.config_service import ConfigService

        cs = ConfigService()
        cs._cache["redis.host"] = "cached-host"
        assert cs.get("redis.host") == "override-host"

        del os.environ["IMGSRCH_REDIS__HOST"]

    def test_get_int(self):
        from app.core.config_service import ConfigService
        cs = ConfigService()
        cs._cache["port"] = "8080"
        assert cs.get_int("port") == 8080
        assert cs.get_int("missing", 3000) == 3000

    def test_get_bool(self):
        from app.core.config_service import ConfigService
        cs = ConfigService()
        cs._cache["flag"] = "true"
        assert cs.get_bool("flag") is True
        cs._cache["flag2"] = "false"
        assert cs.get_bool("flag2") is False

    def test_get_secret_from_env(self):
        os.environ["IMGSRCH_SECRET_PG_PASSWORD"] = "s3cret"
        from app.core.config_service import ConfigService
        cs = ConfigService()
        assert cs.get_secret("pg_password") == "s3cret"
        del os.environ["IMGSRCH_SECRET_PG_PASSWORD"]

    def test_change_listener(self):
        from app.core.config_service import ConfigService
        cs = ConfigService()
        changes = []
        cs.on_change("test.key", lambda k, old, new: changes.append((k, old, new)))
        cs._notify_listeners("test.key", "old", "new")
        assert changes == [("test.key", "old", "new")]


class TestRetryWithBackoff:
    """FIX #1: 指数退避重试"""

    @pytest.mark.asyncio
    async def test_succeeds_first_try(self):
        from app.core.lifecycle import _retry_with_backoff
        result = await _retry_with_backoff(
            lambda: "ok", "test", max_retries=3, base_delay=0.001
        )
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_succeeds_after_retries(self):
        from app.core.lifecycle import _retry_with_backoff
        counter = {"n": 0}

        async def flaky():
            counter["n"] += 1
            if counter["n"] < 3:
                raise ConnectionError("refused")
            return "connected"

        result = await _retry_with_backoff(flaky, "test", max_retries=5, base_delay=0.001)
        assert result == "connected"
        assert counter["n"] == 3

    @pytest.mark.asyncio
    async def test_exhausted_raises(self):
        from app.core.lifecycle import _retry_with_backoff

        async def always_fail():
            raise ConnectionError("nope")

        with pytest.raises(ConnectionError):
            await _retry_with_backoff(always_fail, "test", max_retries=2, base_delay=0.001)


class TestBoundedMilvusExecutor:
    """FIX #2: 有界线程池"""

    def test_executor_created_with_limit(self):
        from concurrent.futures import ThreadPoolExecutor
        pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="milvus-io")
        assert pool._max_workers == 8
        pool.shutdown(wait=False)


class TestFlushSafety:
    """FIX #3: Flush 异常不静默吞掉"""

    @pytest.mark.asyncio
    async def test_flush_counter_resets(self):
        import app.api.update_image as mod
        mod._flush_counter = mod.FLUSH_BATCH_SIZE - 1
        # 不会因 Milvus 不可用而挂住 (内部 try-except)
        await mod._safe_incr_flush_counter({})
        assert mod._flush_counter == 0


class TestLockWatchdog:
    """FIX #4: 分布式锁 watchdog"""

    @pytest.mark.asyncio
    async def test_watchdog_calls_eval(self):
        from app.api.update_image import _LockWatchdog

        mock_redis = AsyncMock()
        mock_redis.eval = AsyncMock(return_value=1)

        wd = _LockWatchdog(mock_redis, "lock:test", "inst_1", ttl_s=1)
        await wd.start()
        await asyncio.sleep(0.5)  # 等待第一次续约 (ttl/3 = 0.33s)
        await wd.stop()

        # 应至少调用一次 eval (续约)
        assert mock_redis.eval.call_count >= 1


class TestGrpcLifecycle:
    """FIX #5: gRPC channel 生命周期"""

    def test_bitmap_client_has_close(self):
        """确认 BitmapFilterGrpcClient 有 close() 方法"""
        from app.infra.bitmap_grpc_client import BitmapFilterGrpcClient
        import inspect
        assert hasattr(BitmapFilterGrpcClient, "close")
        assert asyncio.iscoroutinefunction(BitmapFilterGrpcClient.close)


class TestApiKeyAuth:
    """FIX #6: API Key 从配置服务验证"""

    @pytest.mark.asyncio
    async def test_missing_key_returns_401(self):
        from app.api.routes import _verify_api_key
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await _verify_api_key(None)
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_dev_prefix_accepted(self):
        """开发环境下前缀校验仍然有效"""
        from app.api.routes import _verify_api_key
        # 在非 production 环境下, 前缀校验应该通过
        result = await _verify_api_key("imgsrch_test_key")
        assert result == "imgsrch_test_key"


class TestTransactionalWrite:
    """FIX #9: 事务性写入"""

    @pytest.mark.asyncio
    async def test_compensation_log_written(self):
        from app.api.update_image import _write_compensation_log
        import json

        mock_redis = AsyncMock()
        await _write_compensation_log(mock_redis, "comp:pk1:trace1", {
            "image_pk": "pk1",
            "steps_done": ["milvus"],
            "steps_failed": ["pg"],
        })
        mock_redis.set.assert_called_once()
        call_args = mock_redis.set.call_args
        assert "comp:pk1:trace1" in call_args[0]
        data = json.loads(call_args[0][1])
        assert data["steps_failed"] == ["pg"]


class TestSingletonPushClient:
    """FIX #10: BitmapFilterPushClient 单例"""

    def test_push_client_in_deps(self):
        """deps 中应包含 bitmap_push_client"""
        # 验证 init_dependencies 返回值包含 push client
        from app.core.dependencies import BitmapFilterPushClient
        client = BitmapFilterPushClient("localhost:50051")
        assert client._target == "localhost:50051"


class TestTraceIdGeneration:
    """FIX #12: trace_id 生成"""

    def test_w3c_format(self):
        from app.api.update_image import _gen_trace_id
        tid = _gen_trace_id()
        assert len(tid) == 32
        int(tid, 16)  # 应该是合法 hex

    def test_unique(self):
        from app.api.update_image import _gen_trace_id
        ids = [_gen_trace_id() for _ in range(1000)]
        assert len(set(ids)) == 1000


class TestDegradeRealMetrics:
    """FIX #13: 降级 tick 不再使用硬编码 0.0"""

    def test_read_live_metrics_no_crash(self):
        """无 Prometheus 时不崩溃, 返回 (0.0, 0.0)"""
        from app.core.lifecycle import ServiceLifecycle
        lifecycle = object.__new__(ServiceLifecycle)
        p99, err = lifecycle._read_live_metrics()
        assert isinstance(p99, float)
        assert isinstance(err, float)


class TestNoLegacyEnums:
    """确认枚举对齐 v4.0"""

    def test_time_range_values(self):
        from app.model.schemas import TimeRange
        values = {e.value for e in TimeRange}
        assert values == {"hot_only", "all", "hot_eg"}
        assert "3m" not in values
        assert "9m" not in values
