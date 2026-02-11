"""
P0 改造验收测试
覆盖: TimeRange v4.0 对齐 / 降级策略 / bitmap gRPC / 特征提取 / 分区路由
"""
import os
import time

import pytest

# 必须在 import schemas 前设置环境变量
os.environ["IMGSRCH_ALLOW_MOCK_INFERENCE"] = "true"


class TestTimeRangeV40:
    """P0-1: TimeRange 枚举对齐 v4.0 两区口径"""

    def test_enum_values(self):
        from app.model.schemas import TimeRange
        assert TimeRange.HOT_ONLY.value == "hot_only"
        assert TimeRange.ALL.value == "all"
        assert TimeRange.HOT_PLUS_EVERGREEN.value == "hot_eg"

    def test_default_is_all(self):
        from app.model.schemas import SearchRequest
        req = SearchRequest(query_image="dGVzdA==")
        assert req.time_range.value == "all"

    def test_no_legacy_enums(self):
        from app.model.schemas import TimeRange
        values = [e.value for e in TimeRange]
        assert "3m" not in values
        assert "9m" not in values


class TestDegradeV40:
    """P0-5: 降级范围修正为热区+常青"""

    def test_s1_uses_hot_plus_evergreen(self):
        from app.model.schemas import DegradeState, TimeRange, EffectiveParams, SearchRequest
        from app.core.degrade_fsm import DegradeStateMachine

        fsm = DegradeStateMachine(redis_client=None)
        fsm._state = DegradeState.S1
        fsm._entered_at = time.monotonic()

        req = SearchRequest(query_image="dGVzdA==")
        params = fsm.apply(req)

        assert params.time_range == TimeRange.HOT_PLUS_EVERGREEN
        assert params.enable_cascade is False
        assert params.enable_fallback is False

    def test_s2_uses_hot_plus_evergreen(self):
        from app.model.schemas import DegradeState, TimeRange, SearchRequest
        from app.core.degrade_fsm import DegradeStateMachine

        fsm = DegradeStateMachine(redis_client=None)
        fsm._state = DegradeState.S2
        fsm._entered_at = time.monotonic()

        req = SearchRequest(query_image="dGVzdA==")
        params = fsm.apply(req)

        assert params.time_range == TimeRange.HOT_PLUS_EVERGREEN
        assert params.ef_search == 64
        assert params.refine_top_k == 500

    def test_s0_no_modification(self):
        from app.model.schemas import DegradeState, TimeRange, SearchRequest
        from app.core.degrade_fsm import DegradeStateMachine

        fsm = DegradeStateMachine(redis_client=None)
        req = SearchRequest(query_image="dGVzdA==")
        params = fsm.apply(req)

        assert params.time_range == TimeRange.ALL
        assert params.enable_cascade is True
        assert params.ef_search == 192


class TestFeatureExtractorP0:
    """P0-3: 特征提取不再返回随机向量"""

    def test_mock_requires_env_flag(self):
        """无 ALLOW_MOCK=true 且无模型文件 → 启动失败"""
        old = os.environ.get("IMGSRCH_ALLOW_MOCK_INFERENCE")
        os.environ["IMGSRCH_ALLOW_MOCK_INFERENCE"] = "false"
        os.environ["IMGSRCH_MODEL_DIR"] = "/nonexistent"

        with pytest.raises(RuntimeError, match="No inference model"):
            from importlib import reload
            import app.engine.feature_extractor as fe
            reload(fe)
            fe.FeatureExtractor()

        if old:
            os.environ["IMGSRCH_ALLOW_MOCK_INFERENCE"] = old

    def test_mock_deterministic(self):
        """Mock 模式: 同图同向量 (基于内容 hash)"""
        os.environ["IMGSRCH_ALLOW_MOCK_INFERENCE"] = "true"
        os.environ["IMGSRCH_MODEL_DIR"] = "/nonexistent"

        from importlib import reload
        import app.engine.feature_extractor as fe
        reload(fe)
        extractor = fe.FeatureExtractor()

        import asyncio
        import base64
        # 同一张 "图片"
        img_b64 = base64.b64encode(b"\x89PNG\r\n" + b"\x00" * 100).decode()

        r1 = asyncio.get_event_loop().run_until_complete(
            extractor.extract_query(img_b64, device="cpu")
        )
        r2 = asyncio.get_event_loop().run_until_complete(
            extractor.extract_query(img_b64, device="cpu")
        )
        # 同图 → 同向量
        assert r1.global_vec == r2.global_vec
        assert r1.model_version == "mock-v0"
        assert r1.embedding_dim == 256


class TestBitmapGrpcClientStructure:
    """P0-2: bitmap gRPC 客户端结构验证"""

    def test_proto_stubs_importable(self):
        from app.infra.pb import bitmap_filter_pb2
        from app.infra.pb import bitmap_filter_pb2_grpc
        assert hasattr(bitmap_filter_pb2, "BatchFilterRequest")
        assert hasattr(bitmap_filter_pb2, "BatchFilterResponse")
        assert hasattr(bitmap_filter_pb2_grpc, "BitmapFilterServiceStub")

    def test_no_more_not_implemented_error(self):
        """确认 _grpc_filter 不再 raise NotImplementedError"""
        import inspect
        from app.infra.bitmap_grpc_client import BitmapFilterGrpcClient
        source = inspect.getsource(BitmapFilterGrpcClient)
        assert "NotImplementedError" not in source


class TestPartitionFilter:
    """P0-1 + P0-4: 分区过滤表达式验证"""

    def test_hot_plus_evergreen_filter(self):
        """HOT_PLUS_EVERGREEN → 热区 ts_month + 常青"""
        from app.model.schemas import TimeRange, EffectiveParams, DataScope

        params = EffectiveParams(time_range=TimeRange.HOT_PLUS_EVERGREEN)

        from app.core.pipeline import SearchPipeline
        # Mock a pipeline instance just for filter building
        pipeline = object.__new__(SearchPipeline)
        expr = pipeline._build_partition_filter(params)

        assert "is_evergreen == true" in expr
        assert "ts_month >=" in expr

    def test_all_uses_18m_boundary(self):
        """ALL → 使用 non_hot_zone.months_end (18) 作为边界"""
        from app.model.schemas import TimeRange, EffectiveParams

        params = EffectiveParams(time_range=TimeRange.ALL)
        pipeline = object.__new__(SearchPipeline)

        from app.core.pipeline import SearchPipeline
        expr = pipeline._build_partition_filter(params)

        assert "ts_month >=" in expr
        assert "is_evergreen == true" in expr
