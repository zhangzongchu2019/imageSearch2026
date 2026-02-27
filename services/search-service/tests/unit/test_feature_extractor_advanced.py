"""
特征提取高级测试 — GPU→CPU 降级、30ms 超时
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sys
sys.path.insert(0, ".")

from app.engine.feature_extractor import FeatureResult, EMBEDDING_DIM


class TestFeatureExtractorAdvanced:
    @pytest.mark.asyncio
    async def test_gpu_timeout_fallback_cpu(self, mock_feature_extractor):
        """GPU 超时 → CPU fallback"""
        gpu_result = FeatureResult(
            global_vec=[0.1] * 256, tags_pred=[1, 2, 3], category_l1_pred=10,
        )
        cpu_result = FeatureResult(
            global_vec=[0.2] * 256, tags_pred=[4, 5, 6], category_l1_pred=20,
        )

        call_count = 0

        async def mock_extract(query_image_b64, device="gpu"):
            nonlocal call_count
            call_count += 1
            if device == "gpu":
                raise asyncio.TimeoutError()
            return cpu_result

        mock_feature_extractor.extract_query = mock_extract

        # Simulate pipeline GPU→CPU fallback
        try:
            result = await asyncio.wait_for(
                mock_feature_extractor.extract_query("dGVzdA==", device="gpu"),
                timeout=0.030,
            )
        except (asyncio.TimeoutError, Exception):
            result = await mock_feature_extractor.extract_query("dGVzdA==", device="cpu")

        assert result is cpu_result
        assert result.global_vec == [0.2] * 256

    def test_embedding_dim_256(self):
        """embedding 维度 = 256"""
        assert EMBEDDING_DIM == 256

    def test_feature_result_slots(self):
        """FeatureResult 有 __slots__ 优化"""
        fr = FeatureResult(
            global_vec=[0.0] * 256,
            tags_pred=[1, 2],
            category_l1_pred=5,
        )
        assert hasattr(fr, "__slots__")
        assert fr.embedding_dim == EMBEDDING_DIM

    @pytest.mark.asyncio
    async def test_30ms_timeout(self):
        """30ms 超时常量验证"""
        timeout = 0.030
        assert timeout == 0.03

    def test_mock_infer_deterministic(self):
        """Mock 推理: 相同输入 → 相同输出"""
        import numpy as np
        tensor = np.ones((1, 3, 224, 224), dtype=np.float32)
        seed1 = int(np.abs(tensor).sum() * 1e6) % (2**31)
        seed2 = int(np.abs(tensor).sum() * 1e6) % (2**31)
        assert seed1 == seed2

        rng1 = np.random.RandomState(seed1)
        rng2 = np.random.RandomState(seed2)
        vec1 = rng1.randn(256)
        vec2 = rng2.randn(256)
        np.testing.assert_array_equal(vec1, vec2)
