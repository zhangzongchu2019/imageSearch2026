"""
特征提取 _extract_features() 测试
覆盖: 真实推理、mock 推理、无配置异常、HTTP 错误、超时
"""
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestExtractFeaturesInference:
    """真实推理服务路径"""

    @pytest.mark.asyncio
    async def test_inference_service_success(self):
        """INFERENCE_SERVICE_URL 配置时走 HTTP 推理"""
        from app.api.update_image import _extract_features

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "global_vec": [0.1] * 256,
            "tags_pred": [10, 20, 30],
            "category_l1_pred": 5,
        }

        with patch.dict(os.environ, {"INFERENCE_SERVICE_URL": "http://inference:8000"}):
            with patch("httpx.AsyncClient") as mock_client_cls:
                mock_client = AsyncMock()
                mock_client.post.return_value = mock_response
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=False)
                mock_client_cls.return_value = mock_client

                result = await _extract_features(None, b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)

        assert len(result.global_vec) == 256
        assert result.tags_pred == [10, 20, 30]
        assert result.category_l1_pred == 5

    @pytest.mark.asyncio
    async def test_inference_service_http_error(self):
        """推理服务返回 5xx → raise_for_status 抛异常"""
        import httpx

        from app.api.update_image import _extract_features

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Server Error", request=MagicMock(), response=mock_response
        )

        with patch.dict(os.environ, {"INFERENCE_SERVICE_URL": "http://inference:8000"}):
            with patch("httpx.AsyncClient") as mock_client_cls:
                mock_client = AsyncMock()
                mock_client.post.return_value = mock_response
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=False)
                mock_client_cls.return_value = mock_client

                with pytest.raises(httpx.HTTPStatusError):
                    await _extract_features(None, b"image_data")

    @pytest.mark.asyncio
    async def test_inference_missing_optional_fields(self):
        """推理服务返回只有 global_vec, tags_pred/category_l1_pred 缺省"""
        from app.api.update_image import _extract_features

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "global_vec": [0.5] * 256,
        }

        with patch.dict(os.environ, {"INFERENCE_SERVICE_URL": "http://inference:8000"}):
            with patch("httpx.AsyncClient") as mock_client_cls:
                mock_client = AsyncMock()
                mock_client.post.return_value = mock_response
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=False)
                mock_client_cls.return_value = mock_client

                result = await _extract_features(None, b"image_data")

        assert result.tags_pred == []
        assert result.category_l1_pred == 0


class TestExtractFeaturesMock:
    """Mock 推理路径"""

    @pytest.mark.asyncio
    async def test_mock_inference_enabled(self):
        """IMGSRCH_ALLOW_MOCK_INFERENCE=true → 生成确定性向量"""
        from app.api.update_image import _extract_features

        env = {"IMGSRCH_ALLOW_MOCK_INFERENCE": "true"}
        with patch.dict(os.environ, env, clear=False):
            # 确保没有 INFERENCE_SERVICE_URL
            os.environ.pop("INFERENCE_SERVICE_URL", None)
            result = await _extract_features(None, b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)

        assert len(result.global_vec) == 256
        assert len(result.tags_pred) == 5
        assert 0 <= result.category_l1_pred < 200

    @pytest.mark.asyncio
    async def test_mock_inference_deterministic(self):
        """相同 image_bytes → 相同特征向量 (确定性)"""
        from app.api.update_image import _extract_features

        image_bytes = b"\x89PNG\r\n\x1a\n" + b"\x42" * 100
        env = {"IMGSRCH_ALLOW_MOCK_INFERENCE": "true"}

        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("INFERENCE_SERVICE_URL", None)
            r1 = await _extract_features(None, image_bytes)
            r2 = await _extract_features(None, image_bytes)

        assert r1.global_vec == r2.global_vec
        assert r1.tags_pred == r2.tags_pred

    @pytest.mark.asyncio
    async def test_mock_vec_normalized(self):
        """Mock 向量 L2 归一化"""
        import math

        from app.api.update_image import _extract_features

        env = {"IMGSRCH_ALLOW_MOCK_INFERENCE": "true"}
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("INFERENCE_SERVICE_URL", None)
            result = await _extract_features(None, b"\x01\x02\x03\x04" * 30)

        norm = math.sqrt(sum(x * x for x in result.global_vec))
        assert abs(norm - 1.0) < 0.01


class TestExtractFeaturesNoConfig:
    """无配置异常路径"""

    @pytest.mark.asyncio
    async def test_no_config_raises_runtime_error(self):
        """无推理服务且 mock 未开启 → RuntimeError"""
        from app.api.update_image import _extract_features

        env = {"IMGSRCH_ALLOW_MOCK_INFERENCE": "false"}
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("INFERENCE_SERVICE_URL", None)
            with pytest.raises(RuntimeError, match="No inference service configured"):
                await _extract_features(None, b"image_data")
