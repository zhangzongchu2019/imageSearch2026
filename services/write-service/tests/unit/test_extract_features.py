"""
特征提取 _extract_features() 测试
覆盖: 远程推理服务成功、HTTP 错误、缺省字段、无配置异常
"""
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestExtractFeaturesInference:
    """远程推理服务路径"""

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

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response

        with patch.dict(os.environ, {"INFERENCE_SERVICE_URL": "http://inference:8090"}):
            deps = {"inference_client": mock_client}
            result = await _extract_features(deps, b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)

        assert len(result.global_vec) == 256
        assert result.tags_pred == [10, 20, 30]
        assert result.category_l1_pred == 5
        mock_client.post.assert_called_once()

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

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response

        with patch.dict(os.environ, {"INFERENCE_SERVICE_URL": "http://inference:8090"}):
            deps = {"inference_client": mock_client}
            with pytest.raises(httpx.HTTPStatusError):
                await _extract_features(deps, b"image_data")

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

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response

        with patch.dict(os.environ, {"INFERENCE_SERVICE_URL": "http://inference:8090"}):
            deps = {"inference_client": mock_client}
            result = await _extract_features(deps, b"image_data")

        assert result.tags_pred == []
        assert result.category_l1_pred == 0


class TestExtractFeaturesNoConfig:
    """无配置异常路径"""

    @pytest.mark.asyncio
    async def test_no_inference_client_raises_runtime_error(self):
        """deps 中无 inference_client → RuntimeError"""
        from app.api.update_image import _extract_features

        deps = {}
        with pytest.raises(RuntimeError, match="No inference_client"):
            await _extract_features(deps, b"image_data")

    @pytest.mark.asyncio
    async def test_none_inference_client_raises_runtime_error(self):
        """inference_client=None → RuntimeError"""
        from app.api.update_image import _extract_features

        deps = {"inference_client": None}
        with pytest.raises(RuntimeError, match="No inference_client"):
            await _extract_features(deps, b"image_data")
