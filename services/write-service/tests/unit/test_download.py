"""
图片下载 + 重试测试
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

import sys
sys.path.insert(0, ".")

from fastapi import HTTPException
from app.api.update_image import _download_image


class TestDownloadImage:
    @pytest.mark.asyncio
    async def test_success(self):
        """正常下载成功"""
        mock_resp = MagicMock()
        mock_resp.content = b"\x89PNG" + b"\x00" * 100
        mock_resp.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock()

        with patch("app.api.update_image.httpx.AsyncClient", return_value=mock_client):
            data = await _download_image("https://example.com/img.jpg")
        assert data == mock_resp.content

    @pytest.mark.asyncio
    async def test_retry_on_failure(self):
        """首次失败, 重试成功"""
        import httpx
        good_resp = MagicMock()
        good_resp.content = b"\x89PNG" + b"\x00" * 50
        good_resp.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(
            side_effect=[httpx.HTTPError("timeout"), good_resp]
        )
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock()

        with patch("app.api.update_image.httpx.AsyncClient", return_value=mock_client):
            with patch("app.api.update_image.asyncio.sleep", new_callable=AsyncMock):
                data = await _download_image("https://example.com/img.jpg")
        assert data == good_resp.content

    @pytest.mark.asyncio
    async def test_size_limit_10mb(self):
        """超过 10MB 返回 400"""
        mock_resp = MagicMock()
        mock_resp.content = b"\x00" * (10 * 1024 * 1024 + 1)
        mock_resp.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock()

        with patch("app.api.update_image.httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(HTTPException) as exc_info:
                await _download_image("https://example.com/big.jpg")
        assert exc_info.value.status_code == 400
        assert "10MB" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_all_retries_exhausted(self):
        """所有重试用尽 → HTTP 400"""
        import httpx
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=httpx.HTTPError("server error"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock()

        with patch("app.api.update_image.httpx.AsyncClient", return_value=mock_client):
            with patch("app.api.update_image.asyncio.sleep", new_callable=AsyncMock):
                with pytest.raises(HTTPException) as exc_info:
                    await _download_image("https://example.com/fail.jpg")
        assert exc_info.value.status_code == 400
