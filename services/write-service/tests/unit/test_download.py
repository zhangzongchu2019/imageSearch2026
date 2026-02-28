"""
图片下载 + 重试测试
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from contextlib import asynccontextmanager

import sys
sys.path.insert(0, ".")

from fastapi import HTTPException
from app.api.update_image import _download_image


def _make_async_client_mock(get_side_effect=None, get_return_value=None):
    """创建能正确用于 async with 的 httpx.AsyncClient mock"""
    inner_client = AsyncMock()
    if get_side_effect is not None:
        inner_client.get = AsyncMock(side_effect=get_side_effect)
    elif get_return_value is not None:
        inner_client.get = AsyncMock(return_value=get_return_value)

    @asynccontextmanager
    async def _ctx(*args, **kwargs):
        yield inner_client

    return _ctx, inner_client


class TestDownloadImage:
    @pytest.mark.asyncio
    async def test_success(self):
        """正常下载成功"""
        mock_resp = MagicMock()
        mock_resp.content = b"\x89PNG" + b"\x00" * 100
        mock_resp.raise_for_status = MagicMock()

        ctx_factory, _ = _make_async_client_mock(get_return_value=mock_resp)

        with patch("app.api.update_image.httpx.AsyncClient", ctx_factory):
            data = await _download_image("https://example.com/img.jpg")
        assert data == mock_resp.content

    @pytest.mark.asyncio
    async def test_retry_on_failure(self):
        """首次失败, 重试成功"""
        import httpx
        good_resp = MagicMock()
        good_resp.content = b"\x89PNG" + b"\x00" * 50
        good_resp.raise_for_status = MagicMock()

        ctx_factory, _ = _make_async_client_mock(
            get_side_effect=[httpx.HTTPError("timeout"), good_resp]
        )

        with patch("app.api.update_image.httpx.AsyncClient", ctx_factory):
            with patch("app.api.update_image.asyncio.sleep", new_callable=AsyncMock):
                data = await _download_image("https://example.com/img.jpg")
        assert data == good_resp.content

    @pytest.mark.asyncio
    async def test_size_limit_10mb(self):
        """超过 10MB 返回 400"""
        mock_resp = MagicMock()
        mock_resp.content = b"\x00" * (10 * 1024 * 1024 + 1)
        mock_resp.raise_for_status = MagicMock()

        ctx_factory, _ = _make_async_client_mock(get_return_value=mock_resp)

        with patch("app.api.update_image.httpx.AsyncClient", ctx_factory):
            with pytest.raises(HTTPException) as exc_info:
                await _download_image("https://example.com/big.jpg")
        assert exc_info.value.status_code == 400
        assert "10MB" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_all_retries_exhausted(self):
        """所有重试用尽 → HTTP 400"""
        import httpx

        ctx_factory, _ = _make_async_client_mock(
            get_side_effect=httpx.HTTPError("server error")
        )

        with patch("app.api.update_image.httpx.AsyncClient", ctx_factory):
            with patch("app.api.update_image.asyncio.sleep", new_callable=AsyncMock):
                with pytest.raises(HTTPException) as exc_info:
                    await _download_image("https://example.com/fail.jpg")
        assert exc_info.value.status_code == 400
