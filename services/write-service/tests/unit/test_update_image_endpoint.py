"""
update_image / bind_merchant / update_video 端点处理器测试
覆盖: 请求校验、响应格式、编排流程、去重分支、异常处理
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
def mock_deps():
    """模拟完整依赖容器"""
    deps = MagicMock()
    deps.redis = AsyncMock()
    deps.pg = AsyncMock()
    deps.milvus = MagicMock()
    deps.kafka = AsyncMock()
    deps.vocab = MagicMock()
    deps.bitmap_push_client = AsyncMock()
    deps.instance_id = "test-pod-001"
    deps.milvus_executor = None
    return deps


class TestUpdateImageEndpoint:
    """POST /api/v1/image/update 端点"""

    @pytest.mark.asyncio
    async def test_new_image_returns_accepted(self, mock_deps):
        """新图片 → status=accepted, is_new=True"""
        from app.api.update_image import update_image

        mock_deps.redis.get = AsyncMock(return_value=None)
        mock_deps.pg.acquire = AsyncMock()

        mock_request = MagicMock()
        mock_request.app.state.deps = mock_deps

        req = MagicMock()
        req.uri = "https://cdn.test.com/img1.jpg"
        req.merchant_id = "m001"
        req.category_l1 = "shoes"
        req.product_id = None
        req.tags = []

        with patch("app.api.update_image._check_dedup", new_callable=AsyncMock) as mock_dedup:
            mock_dedup.return_value = (True, "abc123def456" * 2 + "abcdef00")
            with patch("app.api.update_image._process_new_image", new_callable=AsyncMock):
                with patch("app.api.update_image._emit_merchant_event", new_callable=AsyncMock):
                    result = await update_image(req, mock_request)

        assert result["status"] == "accepted"
        assert result["is_new"] is True

    @pytest.mark.asyncio
    async def test_duplicate_image_returns_not_new(self, mock_deps):
        """重复图片 → status=accepted, is_new=False, 不调 _process_new_image"""
        from app.api.update_image import update_image

        mock_request = MagicMock()
        mock_request.app.state.deps = mock_deps

        req = MagicMock()
        req.uri = "https://cdn.test.com/img_dup.jpg"
        req.merchant_id = "m002"
        req.category_l1 = "bags"
        req.product_id = None
        req.tags = []

        with patch("app.api.update_image._check_dedup", new_callable=AsyncMock) as mock_dedup:
            mock_dedup.return_value = (False, "abc123def456" * 2 + "abcdef00")
            with patch("app.api.update_image._process_new_image", new_callable=AsyncMock) as mock_process:
                with patch("app.api.update_image._emit_merchant_event", new_callable=AsyncMock):
                    result = await update_image(req, mock_request)

        assert result["is_new"] is False
        mock_process.assert_not_called()

    @pytest.mark.asyncio
    async def test_merchant_event_always_emitted(self, mock_deps):
        """无论新旧图片, merchant event 都发送"""
        from app.api.update_image import update_image

        mock_request = MagicMock()
        mock_request.app.state.deps = mock_deps

        req = MagicMock()
        req.uri = "https://cdn.test.com/img2.jpg"
        req.merchant_id = "m003"
        req.category_l1 = "shoes"
        req.product_id = None
        req.tags = []

        with patch("app.api.update_image._check_dedup", new_callable=AsyncMock) as mock_dedup:
            mock_dedup.return_value = (False, "abc123def456" * 2 + "abcdef00")
            with patch("app.api.update_image._process_new_image", new_callable=AsyncMock):
                with patch("app.api.update_image._emit_merchant_event", new_callable=AsyncMock) as mock_emit:
                    await update_image(req, mock_request)

        mock_emit.assert_called_once()


class TestBindMerchantEndpoint:
    """POST /api/v1/merchant/bindImage 端点"""

    @pytest.mark.asyncio
    async def test_bind_returns_accepted(self, mock_deps):
        """绑定成功 → status=accepted"""
        from app.api.update_image import bind_merchant

        mock_request = MagicMock()
        mock_request.app.state.deps = mock_deps

        req = MagicMock()
        req.image_id = "a" * 32
        req.merchant_id = "m_bind_001"

        with patch("app.api.update_image._emit_merchant_event", new_callable=AsyncMock) as mock_emit:
            result = await bind_merchant(req, mock_request)

        assert result["status"] == "accepted"
        assert result["image_id"] == "a" * 32
        mock_emit.assert_called_once()

    @pytest.mark.asyncio
    async def test_bind_emits_correct_source(self, mock_deps):
        """绑定事件 source=bind-merchant"""
        from app.api.update_image import bind_merchant

        mock_request = MagicMock()
        mock_request.app.state.deps = mock_deps

        req = MagicMock()
        req.image_id = "b" * 32
        req.merchant_id = "m_bind_002"

        with patch("app.api.update_image._emit_merchant_event", new_callable=AsyncMock) as mock_emit:
            await bind_merchant(req, mock_request)

        call_args = mock_emit.call_args
        assert call_args[0][3] == "bind-merchant"


class TestUpdateVideoEndpoint:
    """POST /api/v1/video/update 端点"""

    @pytest.mark.asyncio
    async def test_video_returns_accepted(self, mock_deps):
        """视频写入 → status=accepted, frame_count=1"""
        from app.api.update_image import update_video

        mock_request = MagicMock()
        mock_request.app.state.deps = mock_deps

        req = MagicMock()
        req.video_uri = "https://cdn.test.com/vid1.mp4"
        req.merchant_id = "m_vid_001"
        req.category_l1 = "shoes"
        req.product_id = None
        req.max_frames = 3

        with patch("app.api.update_image._emit_merchant_event", new_callable=AsyncMock):
            result = await update_video(req, mock_request)

        assert result["status"] == "accepted"
        assert result["frame_count"] == 1

    @pytest.mark.asyncio
    async def test_video_frame_pk_derived_from_uri(self, mock_deps):
        """frame_pk 由 video_uri + "_frame_0" 派生"""
        from app.api.update_image import _gen_image_pk, update_video

        mock_request = MagicMock()
        mock_request.app.state.deps = mock_deps

        req = MagicMock()
        req.video_uri = "https://cdn.test.com/vid2.mp4"
        req.merchant_id = "m_vid_002"

        expected_pk = _gen_image_pk(req.video_uri + "_frame_0")

        with patch("app.api.update_image._emit_merchant_event", new_callable=AsyncMock) as mock_emit:
            await update_video(req, mock_request)

        emitted_pk = mock_emit.call_args[0][1]
        assert emitted_pk == expected_pk

    @pytest.mark.asyncio
    async def test_video_emits_correct_source(self, mock_deps):
        """视频事件 source=update-video"""
        from app.api.update_image import update_video

        mock_request = MagicMock()
        mock_request.app.state.deps = mock_deps

        req = MagicMock()
        req.video_uri = "https://cdn.test.com/vid3.mp4"
        req.merchant_id = "m_vid_003"

        with patch("app.api.update_image._emit_merchant_event", new_callable=AsyncMock) as mock_emit:
            await update_video(req, mock_request)

        call_args = mock_emit.call_args
        assert call_args[0][3] == "update-video"
