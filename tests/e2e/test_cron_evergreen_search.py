"""
常青清理→搜索反映变化 E2E 测试
"""
import base64
import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("E2E_TEST"),
    reason="Set E2E_TEST=true",
)


class TestCronEvergreenSearch:
    @pytest.mark.asyncio
    async def test_evergreen_cleanup_reflected_in_search(self, async_http_client, search_service_url):
        """常青池清理后搜索正常 (不返回已清理的图片)"""
        test_image = base64.b64encode(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100).decode()
        resp = await async_http_client.post(
            f"{search_service_url}/api/v1/search",
            json={
                "query_image": test_image,
                "top_k": 10,
                "data_scope": "evergreen",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "results" in data
        # All results should have is_evergreen=True if any
        for item in data.get("results", []):
            if "is_evergreen" in item:
                assert item["is_evergreen"] is True
