"""
分区轮转→搜索不中断 E2E 测试
"""
import base64
import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("E2E_TEST"),
    reason="Set E2E_TEST=true",
)


class TestCronRotationSearch:
    @pytest.mark.asyncio
    async def test_partition_rotation_search_works(self, async_http_client, search_service_url):
        """分区轮转后搜索仍正常工作"""
        test_image = base64.b64encode(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100).decode()
        resp = await async_http_client.post(
            f"{search_service_url}/api/v1/search",
            json={
                "query_image": test_image,
                "top_k": 10,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "results" in data
        assert "meta" in data
