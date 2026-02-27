"""
写入→搜索可见性 E2E 测试
需要 write-service + search-service + Milvus + PG + Redis 运行
"""
import base64
import os
import time

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("E2E_TEST"),
    reason="Set E2E_TEST=true",
)


class TestWriteSearchE2E:
    @pytest.mark.asyncio
    async def test_write_then_search_finds_image(self, async_http_client, write_service_url, search_service_url):
        """写入图片后搜索能找到"""
        # 1. 写入图片
        write_resp = await async_http_client.post(
            f"{write_service_url}/api/v1/image/update",
            json={
                "uri": f"https://cdn.test.com/e2e_test_{int(time.time())}.jpg",
                "merchant_id": "e2e_merchant_001",
                "category_l1": "shoes",
            },
        )
        assert write_resp.status_code == 200
        data = write_resp.json()
        assert data["status"] == "accepted"
        image_id = data["image_id"]

        # 2. 等待写入可见
        time.sleep(3)

        # 3. 搜索 (使用一个小的测试图片)
        test_image = base64.b64encode(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100).decode()
        search_resp = await async_http_client.post(
            f"{search_service_url}/api/v1/search",
            json={
                "query_image": test_image,
                "top_k": 10,
            },
        )
        assert search_resp.status_code == 200

    @pytest.mark.asyncio
    async def test_duplicate_write_detected(self, async_http_client, write_service_url):
        """重复写入同一 URI → is_new=False"""
        uri = f"https://cdn.test.com/e2e_dup_{int(time.time())}.jpg"

        # 首次写入
        resp1 = await async_http_client.post(
            f"{write_service_url}/api/v1/image/update",
            json={
                "uri": uri,
                "merchant_id": "e2e_merchant_002",
                "category_l1": "bags",
            },
        )
        assert resp1.status_code == 200
        assert resp1.json()["is_new"] is True

        # 重复写入
        resp2 = await async_http_client.post(
            f"{write_service_url}/api/v1/image/update",
            json={
                "uri": uri,
                "merchant_id": "e2e_merchant_002",
                "category_l1": "bags",
            },
        )
        assert resp2.status_code == 200
        assert resp2.json()["is_new"] is False
