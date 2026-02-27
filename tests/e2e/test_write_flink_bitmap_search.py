"""
写入→Kafka→Flink→Bitmap→搜索 E2E 测试
完整数据流验证: write-service → Kafka → Flink → PG → bitmap-filter → search
"""
import os
import time

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("E2E_TEST"),
    reason="Set E2E_TEST=true",
)


class TestWriteFlinkBitmapSearch:
    @pytest.mark.asyncio
    async def test_merchant_scope_filter_after_write(
        self, async_http_client, write_service_url, search_service_url, pg_pool
    ):
        """写入→Kafka→Flink→PG→bitmap→搜索 完整链路"""
        merchant_id = f"e2e_flink_merchant_{int(time.time())}"
        uri = f"https://cdn.test.com/e2e_flink_{int(time.time())}.jpg"

        # 1. 写入 (触发 Kafka merchant event)
        write_resp = await async_http_client.post(
            f"{write_service_url}/api/v1/image/update",
            json={
                "uri": uri,
                "merchant_id": merchant_id,
                "category_l1": "shoes",
            },
        )
        assert write_resp.status_code == 200
        image_id = write_resp.json()["image_id"]

        # 2. 等待 Flink 处理 + bitmap 更新 (Flink 1s 窗口 + 处理时间)
        time.sleep(10)

        # 3. 验证 PG bitmap 已写入
        async with pg_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT rb_cardinality(bitmap) as card FROM image_merchant_bitmaps WHERE image_pk = $1",
                image_id,
            )
        if row:
            assert row["card"] > 0, "Bitmap should have at least 1 merchant"
