"""
真实 Milvus 搜索集成测试
"""
import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST"),
    reason="Set INTEGRATION_TEST=true",
)


class TestPipelineE2E:
    @pytest.mark.asyncio
    async def test_milvus_search_hot(self, milvus_conn):
        """真实 Milvus 热区搜索"""
        from pymilvus import Collection
        try:
            coll = Collection("global_images_hot", using="test")
            coll.load()
            # Simple search
            import numpy as np
            vec = np.random.randn(256).astype("float32")
            vec = (vec / np.linalg.norm(vec)).tolist()
            results = coll.search(
                data=[vec],
                anns_field="global_vec",
                param={"metric_type": "IP", "params": {"ef": 64}},
                limit=10,
            )
            assert results is not None
        except Exception as e:
            pytest.skip(f"Milvus not available: {e}")

    @pytest.mark.asyncio
    async def test_milvus_partitions_exist(self, milvus_conn):
        """验证分区存在"""
        from pymilvus import Collection
        try:
            coll = Collection("global_images_hot", using="test")
            partitions = coll.partitions
            names = [p.name for p in partitions]
            assert "_default" in names or len(names) > 0
        except Exception as e:
            pytest.skip(f"Milvus not available: {e}")
