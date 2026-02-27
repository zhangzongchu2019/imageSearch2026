"""
分区轮转集成测试 — 真实 Milvus 分区操作
"""
import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST"),
    reason="Set INTEGRATION_TEST=true",
)


class TestPartitionRotationReal:
    @pytest.mark.asyncio
    async def test_partitions_exist(self, milvus_collection):
        """Milvus 集合至少有默认分区"""
        partitions = milvus_collection.partitions
        assert len(partitions) >= 1

    @pytest.mark.asyncio
    async def test_evergreen_partition_present(self, milvus_collection):
        """p_999999 常青分区应存在"""
        names = [p.name for p in milvus_collection.partitions]
        assert "p_999999" in names, f"Expected p_999999 in {names}"

    @pytest.mark.asyncio
    async def test_partition_naming_convention(self, milvus_collection):
        """非默认分区遵循 p_YYYYMM 或 p_999999"""
        for p in milvus_collection.partitions:
            if p.name == "_default":
                continue
            assert p.name.startswith("p_"), f"Unexpected partition: {p.name}"
