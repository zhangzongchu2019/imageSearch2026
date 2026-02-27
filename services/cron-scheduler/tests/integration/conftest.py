"""
cron-scheduler 集成测试 fixtures — 真实 PG + Milvus
"""
import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST"),
    reason="Set INTEGRATION_TEST=true",
)


@pytest.fixture(scope="session")
async def pg_pool():
    import asyncpg
    dsn = os.getenv("PG_DSN", "postgresql://postgres@localhost:5432/image_search")
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=3)
    yield pool
    await pool.close()


@pytest.fixture(scope="session")
def milvus_collection():
    from pymilvus import Collection, connections
    host = os.getenv("MILVUS_HOST", "localhost")
    port = int(os.getenv("MILVUS_PORT", "19530"))
    connections.connect(alias="test", host=host, port=port)
    coll = Collection(os.getenv("HOT_COLLECTION", "global_images_hot"), using="test")
    yield coll
    connections.disconnect("test")
