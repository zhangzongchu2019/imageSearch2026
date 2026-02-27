"""
write-service 集成测试 fixtures — 真实 PG/Redis/Milvus 连接
需要 INTEGRATION_TEST=true 环境变量
"""
import os

import pytest

SKIP_REASON = "Set INTEGRATION_TEST=true to run integration tests"


def pytest_collection_modifyitems(config, items):
    if not os.getenv("INTEGRATION_TEST"):
        skip = pytest.mark.skip(reason=SKIP_REASON)
        for item in items:
            item.add_marker(skip)


@pytest.fixture(scope="session")
async def pg_pool():
    import asyncpg
    dsn = os.getenv("PG_DSN", "postgresql://postgres@localhost:5432/image_search")
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=3)
    yield pool
    await pool.close()


@pytest.fixture(scope="session")
async def redis_client():
    import redis.asyncio as aioredis
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    client = aioredis.from_url(url, decode_responses=True)
    yield client
    await client.aclose()
