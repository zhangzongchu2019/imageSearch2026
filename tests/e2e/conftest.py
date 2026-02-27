"""
E2E 测试 fixtures — httpx clients, pg_pool, 环境变量
需要 E2E_TEST=true 且全部服务运行
"""
import os

import pytest

SKIP_REASON = "Set E2E_TEST=true and start all services"


def pytest_collection_modifyitems(config, items):
    if not os.getenv("E2E_TEST"):
        skip = pytest.mark.skip(reason=SKIP_REASON)
        for item in items:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def write_service_url():
    return os.getenv("WRITE_SERVICE_URL", "http://localhost:8081")


@pytest.fixture(scope="session")
def search_service_url():
    return os.getenv("SEARCH_SERVICE_URL", "http://localhost:8080")


@pytest.fixture(scope="session")
async def pg_pool():
    import asyncpg
    dsn = os.getenv("PG_DSN", "postgresql://postgres@localhost:5432/image_search")
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=3)
    yield pool
    await pool.close()


@pytest.fixture(scope="session")
def http_client():
    import httpx
    client = httpx.Client(timeout=30.0)
    yield client
    client.close()


@pytest.fixture(scope="session")
async def async_http_client():
    import httpx
    async with httpx.AsyncClient(timeout=30.0) as client:
        yield client
