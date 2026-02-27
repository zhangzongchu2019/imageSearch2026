"""
cron-scheduler 测试 fixtures
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def mock_pg():
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchval = AsyncMock(return_value=0)
    conn.execute = AsyncMock(return_value="DELETE 0")

    pool = AsyncMock()
    pool.acquire = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=conn),
        __aexit__=AsyncMock(),
    ))
    pool.close = AsyncMock()
    return pool


@pytest.fixture
def mock_milvus():
    return MagicMock()


@pytest.fixture
def mock_deps(mock_pg, mock_milvus):
    return {
        "pg": mock_pg,
        "milvus": mock_milvus,
        "bitmap_filter_host": "localhost:50051",
    }
