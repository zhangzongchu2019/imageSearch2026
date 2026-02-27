"""
write-service 测试 fixtures — mock 所有外部依赖
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
def mock_redis():
    """Mock Redis 客户端"""
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock(return_value=True)
    redis.eval = AsyncMock(return_value=1)
    redis.ping = AsyncMock(return_value=True)
    return redis


@pytest.fixture
def mock_pg():
    """Mock PostgreSQL 连接池"""
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchval = AsyncMock(return_value=1)
    conn.execute = AsyncMock(return_value="DELETE 0")
    conn.transaction = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(),
        __aexit__=AsyncMock(),
    ))

    pool = AsyncMock()
    pool.acquire = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=conn),
        __aexit__=AsyncMock(),
    ))
    pool.close = AsyncMock()
    return pool


@pytest.fixture
def mock_kafka():
    """Mock Kafka producer"""
    kafka = AsyncMock()
    kafka.send_and_wait = AsyncMock()
    kafka.stop = AsyncMock()
    return kafka


@pytest.fixture
def mock_milvus():
    """Mock Milvus connections"""
    return MagicMock()


@pytest.fixture
def mock_vocab():
    """Mock vocabulary encoder"""
    vocab = MagicMock()
    vocab.encode = MagicMock(side_effect=lambda vt, val: {
        ("category_l1", "shoes"): 42,
        ("category_l1", "bags"): 99,
        ("tag", "leather"): 10,
        ("tag", "red"): 20,
    }.get((vt, val)))
    return vocab


@pytest.fixture
def mock_bitmap_push_client():
    """Mock bitmap-filter gRPC push client"""
    client = AsyncMock()
    client.push_update = AsyncMock()
    client.close = AsyncMock()
    return client


@pytest.fixture
def mock_deps(mock_redis, mock_pg, mock_kafka, mock_milvus, mock_vocab, mock_bitmap_push_client):
    """Complete mock dependencies dict"""
    return {
        "pg": mock_pg,
        "redis": mock_redis,
        "kafka": mock_kafka,
        "milvus": mock_milvus,
        "vocab": mock_vocab,
        "instance_id": "test-instance-001",
        "bitmap_push_client": mock_bitmap_push_client,
        "milvus_executor": None,
    }
