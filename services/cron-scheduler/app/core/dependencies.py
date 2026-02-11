"""cron-scheduler 依赖初始化"""
import os
import asyncpg
from pymilvus import connections


async def init_deps() -> dict:
    pg_dsn = os.getenv("PG_DSN", "postgresql://postgres@localhost:5432/image_search")
    milvus_host = os.getenv("MILVUS_HOST", "localhost")

    pg = await asyncpg.create_pool(dsn=pg_dsn, min_size=2, max_size=5)
    connections.connect(alias="default", host=milvus_host, port=19530)

    return {"pg": pg, "milvus": connections}
