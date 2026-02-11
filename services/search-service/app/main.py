"""
以图搜商品系统 · search-service 主入口
v1.2 两区架构 · 双路径延迟模型
"""
import asyncio
import signal
from contextlib import asynccontextmanager

import structlog
import uvicorn
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.api.routes import router as api_router
from app.core.config import get_settings
from app.core.lifecycle import ServiceLifecycle
from app.core.metrics import setup_metrics

logger = structlog.get_logger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """服务生命周期管理: 初始化所有依赖 → 服务就绪 → 优雅关闭"""
    lifecycle = ServiceLifecycle()
    try:
        await lifecycle.startup()
        app.state.lifecycle = lifecycle
        logger.info(
            "search-service started",
            version=settings.system.version,
            env=settings.system.env,
            zones="hot_hnsw+non_hot_diskann",
        )
        yield
    finally:
        await lifecycle.shutdown()
        logger.info("search-service shutdown complete")


def create_app() -> FastAPI:
    app = FastAPI(
        title="以图搜商品系统 · 检索服务",
        version=settings.system.version,
        docs_url="/docs" if settings.system.env != "production" else None,
        redoc_url=None,
        lifespan=lifespan,
    )

    # 路由
    app.include_router(api_router, prefix="/api/v1")

    # Prometheus 指标
    Instrumentator(
        should_group_status_codes=True,
        should_group_untemplated=True,
        excluded_handlers=["/healthz", "/readyz", "/metrics"],
    ).instrument(app).expose(app, endpoint="/metrics")

    setup_metrics()
    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.server.port,
        workers=settings.server.workers,
        loop="uvloop",
        http="httptools",
        access_log=False,
        log_level="warning",
    )
