"""
以图搜商品系统 · write-service 主入口
v1.2: 写入可见性 3× 压缩 (5s/10s/15s)
"""
from contextlib import asynccontextmanager

import structlog
import uvicorn
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.api.update_image import router as write_router
from app.core.dependencies import init_dependencies, close_dependencies

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    deps = await init_dependencies()
    app.state.deps = deps
    logger.info("write-service started")
    yield
    await close_dependencies(deps)
    logger.info("write-service stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title="以图搜商品系统 · 写入服务",
        version="1.2.0",
        lifespan=lifespan,
    )
    app.include_router(write_router, prefix="/api/v1")

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz():
        return {"status": "ready"}

    Instrumentator(
        excluded_handlers=["/healthz", "/readyz", "/metrics"],
    ).instrument(app).expose(app, endpoint="/metrics")
    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app", host="0.0.0.0", port=8081,
        workers=2, loop="uvloop", http="httptools",
    )
