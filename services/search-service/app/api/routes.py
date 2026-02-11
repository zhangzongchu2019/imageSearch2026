"""
API 路由定义 — 对齐系统设计 v1.2 接口规范
"""
from __future__ import annotations

import base64
import time
import uuid
from typing import Optional

import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Request

from app.core.config import get_settings
from app.core.metrics import METRICS
from app.model.schemas import (
    BehaviorReportRequest,
    DegradeState,
    ErrorDetail,
    ErrorResponse,
    SearchRequest,
    SearchResponse,
)

logger = structlog.get_logger(__name__)
settings = get_settings()
router = APIRouter()


def _gen_request_id() -> str:
    return f"req_{time.strftime('%Y%m%d')}_{uuid.uuid4().hex[:12]}"


async def _verify_api_key(x_api_key: Optional[str] = Header(None)) -> str:
    """API Key 认证 — 从配置服务或 PG 加载, SHA256 校验 (FIX #6)"""
    if not x_api_key:
        raise HTTPException(status_code=401, detail={
            "error": {"code": "100_02_01", "message": "X-API-Key header is required"}
        })
    # 校验: SHA256(api_key) → 查 api_keys 表 或配置服务
    import hashlib
    key_hash = hashlib.sha256(x_api_key.encode()).hexdigest()

    # 优先从配置服务获取 API Key 白名单
    from app.core.config_service import get_config_service
    cs = get_config_service()
    allowed_keys_csv = cs.get_str("api_keys.hashes", "")
    if allowed_keys_csv:
        allowed_hashes = {h.strip() for h in allowed_keys_csv.split(",") if h.strip()}
        if key_hash in allowed_hashes:
            return x_api_key
        raise HTTPException(status_code=403, detail={
            "error": {"code": "100_02_02", "message": "Invalid API Key"}
        })

    # 降级: 前缀校验 (仅开发环境)
    if settings.system.env != "production":
        if x_api_key.startswith("imgsrch_"):
            return x_api_key
    raise HTTPException(status_code=403, detail={
        "error": {"code": "100_02_02", "message": "Invalid API Key"}
    })


# ── 检索接口 ──

@router.post(
    "/image/search",
    response_model=SearchResponse,
    summary="图片检索",
    description="以图搜商品主检索接口 · 双路径: 快路径 ≤240ms / 级联 ≤400ms",
)
async def search_image(
    req: SearchRequest,
    request: Request,
    api_key: str = Depends(_verify_api_key),
):
    request_id = _gen_request_id()
    lifecycle = request.app.state.lifecycle

    # 限流检查
    if not lifecycle.rate_limiter.try_acquire("global_search"):
        METRICS.rate_limited_total.labels(type="global").inc()
        # FIX-9: 补充 quota 配额信息, 帮助调用方制定重试策略
        bucket = lifecycle.rate_limiter._buckets.get("global_search", {})
        raise HTTPException(status_code=429, detail={
            "error": {
                "code": "100_03_01",
                "message": "QPS limit exceeded",
                "request_id": request_id,
                "retry_after_ms": 200,
                "quota": {
                    "limit_qps": bucket.get("rate", 0),
                    "burst": bucket.get("burst", 0),
                    "remaining_tokens": round(bucket.get("tokens", 0), 2),
                },
            }
        })

    # 校验 query_image base64
    try:
        img_bytes = base64.b64decode(req.query_image, validate=True)
        if len(img_bytes) > 10 * 1024 * 1024:
            raise HTTPException(status_code=400, detail={
                "error": {"code": "100_01_02", "message": "query_image exceeds 10MB"}
            })
    except Exception:
        raise HTTPException(status_code=400, detail={
            "error": {"code": "100_01_01", "message": "Invalid query_image base64"}
        })

    # 执行流水线
    try:
        response = await lifecycle.pipeline.execute(req, request_id)
        return response
    except Exception as e:
        logger.error("search_pipeline_error", request_id=request_id, error=str(e))
        METRICS.search_errors_total.labels(code="100_05_01").inc()
        raise HTTPException(status_code=500, detail={
            "error": {
                "code": "100_05_01",
                "message": "Internal processing error",
                "request_id": request_id,
                "timestamp": int(time.time() * 1000),
            }
        })


# ── 行为上报 ──

@router.post("/behavior/report", summary="行为上报 (可选)")
async def report_behavior(
    req: BehaviorReportRequest,
    request: Request,
    api_key: str = Depends(_verify_api_key),
):
    lifecycle = request.app.state.lifecycle
    try:
        await lifecycle.behavior_reporter.report(req)
        return {"status": "accepted"}
    except Exception as e:
        # 行为上报失败不阻塞, 仅记录
        logger.warning("behavior_report_failed", error=str(e))
        METRICS.behavior_report_errors_total.inc()
        return {"status": "accepted", "warning": "report may be delayed"}


# ── 健康检查 ──

@router.get("/system/status", summary="系统状态")
async def system_status(request: Request):
    lifecycle = request.app.state.lifecycle
    return {
        "status": "serving",
        "version": settings.system.version,
        "degrade_state": lifecycle.degrade_fsm.state.value,
        "uptime_s": int(time.monotonic() - lifecycle.start_time),
    }


@router.get("/healthz", include_in_schema=False)
async def healthz():
    return {"status": "ok"}


@router.get("/readyz", include_in_schema=False)
async def readyz(request: Request):
    lifecycle = request.app.state.lifecycle
    if not lifecycle.ready:
        raise HTTPException(status_code=503, detail="not ready")

    # FIX-C: 运行时依赖健康检查 — 任一关键依赖不可用则拒绝流量
    checks = {}
    try:
        checks["redis"] = lifecycle.redis_client is not None and await lifecycle.redis_client.ping()
    except Exception:
        checks["redis"] = False
    try:
        from pymilvus import connections
        checks["milvus"] = connections.has_connection("default")
    except Exception:
        checks["milvus"] = False
    checks["model"] = lifecycle.pipeline is not None

    all_ok = all(checks.values())
    if not all_ok:
        raise HTTPException(status_code=503, detail={
            "status": "not_ready",
            "checks": checks,
        })
    return {"status": "ready", "checks": checks}


# ── 管理接口 ──

@router.get("/admin/degrade/status", summary="降级状态查询 (P0-F: 测试可观测)")
async def degrade_status(request: Request):
    """返回 FSM 完整状态 — 含 epoch、窗口指标、Redis 连接状态"""
    lifecycle = request.app.state.lifecycle
    fsm = lifecycle._degrade_fsm
    return fsm.status()


@router.get("/admin/breakers", summary="熔断器状态查询 (P0-A+F)")
async def breaker_status():
    """返回所有熔断器当前状态"""
    from app.core.circuit_breaker import all_breakers
    return all_breakers()


@router.post("/admin/breakers/{name}/force", summary="熔断器手动控制 (P0-F: 故障注入)")
async def force_breaker(name: str, state: str = "open"):
    """手动打开/关闭熔断器 — 用于故障注入测试

    state: open | closed | half_open
    """
    from app.core.circuit_breaker import BreakerState, get_breaker
    state_map = {
        "open": BreakerState.OPEN,
        "closed": BreakerState.CLOSED,
        "half_open": BreakerState.HALF_OPEN,
    }
    if state not in state_map:
        raise HTTPException(400, f"Invalid state: {state}. Use: open/closed/half_open")
    breaker = get_breaker(name)
    breaker.force_state(state_map[state], reason="admin_api_force")
    return breaker.status()

@router.post("/admin/degrade/override", summary="人工降级覆盖")
async def degrade_override(
    state: str,
    reason: str = "",
    request: Request = None,
):
    try:
        target = DegradeState(state)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid state: {state}")
    lifecycle = request.app.state.lifecycle
    lifecycle.degrade_fsm.force_state(target, reason)
    return {"status": "ok", "state": target.value}


@router.post("/admin/degrade/release", summary="解除人工降级")
async def degrade_release(reason: str = "", request: Request = None):
    lifecycle = request.app.state.lifecycle
    lifecycle.degrade_fsm.force_state(DegradeState.S0, reason)
    return {"status": "ok", "state": "S0"}


@router.post("/admin/config/reload", summary="配置热更新")
async def config_reload():
    from app.core.config import reload_settings
    reload_settings()
    return {"status": "reloaded"}


@router.get("/admin/config/audit", summary="FIX-I: 配置变更审计日志")
async def config_audit(request: Request, limit: int = 20):
    """返回最近的配置变更记录, 含版本号/时间/旧值/新值"""
    lifecycle = request.app.state.lifecycle
    cs = lifecycle.config_service
    return {
        "current_version": cs.config_version,
        "recent_changes": cs.get_audit_log(limit=min(limit, 100)),
    }


@router.get("/admin/rate_limiter/status", summary="FIX-L: 限流器内部状态")
async def rate_limiter_status(request: Request):
    """返回各令牌桶的当前令牌数、速率、突发上限"""
    lifecycle = request.app.state.lifecycle
    return lifecycle.rate_limiter.status()
