"""
请求/响应模型 — Pydantic v2 严格校验
对齐 BRD v4.0.1 / 系统设计 v1.2
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


# ── 枚举 ──

class DataScope(str, Enum):
    ALL = "all"
    ROLLING = "rolling"
    EVERGREEN = "evergreen"


class TimeRange(str, Enum):
    """v4.0 两区口径:
    - HOT_ONLY: 近 5 个月热区
    - ALL: 近 18 个月 + 常青 (默认)
    - HOT_PLUS_EVERGREEN: 热区 + 常青 (降级专用)
    """
    HOT_ONLY = "hot_only"          # 近 5 个月
    ALL = "all"                     # 近 18 个月 + 常青
    HOT_PLUS_EVERGREEN = "hot_eg"  # 热区 + 常青 (S1/S2 降级)


class Confidence(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class Strategy(str, Enum):
    FAST_PATH = "fast_path"
    CASCADE_PATH = "cascade_path"


class DegradeState(str, Enum):
    S0 = "S0"
    S1 = "S1"
    S2 = "S2"
    S3 = "S3"
    S4 = "S4"


# ── 请求模型 ──

class SearchRequest(BaseModel):
    query_image: str = Field(..., min_length=1, description="Base64 编码查询图片")
    merchant_scope: Optional[List[str]] = Field(
        None, max_length=3000, description="商家 ID 列表 (≤3000)"
    )
    merchant_scope_id: Optional[str] = Field(None, max_length=64, description="预注册 Scope ID")
    top_k: int = Field(100, ge=1, le=200)
    data_scope: DataScope = DataScope.ALL
    time_range: TimeRange = TimeRange.ALL

    @field_validator("merchant_scope")
    @classmethod
    def validate_scope(cls, v, info):
        if v is not None and info.data.get("merchant_scope_id"):
            raise ValueError("merchant_scope 与 merchant_scope_id 不可同时传入")
        return v


class UpdateImageRequest(BaseModel):
    uri: str = Field(..., min_length=1, max_length=2048)
    merchant_id: str = Field(..., min_length=1, max_length=64)
    category_l1: str = Field(..., min_length=1)
    product_id: Optional[str] = Field(None, max_length=64)
    tags: Optional[List[str]] = Field(None, max_length=32)


class UpdateVideoRequest(BaseModel):
    video_uri: str = Field(..., min_length=1, max_length=2048)
    merchant_id: str = Field(..., min_length=1, max_length=64)
    category_l1: str = Field(..., min_length=1)
    product_id: Optional[str] = Field(None, max_length=64)
    max_frames: int = Field(3, ge=1, le=10)


class BehaviorReportRequest(BaseModel):
    event_type: str = Field(..., pattern="^(impression|click|inquiry|order|negative_feedback)$")
    request_id: str = Field(..., min_length=1)
    image_id: str = Field(..., min_length=32, max_length=32)
    position: int = Field(..., ge=1)


# ── 有效参数 (经降级覆盖后) ──

@dataclass
class EffectiveParams:
    """经降级 FSM 覆盖后的有效搜索参数"""
    merchant_scope: Optional[List[str]] = None
    top_k: int = 100
    data_scope: DataScope = DataScope.ALL
    time_range: TimeRange = TimeRange.ALL
    ef_search: int = 192
    refine_top_k: int = 2000
    enable_fallback: bool = True
    enable_cascade: bool = True

    @classmethod
    def from_request(cls, req: "SearchRequest") -> "EffectiveParams":
        from app.core.config import get_settings
        s = get_settings()
        return cls(
            merchant_scope=req.merchant_scope,
            top_k=req.top_k,
            data_scope=req.data_scope,
            time_range=req.time_range,
            ef_search=s.hot_zone.ef_search,
            refine_top_k=s.search.refine.top_k,
            enable_fallback=s.feature_flags.enable_fallback,
            enable_cascade=s.feature_flags.enable_cascade_path,
        )


# ── 响应模型 ──

class SearchResultItem(BaseModel):
    image_id: str
    score: float
    product_id: Optional[str] = None
    position: int
    is_evergreen: bool = False
    category_l1: Optional[str] = None
    tags: Optional[List[str]] = None


class SearchMeta(BaseModel):
    request_id: str
    total_results: int
    strategy: Strategy
    confidence: Confidence
    degraded: bool = False
    filter_skipped: bool = False
    degrade_state: DegradeState = DegradeState.S0
    search_scope_desc: str
    latency_ms: int
    zone_hit: str = "hot"  # "hot" | "hot+non_hot"

    # 性能分解
    feature_ms: Optional[int] = None
    ann_hot_ms: Optional[int] = None
    ann_non_hot_ms: Optional[int] = None
    tag_recall_ms: Optional[int] = None
    filter_ms: Optional[int] = None
    refine_ms: Optional[int] = None


class SearchResponse(BaseModel):
    results: List[SearchResultItem]
    meta: SearchMeta


class UpdateImageResponse(BaseModel):
    status: str  # "accepted"
    image_id: str
    is_new: bool


class ErrorDetail(BaseModel):
    code: str
    message: str
    request_id: str
    timestamp: int


class ErrorResponse(BaseModel):
    error: ErrorDetail
