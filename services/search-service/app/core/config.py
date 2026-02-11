"""
配置管理 — Pydantic Settings + YAML 文件加载
支持环境变量覆盖, 配置热更新
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


# ── 子配置模型 ──

class SystemConfig(BaseModel):
    version: str = "1.2.0"
    env: str = "development"
    cluster_id: str = "imgsrch-dev-01"


class ServerConfig(BaseModel):
    port: int = 8080
    workers: int = 4
    max_inflight: int = 200


class HotZoneConfig(BaseModel):
    months: int = 5
    M: int = 24
    efConstruction: int = 200
    ef_search: int = 192
    ef_search_s1: int = 128
    ef_search_s2: int = 64
    estimated_vectors: int = 750_000_000
    replicas: int = 3


class NonHotZoneConfig(BaseModel):
    months_start: int = 6
    months_end: int = 18
    index_type: str = "DISKANN"
    max_degree: int = 64
    search_list_size: int = 200
    estimated_vectors: int = 2_200_000_000
    replicas: int = 2


class EvergreenConfig(BaseModel):
    max_count: int = 300_000_000
    green_threshold: int = 250_000_000
    yellow_threshold: int = 280_000_000
    min_retention_years: int = 2
    ts_month_value: int = 999999


class DualPathConfig(BaseModel):
    fast_p99_ms: int = 240
    cascade_p99_ms: int = 400
    cascade_trigger: float = 0.80
    cascade_timeout_ms: int = 250
    fast_traffic_ratio: float = 0.80


class AnnConfig(BaseModel):
    coarse_top_k: int = 2000
    hot_timeout_ms: int = 150
    non_hot_timeout_ms: int = 250


class TagRecallConfig(BaseModel):
    enabled: bool = True
    idf_top_k: int = 5
    timeout_ms: int = 10
    inverted_top_k: int = 500


class RefineConfig(BaseModel):
    top_k: int = 2000
    top_k_s1: int = 1000
    top_k_s2: int = 500
    use_float32: bool = True


class FallbackConfig(BaseModel):
    score_threshold: float = 0.80
    sub_image_top_k: int = 200
    tag_top_k: int = 200
    enabled: bool = True


class FusionConfig(BaseModel):
    weight_global: float = 0.40
    weight_sub: float = 0.30
    weight_tag: float = 0.20
    weight_cat: float = 0.10


class ConfidenceConfig(BaseModel):
    high_score: float = 0.75
    high_min_count: int = 10
    medium_score: float = 0.50


class ResponseConfig(BaseModel):
    max_top_k: int = 200
    default_top_k: int = 100
    default_time_range: str = "all"
    default_data_scope: str = "all"


class SearchConfig(BaseModel):
    dual_path: DualPathConfig = DualPathConfig()
    ann: AnnConfig = AnnConfig()
    tag_recall: TagRecallConfig = TagRecallConfig()
    refine: RefineConfig = RefineConfig()
    fallback: FallbackConfig = FallbackConfig()
    fusion: FusionConfig = FusionConfig()
    confidence: ConfidenceConfig = ConfidenceConfig()
    response: ResponseConfig = ResponseConfig()


class DegradeS0S1Config(BaseModel):
    p99_threshold_ms: int = 450
    duration_s: int = 120
    error_rate_threshold: float = 0.0005


class DegradeS0S2Config(BaseModel):
    p99_threshold_ms: int = 800
    duration_s: int = 30
    error_rate_threshold: float = 0.01


class DegradeRecoveryConfig(BaseModel):
    s1_dwell_s: int = 300
    s2_dwell_s: int = 600
    s3_observe_s: int = 600
    s3_cooldown_s: int = 900
    s3_ramp_stages: List[float] = [0.1, 0.5, 1.0]


class DegradeConfig(BaseModel):
    s0_s1: DegradeS0S1Config = DegradeS0S1Config()
    s0_s2: DegradeS0S2Config = DegradeS0S2Config()
    recovery: DegradeRecoveryConfig = DegradeRecoveryConfig()


class MilvusConfig(BaseModel):
    host: str = "localhost"
    port: int = 19530
    pool_size: int = 16
    token: str = ""
    hot_collection: str = "global_images_hot"
    non_hot_collection: str = "global_images_non_hot"
    sub_collection: str = "sub_images"


class PostgresConfig(BaseModel):
    host: str = "localhost"
    port: int = 5432
    database: str = "image_search"
    user: str = "postgres"
    password: str = ""
    pool_min: int = 5
    pool_max: int = 20
    ssl: bool = False

    @property
    def dsn(self) -> str:
        ssl = "?sslmode=require" if self.ssl else ""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}{ssl}"


class RedisConfig(BaseModel):
    host: str = "localhost"
    port: int = 6379
    password: str = ""
    db: int = 0
    sentinel_master: Optional[str] = None


class KafkaConfig(BaseModel):
    bootstrap_servers: str = "localhost:9092"
    security_protocol: str = "PLAINTEXT"
    search_logs_topic: str = "image-search.search-logs"


class BitmapFilterConfig(BaseModel):
    host: str = "localhost"
    port: int = 50051
    timeout_ms: int = 8
    pg_fallback_timeout_ms: int = 10


class FeatureFlagsConfig(BaseModel):
    enable_fallback: bool = True
    enable_sub_image_search: bool = True
    enable_tag_search: bool = True
    enable_cascade_path: bool = True
    enable_push_update: bool = True
    enable_tag_recall_stage: bool = True
    enable_refine: bool = True


class RateLimitConfig(BaseModel):
    global_search_qps: int = 500
    global_search_burst: int = 800
    per_caller_search_qps: int = 50


# ── 顶层配置 ──

class Settings(BaseSettings):
    system: SystemConfig = SystemConfig()
    server: ServerConfig = ServerConfig()
    hot_zone: HotZoneConfig = HotZoneConfig()
    non_hot_zone: NonHotZoneConfig = NonHotZoneConfig()
    evergreen: EvergreenConfig = EvergreenConfig()
    search: SearchConfig = SearchConfig()
    degrade: DegradeConfig = DegradeConfig()
    milvus: MilvusConfig = MilvusConfig()
    postgres: PostgresConfig = PostgresConfig()
    redis: RedisConfig = RedisConfig()
    kafka: KafkaConfig = KafkaConfig()
    bitmap_filter: BitmapFilterConfig = BitmapFilterConfig()
    feature_flags: FeatureFlagsConfig = FeatureFlagsConfig()
    rate_limit: RateLimitConfig = RateLimitConfig()

    class Config:
        env_prefix = "IMGSRCH_"
        env_nested_delimiter = "__"


def _load_yaml_config() -> dict:
    """按优先级加载 YAML 配置: production > staging > default"""
    env = os.getenv("IMGSRCH_ENV", "development")
    config_dir = Path(__file__).parent.parent.parent / "config"

    merged = {}
    for name in ["default.yaml", f"{env}.yaml"]:
        path = config_dir / name
        if path.exists():
            with open(path) as f:
                data = yaml.safe_load(f) or {}
                _deep_merge(merged, data)
    return merged


def _deep_merge(base: dict, override: dict) -> dict:
    for k, v in override.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
    return base


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    yaml_conf = _load_yaml_config()
    return Settings(**yaml_conf)


def reload_settings() -> Settings:
    get_settings.cache_clear()
    return get_settings()
