"""
配置服务层 — 统一配置获取 + 热更新
支持三种后端:
  1. K8s ConfigMap / Secret (默认, 通过 volume mount)
  2. Nacos 配置中心 (通过 HTTP long-polling)
  3. 环境变量覆盖 (最高优先级)

所有数据库、中间件连接参数均通过本服务获取, 不再硬编码。
"""
from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import structlog
import yaml

logger = structlog.get_logger(__name__)


class ConfigService:
    """统一配置服务 — 支持多后端 + 热更新 + 变更监听

    优先级: 环境变量 > Nacos > K8s ConfigMap > YAML 文件 > 默认值
    """

    def __init__(self):
        self._cache: Dict[str, Any] = {}
        self._listeners: Dict[str, List[Callable]] = {}
        self._lock = threading.RLock()
        self._backend: str = os.getenv("CONFIG_BACKEND", "k8s")  # k8s | nacos | env
        self._nacos_url: str = os.getenv("NACOS_SERVER_ADDR", "")
        self._nacos_namespace: str = os.getenv("NACOS_NAMESPACE", "")
        self._nacos_group: str = os.getenv("NACOS_GROUP", "image-search")
        self._k8s_config_dir: str = os.getenv(
            "K8S_CONFIG_DIR", "/etc/config"
        )
        self._initialized = False
        self._config_version: int = 0  # FIX-I: 配置版本号 (每次变更递增)
        self._audit_log: List[Dict[str, Any]] = []  # FIX-I: 最近 100 条变更记录

    async def init(self):
        """初始化: 按后端类型加载配置"""
        if self._initialized:
            return

        if self._backend == "nacos" and self._nacos_url:
            await self._load_from_nacos()
        elif self._backend == "k8s":
            self._load_from_k8s()
        else:
            self._load_from_env()

        self._initialized = True
        logger.info(
            "config_service_initialized",
            backend=self._backend,
            keys_loaded=len(self._cache),
        )

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值 — 环境变量优先覆盖"""
        env_key = f"IMGSRCH_{key.upper().replace('.', '__')}"
        env_val = os.getenv(env_key)
        if env_val is not None:
            return self._parse_value(env_val)
        with self._lock:
            return self._cache.get(key, default)

    def get_str(self, key: str, default: str = "") -> str:
        val = self.get(key, default)
        return str(val) if val is not None else default

    def get_int(self, key: str, default: int = 0) -> int:
        val = self.get(key, default)
        try:
            return int(val)
        except (ValueError, TypeError):
            return default

    def get_bool(self, key: str, default: bool = False) -> bool:
        val = self.get(key, default)
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.lower() in ("true", "1", "yes")
        return default

    def get_secret(self, key: str) -> Optional[str]:
        """获取敏感配置 (密码/Token) — K8s Secret 或环境变量"""
        env_key = f"IMGSRCH_SECRET_{key.upper()}"
        val = os.getenv(env_key)
        if val:
            return val
        # K8s Secret mount
        secret_path = Path(f"/etc/secrets/{key}")
        if secret_path.exists():
            return secret_path.read_text().strip()
        return self.get_str(f"secrets.{key}")

    def on_change(self, key: str, callback: Callable):
        """注册配置变更监听器"""
        with self._lock:
            self._listeners.setdefault(key, []).append(callback)

    def _notify_listeners(self, key: str, old_val: Any, new_val: Any):
        # FIX-I: 配置变更审计日志
        self._config_version += 1
        audit_entry = {
            "version": self._config_version,
            "key": key,
            "old_value": str(old_val)[:200] if old_val is not None else None,
            "new_value": str(new_val)[:200] if new_val is not None else None,
            "timestamp": time.time(),
            "backend": self._backend,
        }
        self._audit_log.append(audit_entry)
        if len(self._audit_log) > 100:
            self._audit_log = self._audit_log[-100:]
        logger.info(
            "config_change_audit",
            version=self._config_version,
            key=key,
            old_value=audit_entry["old_value"],
            new_value=audit_entry["new_value"],
        )

        callbacks = self._listeners.get(key, [])
        for cb in callbacks:
            try:
                cb(key, old_val, new_val)
            except Exception as e:
                logger.error("config_listener_error", key=key, error=str(e))

    def get_audit_log(self, limit: int = 20) -> List[Dict[str, Any]]:
        """FIX-I: 获取最近的配置变更记录"""
        return self._audit_log[-limit:]

    @property
    def config_version(self) -> int:
        """FIX-I: 当前配置版本号"""
        return self._config_version

    # ── K8s ConfigMap 加载 ──

    def _load_from_k8s(self):
        """从 K8s ConfigMap volume mount 加载"""
        config_dir = Path(self._k8s_config_dir)
        if not config_dir.exists():
            logger.warning("k8s_config_dir_not_found", path=str(config_dir))
            self._load_from_env()
            return

        for file in config_dir.iterdir():
            if file.suffix in (".yaml", ".yml"):
                try:
                    data = yaml.safe_load(file.read_text()) or {}
                    self._flatten_dict(data, prefix="")
                except Exception as e:
                    logger.error("k8s_config_load_error", file=str(file), error=str(e))
            elif file.suffix == ".json":
                try:
                    data = json.loads(file.read_text())
                    self._flatten_dict(data, prefix="")
                except Exception as e:
                    logger.error("k8s_config_load_error", file=str(file), error=str(e))

    def _flatten_dict(self, d: dict, prefix: str):
        for k, v in d.items():
            full_key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                self._flatten_dict(v, full_key)
            else:
                with self._lock:
                    self._cache[full_key] = v

    # ── Nacos 加载 ──

    async def _load_from_nacos(self):
        """从 Nacos 配置中心加载"""
        import httpx
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    f"{self._nacos_url}/nacos/v1/cs/configs",
                    params={
                        "dataId": "image-search-config",
                        "group": self._nacos_group,
                        "tenant": self._nacos_namespace,
                    },
                )
                resp.raise_for_status()
                content = resp.text
                if content.strip().startswith("{"):
                    data = json.loads(content)
                else:
                    data = yaml.safe_load(content) or {}
                self._flatten_dict(data, prefix="")
                logger.info("nacos_config_loaded", keys=len(self._cache))
        except Exception as e:
            logger.error("nacos_config_load_failed", error=str(e))
            self._load_from_env()

    async def start_nacos_listener(self):
        """Nacos long-polling 监听配置变更"""
        if self._backend != "nacos":
            return
        asyncio.create_task(self._nacos_long_poll_loop())

    async def _nacos_long_poll_loop(self):
        import httpx
        while True:
            try:
                async with httpx.AsyncClient(timeout=35.0) as client:
                    resp = await client.post(
                        f"{self._nacos_url}/nacos/v1/cs/configs/listener",
                        data={
                            "Listening-Configs": (
                                f"image-search-config\x02{self._nacos_group}\x02"
                                f"{self._cache.get('__md5', '')}\x02{self._nacos_namespace}\x01"
                            ),
                        },
                        headers={"Long-Pulling-Timeout": "30000"},
                    )
                    if resp.text.strip():
                        logger.info("nacos_config_changed, reloading")
                        await self._load_from_nacos()
            except Exception as e:
                logger.warning("nacos_poll_error", error=str(e))
                await asyncio.sleep(5)

    # ── 环境变量加载 ──

    def _load_from_env(self):
        """纯环境变量模式"""
        for k, v in os.environ.items():
            if k.startswith("IMGSRCH_"):
                config_key = k[8:].lower().replace("__", ".")
                with self._lock:
                    self._cache[config_key] = self._parse_value(v)

    @staticmethod
    def _parse_value(val: str) -> Any:
        if val.lower() in ("true", "false"):
            return val.lower() == "true"
        try:
            return int(val)
        except ValueError:
            pass
        try:
            return float(val)
        except ValueError:
            pass
        return val


# ── 全局单例 ──

_config_service: Optional[ConfigService] = None


def get_config_service() -> ConfigService:
    global _config_service
    if _config_service is None:
        _config_service = ConfigService()
    return _config_service
