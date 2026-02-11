"""
通用工具函数 — 时间/ID/编码
"""
from __future__ import annotations

import hashlib
import time
import uuid
from datetime import datetime


def current_yyyymm() -> int:
    """当前年月, 格式 YYYYMM"""
    return int(datetime.utcnow().strftime("%Y%m"))


def month_subtract(yyyymm: int, months: int) -> int:
    """YYYYMM 减去 N 个月"""
    y, m = divmod(yyyymm, 100)
    total = y * 12 + (m - 1) - months
    return (total // 12) * 100 + (total % 12) + 1


def now_ms() -> int:
    return int(time.time() * 1000)


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def gen_request_id() -> str:
    return f"req_{time.strftime('%Y%m%d')}_{uuid.uuid4().hex[:12]}"


def sha256_hex(data: str) -> str:
    return hashlib.sha256(data.encode()).hexdigest()


def gen_image_pk(uri: str) -> str:
    """image_pk = hex(sha256(uri)[0:16]) → 32 hex chars"""
    digest = hashlib.sha256(uri.encode()).digest()
    return digest[:16].hex()
