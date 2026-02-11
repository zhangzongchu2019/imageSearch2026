"""
词表缓存 — 本地 LRU + Redis + PG 三级
整数编码 ↔ 字符串 双向映射
"""
from __future__ import annotations

import asyncio
from functools import lru_cache
from typing import Dict, Optional

import structlog

logger = structlog.get_logger(__name__)


class VocabCache:
    """词表编解码: int_code ↔ string_val

    三级缓存: 本地 dict → Redis → PG vocabulary_mapping
    启动时全量预热 (≤12,000 行), 运行时 TTL 5min 增量刷新
    """

    def __init__(self, redis_client, pg_pool):
        self._redis = redis_client
        self._pg = pg_pool
        # {vocab_type: {int_code: string_val}}
        self._code_to_str: Dict[str, Dict[int, str]] = {}
        # {vocab_type: {string_val: int_code}}
        self._str_to_code: Dict[str, Dict[str, int]] = {}

    async def warm_up(self):
        """启动时全量加载词表 (≤12,000 行)"""
        async with self._pg.acquire() as conn:
            rows = await conn.fetch(
                "SELECT vocab_type, string_val, int_code FROM vocabulary_mapping"
            )
        for r in rows:
            vt = r["vocab_type"]
            if vt not in self._code_to_str:
                self._code_to_str[vt] = {}
                self._str_to_code[vt] = {}
            self._code_to_str[vt][r["int_code"]] = r["string_val"]
            self._str_to_code[vt][r["string_val"]] = r["int_code"]
        logger.info("vocab_cache_warmed", total=len(rows))

    def decode(self, vocab_type: str, int_code: Optional[int]) -> Optional[str]:
        """int_code → string (本地查找, 无 IO)"""
        if int_code is None:
            return None
        mapping = self._code_to_str.get(vocab_type, {})
        return mapping.get(int_code)

    def encode(self, vocab_type: str, string_val: Optional[str]) -> Optional[int]:
        """string → int_code (本地查找)"""
        if string_val is None:
            return None
        mapping = self._str_to_code.get(vocab_type, {})
        return mapping.get(string_val)

    async def refresh(self):
        """增量刷新 (配合配置中心或定时器)"""
        await self.warm_up()
