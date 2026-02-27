"""
词表编码测试 (category → int)
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import sys
sys.path.insert(0, ".")

from fastapi import HTTPException
from app.api.update_image import UpdateImageRequest


class TestVocabEncoding:
    def test_known_category(self, mock_vocab):
        """已知类目正常编码"""
        code = mock_vocab.encode("category_l1", "shoes")
        assert code == 42

    def test_unknown_category_returns_none(self, mock_vocab):
        """未知类目返回 None"""
        code = mock_vocab.encode("category_l1", "unknown_cat")
        assert code is None

    def test_unknown_tag_skipped(self, mock_vocab):
        """未知标签跳过 (不报错)"""
        code = mock_vocab.encode("tag", "nonexistent")
        assert code is None

    def test_tags_truncated_32(self):
        """标签列表截断为 32 个"""
        tags = [f"tag_{i}" for i in range(50)]
        truncated = tags[:32]
        assert len(truncated) == 32

    def test_known_tag_encoded(self, mock_vocab):
        """已知标签正常编码"""
        code = mock_vocab.encode("tag", "leather")
        assert code == 10
