"""
工具函数 + Schema 校验测试
"""
import pytest
import sys
sys.path.insert(0, ".")

from app.core.utils import gen_image_pk, sha256_hex, month_subtract, current_yyyymm
from app.model.schemas import SearchRequest, DataScope, TimeRange, Confidence


class TestImagePK:
    def test_deterministic(self):
        """同一 URI 生成相同 image_pk (SHA256 确定性)"""
        uri = "https://example.com/image.jpg"
        pk1 = gen_image_pk(uri)
        pk2 = gen_image_pk(uri)
        assert pk1 == pk2
        assert len(pk1) == 32  # 16 bytes hex = 32 chars

    def test_different_uri_different_pk(self):
        pk1 = gen_image_pk("https://a.com/1.jpg")
        pk2 = gen_image_pk("https://a.com/2.jpg")
        assert pk1 != pk2

    def test_hex_format(self):
        pk = gen_image_pk("test")
        assert all(c in "0123456789abcdef" for c in pk)


class TestMonthSubtract:
    def test_simple(self):
        assert month_subtract(202603, 3) == 202512

    def test_year_boundary(self):
        assert month_subtract(202601, 1) == 202512

    def test_18_months(self):
        assert month_subtract(202602, 18) == 202408


class TestSearchRequest:
    def test_valid_request(self):
        req = SearchRequest(query_image="dGVzdA==", top_k=50)
        assert req.top_k == 50
        assert req.data_scope == DataScope.ALL

    def test_top_k_range(self):
        with pytest.raises(Exception):
            SearchRequest(query_image="dGVzdA==", top_k=0)
        with pytest.raises(Exception):
            SearchRequest(query_image="dGVzdA==", top_k=201)

    def test_scope_conflict(self):
        """merchant_scope 和 merchant_scope_id 不可同时传入"""
        with pytest.raises(Exception):
            SearchRequest(
                query_image="dGVzdA==",
                merchant_scope=["m1"],
                merchant_scope_id="scope_1",
            )

    def test_merchant_scope_max(self):
        req = SearchRequest(
            query_image="dGVzdA==",
            merchant_scope=["m" + str(i) for i in range(100)],
        )
        assert len(req.merchant_scope) == 100
