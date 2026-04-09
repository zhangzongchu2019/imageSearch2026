#!/usr/bin/env python3
"""
评测 H3 混合搜索 (向量 + RoaringBitmap 多值过滤)
- 测试不同过滤条件下的延迟变化
- 验证算子下推 (无召回崩塌)
"""
import time
import os
import numpy as np
from pymilvus import connections, Collection

COLLECTION = "test_dinov2_h3"


def main():
    milvus_host = os.environ.get("MILVUS_HOST", "imgsrch-mvs-proxy")
    print(f"连接 {milvus_host}...")
    connections.connect(host=milvus_host, port="19530")
    coll = Collection(COLLECTION)
    coll.load()
    print(f"  ✅ {coll.num_entities:,} entities")

    # 准备 100 个随机查询向量
    np.random.seed(42)
    queries = np.random.randn(100, 1024).astype(np.float32)
    queries = queries / np.linalg.norm(queries, axis=1, keepdims=True)
    queries_list = queries.tolist()

    test_cases = [
        {
            "name": "无过滤",
            "expr": None,
        },
        {
            "name": "单 brand_id (==)",
            "expr": "brand_id == 100",
        },
        {
            "name": "多 brand_id (in 5)",
            "expr": "brand_id in [100, 200, 300, 400, 500]",
        },
        {
            "name": "category_l1 (==)",
            "expr": "category_l1 == 1001",
        },
        {
            "name": "组合过滤 (brand+category)",
            "expr": "brand_id in [100,200,300] and category_l1 == 1001",
        },
        {
            "name": "merchant_ids ARRAY_CONTAINS (1)",
            "expr": "array_contains(merchant_ids, 100)",
        },
        {
            "name": "merchant_ids ARRAY_CONTAINS_ANY (5)",
            "expr": "array_contains_any(merchant_ids, [100, 200, 300, 400, 500])",
        },
        {
            "name": "全条件 (brand+cat+merchant)",
            "expr": "brand_id in [100,200,300] and category_l1 == 1001 and array_contains_any(merchant_ids, [100, 200])",
        },
    ]

    print("\n" + "="*80)
    print("H3 混合搜索过滤测试")
    print("="*80)
    print(f"{'测试用例':<40} {'P50 ms':<12} {'P95 ms':<12} {'返回数':<8}")
    print("-"*80)

    for case in test_cases:
        latencies = []
        result_counts = []
        errors = 0

        for q in queries_list:
            try:
                t0 = time.time()
                res = coll.search(
                    data=[q],
                    anns_field="dinov2_vec",
                    param={"metric_type": "COSINE", "params": {"ef": 300}},
                    limit=10,
                    expr=case["expr"],
                    output_fields=["image_pk"],
                )
                latencies.append((time.time() - t0) * 1000)
                result_counts.append(len(res[0]))
            except Exception as e:
                errors += 1
                if errors <= 2:
                    print(f"  ⚠️  {case['name']} error: {str(e)[:80]}")

        if latencies:
            p50 = np.percentile(latencies, 50)
            p95 = np.percentile(latencies, 95)
            avg_count = np.mean(result_counts)
            print(f"{case['name']:<40} {p50:>9.1f}ms {p95:>9.1f}ms {avg_count:>6.1f}")
        else:
            print(f"{case['name']:<40} {'ALL FAIL':<30}")

    print("="*80)
    print("\n✅ 过滤测试完成")
    print("   关键观察:")
    print("   1. 算子下推: 过滤 vs 无过滤的延迟差异")
    print("   2. 召回稳定性: 返回数应保持 10 (除非过滤后候选不足)")
    print("   3. ARRAY_CONTAINS 是否被支持")


if __name__ == "__main__":
    main()
