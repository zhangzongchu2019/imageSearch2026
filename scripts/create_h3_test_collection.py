#!/usr/bin/env python3
"""
创建 H3 方案测试 Collection
- 主向量: dinov2_vec 1024d (无 PCA, 5 机集群版)
- 二进制: phash 64bit, dhash 64bit
- 标量: category_l1, category_l2, brand_id, merchant_ids (ARRAY)
- 索引: HNSW_SQ8 + refine FP16, M=48, efC=1000

用法:
  python3 create_h3_test_collection.py [--drop]
"""
import argparse
from pymilvus import (
    connections, utility, Collection, CollectionSchema, FieldSchema, DataType
)

COLLECTION = "test_dinov2_h3"


def create_collection(drop=False):
    connections.connect(host="localhost", port="19530")

    if utility.has_collection(COLLECTION):
        if drop:
            utility.drop_collection(COLLECTION)
            print(f"已删除旧 Collection: {COLLECTION}")
        else:
            print(f"Collection 已存在: {COLLECTION}")
            return

    fields = [
        FieldSchema(name="image_pk", dtype=DataType.VARCHAR, max_length=64, is_primary=True),

        # L0 指纹 (Binary, 64bit)
        FieldSchema(name="phash", dtype=DataType.BINARY_VECTOR, dim=64),
        FieldSchema(name="dhash", dtype=DataType.BINARY_VECTOR, dim=64),

        # L1 主向量 DINOv2-L 1024d (5机集群版无 PCA)
        FieldSchema(name="dinov2_vec", dtype=DataType.FLOAT_VECTOR, dim=1024),

        # 标量字段
        FieldSchema(name="category_l1", dtype=DataType.INT32),
        FieldSchema(name="category_l2", dtype=DataType.INT32),
        FieldSchema(name="brand_id", dtype=DataType.INT32),
        # 多值字段: 商家 IDs
        FieldSchema(name="merchant_ids", dtype=DataType.ARRAY, element_type=DataType.INT32,
                    max_capacity=100),

        # 元数据
        FieldSchema(name="ts_month", dtype=DataType.INT32),
        FieldSchema(name="created_at", dtype=DataType.INT64),
    ]

    schema = CollectionSchema(fields, description="H3 方案测试 Collection (DINOv2-L 1024d)")
    coll = Collection(COLLECTION, schema=schema, shards_num=4)
    print(f"创建 Collection: {COLLECTION}")

    # === L1 主向量索引: HNSW_SQ8 + refine FP16 ===
    print("创建主索引 dinov2_vec (HNSW_SQ + refine FP16)...")
    main_index_params = {
        "index_type": "HNSW_SQ",
        "metric_type": "COSINE",
        "params": {
            "M": 48,
            "efConstruction": 1000,
            "sq_type": "SQ8",
            "refine": True,
            "refine_type": "FP16",
        }
    }
    try:
        coll.create_index("dinov2_vec", main_index_params)
        print("  ✅ HNSW_SQ8 + refine FP16 创建成功")
    except Exception as e:
        print(f"  ⚠️  HNSW_SQ 不支持, 回退到 HNSW: {e}")
        # 回退方案
        coll.create_index("dinov2_vec", {
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 48, "efConstruction": 1000}
        })
        print("  ✅ HNSW 创建成功")

    # === L0 二进制索引 (汉明距离) ===
    print("创建 phash/dhash BIN_FLAT 索引...")
    bin_params = {"index_type": "BIN_FLAT", "metric_type": "HAMMING"}
    coll.create_index("phash", bin_params)
    coll.create_index("dhash", bin_params)
    print("  ✅ phash/dhash 索引创建")

    # === 标量索引 ===
    print("创建标量索引...")
    try:
        coll.create_index("brand_id", {"index_type": "STL_SORT"})
        coll.create_index("category_l1", {"index_type": "STL_SORT"})
        coll.create_index("category_l2", {"index_type": "STL_SORT"})
        print("  ✅ 标量索引创建")
    except Exception as e:
        print(f"  ⚠️  标量索引: {e}")

    # === BITMAP 索引 (多值字段) ===
    print("创建 merchant_ids BITMAP 索引...")
    try:
        coll.create_index("merchant_ids", {"index_type": "BITMAP"})
        print("  ✅ BITMAP 索引创建")
    except Exception as e:
        print(f"  ⚠️  BITMAP 索引: {e}")

    print(f"\n✅ Collection {COLLECTION} 已就绪")
    print(f"   字段数: {len(fields)}")
    print(f"   主向量: dinov2_vec dim=1024")
    return coll


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--drop", action="store_true", help="存在则先删除")
    args = parser.parse_args()
    create_collection(drop=args.drop)
