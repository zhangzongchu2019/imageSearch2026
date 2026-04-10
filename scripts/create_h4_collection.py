#!/usr/bin/env python3
"""
创建 H4 双向量 Collection
- 主向量1: dinov2_vec (1024d HNSW_SQ8 + refine FP16) — 实例/形变/同款/视角
- 主向量2: clip_vec   (768d HNSW_SQ8 + refine FP16) — 语义相似 (跨品牌)
- 二进制:  phash, dhash (BIN_FLAT, Hamming)
- 多值字段: merchant_ids (BITMAP)
- 单值标量: brand_id, category_l1/l2 (STL_SORT)

用法:
  python3 create_h4_collection.py --drop
"""
import argparse
from pymilvus import (
    connections, utility, Collection,
    CollectionSchema, FieldSchema, DataType
)

COLLECTION = "h4_main"


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
        # 主键
        FieldSchema(name="image_pk", dtype=DataType.VARCHAR, max_length=64, is_primary=True),

        # L0 指纹 (Binary, 64bit)
        FieldSchema(name="phash", dtype=DataType.BINARY_VECTOR, dim=64),
        FieldSchema(name="dhash", dtype=DataType.BINARY_VECTOR, dim=64),

        # L1 主向量1: DINOv2-L (1024d, 实例/形变/同款)
        FieldSchema(name="dinov2_vec", dtype=DataType.FLOAT_VECTOR, dim=1024),

        # L1 主向量2: CLIP ViT-L-14 (768d, 语义/跨品牌)
        FieldSchema(name="clip_vec", dtype=DataType.FLOAT_VECTOR, dim=768),

        # 标量字段
        FieldSchema(name="url_hash", dtype=DataType.VARCHAR, max_length=64),
        FieldSchema(name="ts_month", dtype=DataType.INT32),
        FieldSchema(name="category_l1", dtype=DataType.INT32),
        FieldSchema(name="category_l2", dtype=DataType.INT32),
        FieldSchema(name="brand_id", dtype=DataType.INT32),

        # 多值字段: 商家 IDs
        FieldSchema(name="merchant_ids", dtype=DataType.ARRAY,
                    element_type=DataType.INT32, max_capacity=100),

        # 元数据
        FieldSchema(name="created_at", dtype=DataType.INT64),
    ]

    schema = CollectionSchema(fields, description="H4 双向量主 Collection (DINOv2 1024d + CLIP 768d)")
    coll = Collection(COLLECTION, schema=schema, shards_num=4)
    print(f"✅ 创建 Collection: {COLLECTION}")
    print(f"   字段数: {len(fields)}")
    print(f"   主向量1: dinov2_vec dim=1024")
    print(f"   主向量2: clip_vec   dim=768")

    # === L1 主向量1: DINOv2 HNSW_SQ8 + refine FP16 ===
    print("\n创建 dinov2_vec 索引 (HNSW_SQ + refine FP16)...")
    dinov2_index = {
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
    coll.create_index("dinov2_vec", dinov2_index)
    print("  ✅ dinov2_vec HNSW_SQ8 + refine FP16")

    # === L1 主向量2: CLIP HNSW_SQ8 + refine FP16 ===
    print("\n创建 clip_vec 索引 (HNSW_SQ + refine FP16)...")
    clip_index = {
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
    coll.create_index("clip_vec", clip_index)
    print("  ✅ clip_vec HNSW_SQ8 + refine FP16")

    # === L0 二进制索引 ===
    print("\n创建 phash/dhash BIN_FLAT 索引...")
    bin_params = {"index_type": "BIN_FLAT", "metric_type": "HAMMING"}
    coll.create_index("phash", bin_params)
    coll.create_index("dhash", bin_params)
    print("  ✅ phash/dhash 索引")

    # === 标量索引 ===
    print("\n创建标量索引...")
    coll.create_index("brand_id", {"index_type": "STL_SORT"})
    coll.create_index("category_l1", {"index_type": "STL_SORT"})
    coll.create_index("category_l2", {"index_type": "STL_SORT"})
    print("  ✅ brand_id, category_l1, category_l2")

    # === BITMAP 索引 (多值字段) ===
    print("\n创建 merchant_ids BITMAP 索引...")
    try:
        coll.create_index("merchant_ids", {"index_type": "BITMAP"})
        print("  ✅ merchant_ids BITMAP")
    except Exception as e:
        print(f"  ⚠️  BITMAP 失败 (尝试 INVERTED): {e}")
        try:
            coll.create_index("merchant_ids", {"index_type": "INVERTED"})
            print("  ✅ merchant_ids INVERTED")
        except Exception as e2:
            print(f"  ⚠️  INVERTED 也失败: {e2}")

    print(f"\n=== H4 Collection 创建完成 ===")
    print(f"Collection: {COLLECTION}")
    print(f"双向量: dinov2_vec(1024d) + clip_vec(768d), 都用 HNSW_SQ8 + refine FP16")
    print(f"过滤: brand/category/merchant_ids")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--drop", action="store_true", help="存在则先删除")
    args = parser.parse_args()
    create_collection(drop=args.drop)
