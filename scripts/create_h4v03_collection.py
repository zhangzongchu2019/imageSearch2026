#!/usr/bin/env python3
"""创建 H4 v0.3 Collection (SSCD 双向量 + 强/弱标签 + quality/scene)"""
from pymilvus import connections, utility, Collection, CollectionSchema, FieldSchema, DataType

COLLECTION = "h4v03_main"

def create(drop=False):
    connections.connect(host="localhost", port="19530")
    if utility.has_collection(COLLECTION):
        if drop:
            utility.drop_collection(COLLECTION)
        else:
            print(f"已存在: {COLLECTION}"); return

    fields = [
        FieldSchema(name="image_pk", dtype=DataType.VARCHAR, max_length=64, is_primary=True),
        # 向量
        FieldSchema(name="sscd_vec", dtype=DataType.FLOAT_VECTOR, dim=512),
        FieldSchema(name="center_vec", dtype=DataType.FLOAT_VECTOR, dim=512),
        FieldSchema(name="phash", dtype=DataType.BINARY_VECTOR, dim=64),
        FieldSchema(name="dhash", dtype=DataType.BINARY_VECTOR, dim=64),
        # 强过滤标签
        FieldSchema(name="category_l1", dtype=DataType.INT32),
        FieldSchema(name="category_l2", dtype=DataType.INT32),
        FieldSchema(name="color_code", dtype=DataType.INT32),
        # 弱打分标签
        FieldSchema(name="material_code", dtype=DataType.INT32),
        FieldSchema(name="pattern_code", dtype=DataType.INT32),
        FieldSchema(name="style_code", dtype=DataType.INT32),
        FieldSchema(name="sleeve_code", dtype=DataType.INT32),
        FieldSchema(name="season_code", dtype=DataType.INT32),
        FieldSchema(name="gender_code", dtype=DataType.INT32),
        # 辅助
        FieldSchema(name="scene_type", dtype=DataType.INT32),
        FieldSchema(name="quality_score", dtype=DataType.INT32),
        # 业务
        FieldSchema(name="brand_id", dtype=DataType.INT32),
        FieldSchema(name="merchant_set_id", dtype=DataType.INT64),
        FieldSchema(name="ts_month", dtype=DataType.INT32),
        FieldSchema(name="created_at", dtype=DataType.INT64),
    ]
    schema = CollectionSchema(fields, description="H4 v0.3 SSCD双向量+强弱标签")
    coll = Collection(COLLECTION, schema=schema, shards_num=4)
    print(f"✅ 创建: {COLLECTION} ({len(fields)} 字段)")

    # HNSW_SQ M=24 (v0.3)
    idx = {"index_type":"HNSW_SQ","metric_type":"COSINE",
           "params":{"M":24,"efConstruction":200,"sq_type":"SQ8","refine":True,"refine_type":"FP16"}}
    coll.create_index("sscd_vec", idx); print("  sscd_vec HNSW_SQ M=24 ✅")
    coll.create_index("center_vec", idx); print("  center_vec HNSW_SQ M=24 ✅")
    coll.create_index("phash", {"index_type":"BIN_FLAT","metric_type":"HAMMING"}); print("  phash ✅")
    coll.create_index("dhash", {"index_type":"BIN_FLAT","metric_type":"HAMMING"}); print("  dhash ✅")
    for f in ["category_l1","category_l2","color_code","brand_id"]:
        coll.create_index(f, {"index_type":"STL_SORT"}); print(f"  {f} STL_SORT ✅")
    print(f"✅ 完成")

if __name__ == "__main__":
    import sys
    create(drop="--drop" in sys.argv)
