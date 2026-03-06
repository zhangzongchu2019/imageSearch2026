"""
初始化 Milvus collections — global_images_hot, global_images_non_hot, sub_images
用于首次部署时创建 schema + 索引 + 初始分区
"""
import sys
import time
from pymilvus import (
    connections,
    Collection,
    CollectionSchema,
    FieldSchema,
    DataType,
    utility,
)

MILVUS_HOST = sys.argv[1] if len(sys.argv) > 1 else "milvus-standalone"
MILVUS_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 19530


def connect():
    for attempt in range(30):
        try:
            connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT)
            print(f"Connected to Milvus at {MILVUS_HOST}:{MILVUS_PORT}")
            return
        except Exception as e:
            print(f"Attempt {attempt+1}/30: Milvus not ready ({e}), retrying...")
            time.sleep(2)
    raise RuntimeError("Cannot connect to Milvus after 30 attempts")


def create_global_collection(name: str, index_type: str):
    """Create global_images_hot or global_images_non_hot"""
    if utility.has_collection(name):
        print(f"Collection '{name}' already exists, skipping.")
        return Collection(name)

    fields = [
        FieldSchema("image_pk", DataType.VARCHAR, is_primary=True, max_length=32),
        FieldSchema("global_vec", DataType.FLOAT_VECTOR, dim=256),
        FieldSchema("category_l1", DataType.INT32),
        FieldSchema("category_l2", DataType.INT32),
        FieldSchema("category_l3", DataType.INT32),
        FieldSchema("tags", DataType.ARRAY, element_type=DataType.INT32, max_capacity=32),
        FieldSchema("color_code", DataType.INT32),
        FieldSchema("material_code", DataType.INT32),
        FieldSchema("style_code", DataType.INT32),
        FieldSchema("season_code", DataType.INT32),
        FieldSchema("is_evergreen", DataType.BOOL),
        FieldSchema("ts_month", DataType.INT32),
        FieldSchema("promoted_at", DataType.INT64),
        FieldSchema("product_id", DataType.VARCHAR, max_length=64),
        FieldSchema("created_at", DataType.INT64),
    ]
    schema = CollectionSchema(fields, description=f"Image search {name}")
    coll = Collection(name, schema)
    print(f"Created collection '{name}'")

    # Create vector index
    if index_type == "HNSW":
        index_params = {
            "metric_type": "COSINE",
            "index_type": "HNSW",
            "params": {"M": 24, "efConstruction": 200},
        }
    else:
        index_params = {
            "metric_type": "COSINE",
            "index_type": "DISKANN",
            "params": {},
        }
    coll.create_index("global_vec", index_params)
    print(f"  Created {index_type} index on global_vec")

    # Create scalar indices
    for field in ["category_l1", "category_l2", "category_l3",
                  "color_code", "material_code", "style_code",
                  "season_code", "ts_month", "is_evergreen"]:
        coll.create_index(field, {"index_type": "INVERTED"})
    coll.create_index("tags", {"index_type": "INVERTED"})
    print(f"  Created INVERTED indices on scalar fields")

    # Create initial partitions (current month + a few months)
    from datetime import datetime, timedelta
    now = datetime.utcnow()
    for offset in range(-2, 4):
        dt = now.replace(day=1) + timedelta(days=32 * offset)
        part_name = f"p_{dt.strftime('%Y%m')}"
        try:
            coll.create_partition(part_name)
            print(f"  Created partition {part_name}")
        except Exception:
            pass  # partition may already exist
    # Evergreen partition
    try:
        coll.create_partition("p_999999")
        print(f"  Created partition p_999999 (evergreen)")
    except Exception:
        pass

    coll.load()
    print(f"  Collection '{name}' loaded into memory")
    return coll


def create_sub_collection():
    name = "sub_images"
    if utility.has_collection(name):
        print(f"Collection '{name}' already exists, skipping.")
        return Collection(name)

    fields = [
        FieldSchema("sub_pk", DataType.VARCHAR, is_primary=True, max_length=48),
        FieldSchema("image_pk", DataType.VARCHAR, max_length=32),
        FieldSchema("sub_vec", DataType.FLOAT_VECTOR, dim=128),
        FieldSchema("bbox", DataType.VARCHAR, max_length=64),
        FieldSchema("confidence", DataType.FLOAT),
        FieldSchema("ts_month", DataType.INT32),
        FieldSchema("is_evergreen", DataType.BOOL),
    ]
    schema = CollectionSchema(fields, description="Sub-image vectors")
    coll = Collection(name, schema)
    print(f"Created collection '{name}'")

    # IVF_PQ index
    index_params = {
        "metric_type": "COSINE",
        "index_type": "IVF_FLAT",
        "params": {"nlist": 128},
    }
    coll.create_index("sub_vec", index_params)
    print(f"  Created IVF_FLAT index on sub_vec")

    coll.create_index("ts_month", {"index_type": "INVERTED"})
    coll.create_index("is_evergreen", {"index_type": "INVERTED"})
    print(f"  Created scalar indices")

    coll.load()
    print(f"  Collection '{name}' loaded into memory")
    return coll


if __name__ == "__main__":
    connect()
    create_global_collection("global_images_hot", "HNSW")
    create_global_collection("global_images_non_hot", "DISKANN")
    create_sub_collection()
    print("\nMilvus initialization complete!")
