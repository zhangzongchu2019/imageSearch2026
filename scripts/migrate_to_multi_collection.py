#!/usr/bin/env python3
"""
架构迁移: global_images_hot (单Collection多Partition)
        → 独立 Collection (每分区一个)

迁移: p_202604 → img_202604_vip
      p_202604_svip → img_202604_svip

每个新 Collection 独立建索引, 互不影响。
"""
import argparse
import logging
import time

from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
    utility,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

MILVUS_HOST = "localhost"
MILVUS_PORT = 19530
OLD_COLLECTION = "global_images_hot"
QUERY_BATCH = 10000  # Milvus iterator max batch = 16384
INSERT_BATCH = 5000

MIGRATIONS = [
    {"src_partition": "p_202604", "dst_collection": "img_202604_vip"},
    {"src_partition": "p_202604_svip", "dst_collection": "img_202604_svip"},
]

FIELDS = [
    "image_pk", "global_vec", "category_l1", "category_l2", "category_l3",
    "tags", "color_code", "material_code", "style_code", "season_code",
    "is_evergreen", "ts_month", "promoted_at", "product_id", "created_at",
]


def create_collection(name: str) -> Collection:
    """Create a new collection with the same schema as the old one."""
    if utility.has_collection(name):
        log.info(f"Collection {name} already exists, using it")
        return Collection(name)

    fields = [
        FieldSchema("image_pk", DataType.VARCHAR, is_primary=True, max_length=64),
        FieldSchema("global_vec", DataType.FLOAT_VECTOR, dim=256),
        FieldSchema("category_l1", DataType.INT32),
        FieldSchema("category_l2", DataType.INT32),
        FieldSchema("category_l3", DataType.INT32),
        FieldSchema("tags", DataType.ARRAY, element_type=DataType.INT32, max_capacity=20),
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
    schema = CollectionSchema(fields, description=f"Physical partition: {name}")
    coll = Collection(name, schema)
    log.info(f"Created collection: {name}")
    return coll


def build_indexes(coll: Collection):
    """Build HNSW + scalar indexes on a collection."""
    name = coll.name
    t0 = time.time()

    log.info(f"[{name}] Building HNSW index...")
    coll.create_index("global_vec", {
        "index_type": "HNSW",
        "metric_type": "COSINE",
        "params": {"M": 24, "efConstruction": 200},
    })
    log.info(f"[{name}] HNSW done in {time.time()-t0:.1f}s")

    scalar_fields = [
        "category_l1", "category_l2", "category_l3", "color_code",
        "material_code", "style_code", "season_code", "is_evergreen",
        "tags", "ts_month",
    ]
    for field in scalar_fields:
        coll.create_index(field, {"index_type": "INVERTED"}, _async=True)
    log.info(f"[{name}] Scalar indexes submitted")

    # Wait and verify
    time.sleep(5)
    coll2 = Collection(name)
    log.info(f"[{name}] Indexes: {len(coll2.indexes)}/11, "
             f"total time {time.time()-t0:.1f}s")


def migrate_partition(src_partition: str, dst_collection_name: str, dry_run: bool = False):
    """Migrate data from a partition in old collection to a new collection."""
    old_coll = Collection(OLD_COLLECTION)

    # Get source count
    old_coll.load(partition_names=[src_partition])
    time.sleep(3)

    count_result = old_coll.query(
        'image_pk != ""',
        partition_names=[src_partition],
        output_fields=["count(*)"],
    )
    total = count_result[0]["count(*)"]
    log.info(f"[{src_partition} → {dst_collection_name}] Source: {total:,} records")

    if dry_run:
        log.info("[DRY RUN] Skipping actual migration")
        old_coll.release()
        return

    # Create destination
    dst_coll = create_collection(dst_collection_name)

    # Use query iterator for large datasets (avoids offset+limit > 16384 limitation)
    migrated = 0
    t0 = time.time()

    iterator = old_coll.query_iterator(
        expr='image_pk != ""',
        partition_names=[src_partition],
        output_fields=FIELDS,
        batch_size=QUERY_BATCH,
    )

    while True:
        results = iterator.next()
        if not results:
            break

        # Insert in sub-batches with retry
        for i in range(0, len(results), INSERT_BATCH):
            batch = results[i:i + INSERT_BATCH]
            rows = [{f: r[f] for f in FIELDS} for r in batch]
            for attempt in range(5):
                try:
                    dst_coll.upsert(rows)
                    break
                except Exception as e:
                    log.warning(f"  upsert failed (attempt {attempt+1}/5): {e}")
                    time.sleep(10 * (attempt + 1))
                    if attempt == 4:
                        log.error(f"  upsert failed after 5 attempts, skipping batch")
            else:
                continue

        migrated += len(results)
        elapsed = time.time() - t0
        rate = migrated / elapsed if elapsed > 0 else 0
        if migrated % 500000 < QUERY_BATCH or migrated >= total:
            log.info(f"  [{dst_collection_name}] {migrated:,}/{total:,} "
                     f"({migrated*100/total:.1f}%) [{rate:,.0f} rec/s]")

    iterator.close()
    # 不 release 旧 collection，避免并行迁移时互相影响

    elapsed = time.time() - t0
    log.info(f"[{dst_collection_name}] Migration done: {migrated:,} records "
             f"in {elapsed:.1f}s ({migrated/elapsed:,.0f} rec/s)")

    # Verify
    dst_count = dst_coll.num_entities
    log.info(f"[{dst_collection_name}] Verified: {dst_count:,} records")

    # Build indexes
    log.info(f"[{dst_collection_name}] Building indexes...")
    build_indexes(dst_coll)

    return migrated


def main():
    parser = argparse.ArgumentParser(description="Migrate to multi-collection architecture")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", type=str, default=None,
                        help="Only migrate specific partition (e.g. p_202604)")
    args = parser.parse_args()

    connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT, timeout=30)
    log.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 架构迁移开始")
    log.info(f"  源: {OLD_COLLECTION}")
    log.info(f"  目标: {[m['dst_collection'] for m in MIGRATIONS]}")

    t0 = time.time()
    for m in MIGRATIONS:
        if args.only and m["src_partition"] != args.only:
            continue
        migrate_partition(m["src_partition"], m["dst_collection"], dry_run=args.dry_run)

    elapsed = time.time() - t0
    log.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 架构迁移完成, 总耗时 {elapsed:.1f}s")

    # Final summary
    log.info("=== 迁移后状态 ===")
    for m in MIGRATIONS:
        name = m["dst_collection"]
        if utility.has_collection(name):
            c = Collection(name)
            log.info(f"  {name}: {c.num_entities:,} records, {len(c.indexes)} indexes")

    # Old collection
    old = Collection(OLD_COLLECTION)
    log.info(f"  {OLD_COLLECTION} (旧): {old.num_entities:,} records (可后续删除)")


if __name__ == "__main__":
    main()
