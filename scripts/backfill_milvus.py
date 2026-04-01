#!/usr/bin/env python3
"""
从 enriched JSONL 文件初始化 Milvus collection 并导入存量数据。

用法:
    python scripts/backfill_milvus.py [--milvus-host localhost] [--data-dir data/features_enriched]

步骤:
  1. 初始化 Milvus collection schema + 索引 (如果不存在)
  2. 读取 enriched JSONL 文件 (含 256 维向量 + 品类/标签)
  3. 批量 upsert 到 global_images_hot
"""
import argparse
import glob
import json
import logging
import os
import random
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── 默认配置 ──
MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))
COLLECTION_NAME = "global_images_hot"
PARTITION_NAME = "p_202603"
TS_MONTH = 202603
MILVUS_BATCH_SIZE = 5000
MERCHANT_ID_START = 10001
MERCHANT_ID_END = 12999

# 品类名 → INT 映射 (与 CLIPFeatureExtractor.CATEGORY_ID_MAP 一致)
CATEGORY_NAME_TO_ID = {
    "服装": 0,
    "鞋帽": 1,
    "箱包": 2,
    "珠宝玉器": 3,
    "首饰配饰": 4,
}


def init_milvus_collection(host: str, port: int):
    """初始化 Milvus collection schema + 索引 (幂等)"""
    from pymilvus import (
        connections, Collection, CollectionSchema, FieldSchema,
        DataType, utility,
    )

    log.info(f"Connecting to Milvus at {host}:{port}...")
    connections.connect("default", host=host, port=port)

    if utility.has_collection(COLLECTION_NAME):
        log.info(f"Collection '{COLLECTION_NAME}' already exists")
        coll = Collection(COLLECTION_NAME)
        coll.load()
        return coll

    log.info(f"Creating collection '{COLLECTION_NAME}'...")
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
    schema = CollectionSchema(fields, description="Image search global_images_hot")
    coll = Collection(COLLECTION_NAME, schema)

    # HNSW 向量索引
    coll.create_index("global_vec", {
        "metric_type": "COSINE",
        "index_type": "HNSW",
        "params": {"M": 24, "efConstruction": 200},
    })
    log.info("  Created HNSW index on global_vec")

    # 标量倒排索引
    for field in ["category_l1", "category_l2", "category_l3",
                  "color_code", "material_code", "style_code",
                  "season_code", "ts_month", "is_evergreen"]:
        coll.create_index(field, {"index_type": "INVERTED"})
    coll.create_index("tags", {"index_type": "INVERTED"})
    log.info("  Created INVERTED indices on scalar fields")

    # 分区
    coll.create_partition(PARTITION_NAME)
    try:
        coll.create_partition("p_999999")
    except Exception:
        pass
    log.info(f"  Created partition {PARTITION_NAME}")

    coll.load()
    log.info(f"  Collection '{COLLECTION_NAME}' ready")
    return coll


def load_enriched_records(data_dir: str) -> list[dict]:
    """读取所有 enriched JSONL 文件"""
    files = sorted(glob.glob(os.path.join(data_dir, "batch_*.jsonl")))
    if not files:
        log.error(f"No batch_*.jsonl files found in {data_dir}")
        sys.exit(1)

    records = []
    for f in files:
        with open(f) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    log.info(f"Loaded {len(records)} records from {len(files)} files")
    return records


def backfill(coll, records: list[dict]):
    """批量 upsert enriched 记录到 Milvus"""
    total = len(records)
    now_ms = int(time.time() * 1000)

    log.info(f"Upserting {total} records (batch={MILVUS_BATCH_SIZE})...")
    t0 = time.time()

    for i in range(0, total, MILVUS_BATCH_SIZE):
        batch = records[i: i + MILVUS_BATCH_SIZE]
        rows = []
        for idx, r in enumerate(batch):
            # category_l1: 优先用已有的 INT 字段, 否则从名称映射
            cat_id = r.get("category_l1_pred")
            if cat_id is None:
                cat_name = r.get("category_l1", "")
                cat_id = CATEGORY_NAME_TO_ID.get(cat_name, 0)

            # tags: 优先用 tag_ids (INT), 否则 hash 编码
            tag_ids = r.get("tag_ids") or r.get("tags_pred") or []
            if tag_ids and isinstance(tag_ids[0], str):
                tag_ids = [hash(t) % 4096 for t in tag_ids]
            # Milvus max_capacity=32
            tag_ids = [int(t) for t in tag_ids[:32]]

            global_idx = i + idx
            merchant_num = random.randint(MERCHANT_ID_START, MERCHANT_ID_END)

            rows.append({
                "image_pk": r["image_pk"],
                "global_vec": r["global_vec"],
                "product_id": r.get("product_id", f"P0{global_idx:07d}"),
                "is_evergreen": False,
                "category_l1": int(cat_id),
                "category_l2": 0,
                "category_l3": 0,
                "tags": tag_ids,
                "color_code": 0,
                "material_code": 0,
                "style_code": 0,
                "season_code": 0,
                "ts_month": TS_MONTH,
                "promoted_at": 0,
                "created_at": now_ms,
            })

        coll.upsert(rows, partition_name=PARTITION_NAME)
        done = min(i + MILVUS_BATCH_SIZE, total)
        elapsed = time.time() - t0
        rate = done / elapsed if elapsed > 0 else 0
        log.info(f"  Upserted {done}/{total} ({rate:.0f} rec/s)")

    log.info("Flushing Milvus...")
    coll.flush()
    log.info(f"Done. Collection entity count: {coll.num_entities}")


def backfill_pg(records: list[dict], pg_dsn: str):
    """批量导入 uri_dedup 到 PostgreSQL"""
    import hashlib
    import psycopg2
    from psycopg2.extras import execute_values

    log.info(f"Connecting to PostgreSQL...")
    conn = psycopg2.connect(pg_dsn)
    cur = conn.cursor()

    PG_BATCH = 2000
    total = len(records)
    inserted = 0
    t0 = time.time()

    log.info(f"Inserting {total} records into uri_dedup...")
    for i in range(0, total, PG_BATCH):
        batch = records[i: i + PG_BATCH]
        values = []
        for r in batch:
            url = r.get("url", "")
            image_pk = r["image_pk"]
            uri_hash = hashlib.sha256(url.encode()).hexdigest()
            values.append((image_pk, uri_hash, url, TS_MONTH))
        execute_values(
            cur,
            "INSERT INTO uri_dedup (image_pk, uri_hash, uri, ts_month) VALUES %s "
            "ON CONFLICT (image_pk) DO UPDATE SET uri = EXCLUDED.uri",
            values,
        )
        inserted += len(batch)
        rate = inserted / (time.time() - t0) if time.time() > t0 else 0
        log.info(f"  PG insert {min(inserted, total)}/{total} ({rate:.0f} rec/s)")

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"PostgreSQL insert done: {inserted} records")


def print_stats(coll):
    """统计品类分布"""
    from pymilvus import Collection
    results = []
    for cat_id, cat_name in enumerate(CATEGORY_NAME_TO_ID.keys()):
        count_res = coll.query(expr=f"category_l1 == {cat_id}", output_fields=["count(*)"])
        count = count_res[0]["count(*)"] if count_res else 0
        results.append((cat_name, cat_id, count))
    log.info("Category distribution:")
    for name, cid, count in results:
        log.info(f"  {name} (id={cid}): {count}")


PG_DSN = os.getenv("PG_DSN", "postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search")


def main():
    parser = argparse.ArgumentParser(description="Backfill Milvus + PG from enriched JSONL")
    parser.add_argument("--milvus-host", default=MILVUS_HOST)
    parser.add_argument("--milvus-port", type=int, default=MILVUS_PORT)
    parser.add_argument("--pg-dsn", default=PG_DSN)
    parser.add_argument("--data-dir", default="data/features_enriched")
    parser.add_argument("--skip-milvus", action="store_true", help="Skip Milvus (already imported)")
    parser.add_argument("--skip-pg", action="store_true", help="Skip PostgreSQL")
    parser.add_argument("--skip-stats", action="store_true")
    args = parser.parse_args()

    records = load_enriched_records(args.data_dir)

    if not args.skip_milvus:
        coll = init_milvus_collection(args.milvus_host, args.milvus_port)
        backfill(coll, records)
        if not args.skip_stats:
            print_stats(coll)
    else:
        log.info("Skipping Milvus (--skip-milvus)")

    if not args.skip_pg:
        backfill_pg(records, args.pg_dsn)
    else:
        log.info("Skipping PostgreSQL (--skip-pg)")


if __name__ == "__main__":
    main()
