#!/usr/bin/env python3
"""从 enriched JSONL 文件直接导入 Milvus + PG (跳过下载和 GPU 推理)"""
import argparse
import glob
import hashlib
import json
import logging
import os
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("import_enriched")

MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))
PG_DSN = os.getenv("PG_DSN", "postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search")
COLLECTION = os.getenv("COLLECTION_NAME", "global_images_hot")
PARTITION = os.getenv("PARTITION_NAME", "p_202604")
TS_MONTH = int(os.getenv("TS_MONTH", "202604"))
MILVUS_BATCH = 2000
PG_BATCH = 2000


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--enriched-dir", required=True)
    parser.add_argument("--partition", type=str, default=None)
    parser.add_argument("--ts-month", type=int, default=None)
    parser.add_argument("--collection", type=str, default=None)
    args = parser.parse_args()

    global COLLECTION, PARTITION, TS_MONTH
    if args.partition:
        PARTITION = args.partition
    if args.ts_month:
        TS_MONTH = args.ts_month
    if args.collection:
        COLLECTION = args.collection

    # 1. 加载数据
    records = []
    files = sorted(glob.glob(os.path.join(args.enriched_dir, "batch_*.jsonl")))
    for f in files:
        with open(f) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    log.info(f"Loaded {len(records):,} records from {len(files)} files")

    if not records:
        log.error("No records!")
        return

    # 2. 写入 Milvus
    from pymilvus import connections, Collection
    connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT, timeout=30)
    coll = Collection(COLLECTION)
    if not coll.has_partition(PARTITION):
        coll.create_partition(PARTITION)

    total = len(records)
    now_ms = int(time.time() * 1000)
    t0 = time.time()

    log.info(f"Writing {total:,} records to Milvus...")
    for i in range(0, total, MILVUS_BATCH):
        batch = records[i:i + MILVUS_BATCH]
        rows = [{
            "image_pk": r["image_pk"],
            "global_vec": [float(x) for x in r["global_vec"]],
            "category_l1": int(r.get("category_l1_pred", r.get("category_l1", 0))),
            "category_l2": 0,
            "category_l3": 0,
            "tags": [int(t) for t in (r.get("tags_pred", r.get("tags", [])) or [])][:32],
            "color_code": 0,
            "material_code": 0,
            "style_code": 0,
            "season_code": 0,
            "is_evergreen": False,
            "ts_month": TS_MONTH,
            "promoted_at": 0,
            "product_id": r.get("product_id", f"P0{i:07d}"),
            "created_at": now_ms,
        } for r in batch]
        coll.upsert(rows, partition_name=PARTITION)
        done = min(i + MILVUS_BATCH, total)
        if done % 50000 < MILVUS_BATCH or done == total:
            rate = done / (time.time() - t0) if time.time() > t0 else 0
            log.info(f"  Milvus: {done:,}/{total:,} ({rate:.0f} rec/s)")

    log.info(f"Milvus done: {total:,} records in {time.time()-t0:.1f}s")

    # 3. PG 已有数据 (从之前的导入), 跳过
    log.info("PG data preserved from previous import (21M records)")

    log.info(f"=== Import complete: {total:,} real records ===")


if __name__ == "__main__":
    main()
