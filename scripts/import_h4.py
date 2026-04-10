#!/usr/bin/env python3
"""
导入 H4 双模型推理结果到 h4_main Collection
- 读 /data/imgsrch/test_h4/dual_gpu*.jsonl
- 同时写入 dinov2_vec + clip_vec + 标量字段
- 模拟 phash/dhash (zero), brand_id, merchant_ids (random)
"""
import json
import os
import glob
import time
import random
import hashlib
import numpy as np
from PIL import Image
from pymilvus import connections, Collection

COLLECTION = "h4_main"
INFER_DIR = "/data/imgsrch/test_h4"
IMAGE_DIR = "/data/test-images"
BATCH_SIZE = 500
NUM_BRANDS = 1000
NUM_MERCHANTS = 5000


def fake_metadata(image_pk):
    rng = random.Random(int(image_pk[:8], 16))
    return {
        "brand_id": rng.randint(1, NUM_BRANDS),
        "merchant_ids": rng.sample(range(1, NUM_MERCHANTS + 1), k=rng.randint(1, 10)),
        "ts_month": 202604,
        "created_at": int(time.time()),
    }


def main():
    print("连接 Milvus...")
    connections.connect(host="localhost", port="19530")
    coll = Collection(COLLECTION)
    print(f"  Collection: {COLLECTION}, 已有 {coll.num_entities:,}")

    jsonl_files = sorted(glob.glob(os.path.join(INFER_DIR, "dual_gpu*.jsonl")))
    print(f"  推理文件: {len(jsonl_files)}")

    # 收集器
    batch = {
        "image_pk": [], "phash": [], "dhash": [],
        "dinov2_vec": [], "clip_vec": [], "url_hash": [],
        "ts_month": [], "category_l1": [], "category_l2": [],
        "brand_id": [], "merchant_ids": [], "created_at": [],
    }

    total_inserted = 0
    t_start = time.time()
    last_log = time.time()
    zero_hash = b'\x00' * 8  # 临时 zero phash/dhash

    for jsonl in jsonl_files:
        with open(jsonl) as f:
            for line in f:
                rec = json.loads(line)
                pk = rec["image_pk"]
                meta = fake_metadata(pk)

                batch["image_pk"].append(pk)
                batch["phash"].append(zero_hash)
                batch["dhash"].append(zero_hash)
                batch["dinov2_vec"].append(rec["dinov2_vec"])
                batch["clip_vec"].append(rec["clip_vec"])
                batch["url_hash"].append(hashlib.sha256(pk.encode()).hexdigest()[:32])
                batch["ts_month"].append(meta["ts_month"])
                batch["category_l1"].append(rec.get("category_l1", 0))
                batch["category_l2"].append(rec.get("category_l2", 0))
                batch["brand_id"].append(meta["brand_id"])
                batch["merchant_ids"].append(meta["merchant_ids"])
                batch["created_at"].append(meta["created_at"])

                if len(batch["image_pk"]) >= BATCH_SIZE:
                    try:
                        coll.insert([
                            batch["image_pk"], batch["phash"], batch["dhash"],
                            batch["dinov2_vec"], batch["clip_vec"],
                            batch["url_hash"], batch["ts_month"],
                            batch["category_l1"], batch["category_l2"],
                            batch["brand_id"], batch["merchant_ids"],
                            batch["created_at"],
                        ])
                        total_inserted += len(batch["image_pk"])
                    except Exception as e:
                        print(f"  插入错误: {str(e)[:100]}")
                    # 清空
                    for k in batch:
                        batch[k].clear()

                    now = time.time()
                    if now - last_log > 10:
                        elapsed = now - t_start
                        rate = total_inserted / max(elapsed, 1)
                        print(f"  [{total_inserted:,}] {rate:.0f}/s {elapsed:.0f}s")
                        last_log = now

    # 末批
    if batch["image_pk"]:
        coll.insert([
            batch["image_pk"], batch["phash"], batch["dhash"],
            batch["dinov2_vec"], batch["clip_vec"],
            batch["url_hash"], batch["ts_month"],
            batch["category_l1"], batch["category_l2"],
            batch["brand_id"], batch["merchant_ids"],
            batch["created_at"],
        ])
        total_inserted += len(batch["image_pk"])

    coll.flush()
    elapsed = time.time() - t_start
    print(f"\n✅ 导入完成: {total_inserted:,} 条, {elapsed:.0f}s ({total_inserted/elapsed:.0f}/s)")
    print(f"   Collection: {coll.num_entities:,}")


if __name__ == "__main__":
    main()
