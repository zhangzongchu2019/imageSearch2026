#!/usr/bin/env python3
"""
导入 DINOv2 推理结果到 H3 测试 Collection
- 读取 /data/imgsrch/test_dinov2/dinov2_gpu*.jsonl
- 生成 phash/dhash (PIL + scipy)
- 模拟随机 brand_id 和 merchant_ids (用于过滤测试)
- 批量插入 Milvus

用法:
  python3 import_h3_test.py [--max N]
"""
import argparse
import json
import os
import glob
import random
import hashlib
import time
import numpy as np
from PIL import Image
from pymilvus import connections, Collection

COLLECTION = "test_dinov2_h3"
INFER_DIR = "/data/imgsrch/test_dinov2"
IMAGE_DIR = "/data/test-images"
BATCH_SIZE = 500
NUM_BRANDS = 1000
NUM_MERCHANTS = 5000


def compute_hashes(img):
    """计算 pHash 和 dHash"""
    # pHash (32x32 → DCT → 8x8)
    img_gray = img.convert("L").resize((32, 32), Image.BICUBIC)
    arr = np.array(img_gray, dtype=np.float32)
    try:
        from scipy.fft import dct
        dct_arr = dct(dct(arr, axis=0), axis=1)
        low_freq = dct_arr[:8, :8]
        median = np.median(low_freq)
        phash_bits = (low_freq > median).astype(np.uint8).flatten()
    except ImportError:
        # 回退到简单平均哈希
        small = img.convert("L").resize((8, 8), Image.BICUBIC)
        arr8 = np.array(small, dtype=np.float32)
        phash_bits = (arr8 > arr8.mean()).astype(np.uint8).flatten()

    # dHash (9x8 → 横向相邻差异)
    small = img.convert("L").resize((9, 8), Image.BICUBIC)
    arr8 = np.array(small, dtype=np.uint8)
    dhash_bits = (arr8[:, 1:] > arr8[:, :-1]).astype(np.uint8).flatten()

    # 64 bit → 8 bytes
    return np.packbits(phash_bits).tobytes(), np.packbits(dhash_bits).tobytes()


def fake_metadata(image_pk):
    """模拟 brand/category/merchant 元数据"""
    rng = random.Random(int(image_pk[:8], 16))
    return {
        "brand_id": rng.randint(1, NUM_BRANDS),
        "category_l1": rng.choice([1001, 1002, 1003, 1004, 1007]),  # 服装/箱包/鞋帽/珠宝/家具
        "category_l2": rng.randint(100101, 100208),
        "merchant_ids": rng.sample(range(1, NUM_MERCHANTS + 1), k=rng.randint(1, 10)),
        "ts_month": 202604,
        "created_at": int(time.time()),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=None, help="最多导入 N 条")
    parser.add_argument("--skip-hash", action="store_true", help="跳过 phash/dhash 计算 (用零值)")
    args = parser.parse_args()

    print(f"连接 Milvus...")
    connections.connect(host="localhost", port="19530")
    coll = Collection(COLLECTION)
    print(f"  Collection: {COLLECTION}, 已有 {coll.num_entities:,} 条")

    # 收集所有 jsonl 文件
    jsonl_files = sorted(glob.glob(os.path.join(INFER_DIR, "dinov2_gpu*.jsonl")))
    print(f"  推理文件: {jsonl_files}")

    # 流式读取 + 批量插入
    batch_pks = []
    batch_phashes = []
    batch_dhashes = []
    batch_vecs = []
    batch_l1 = []
    batch_l2 = []
    batch_brand = []
    batch_merchants = []
    batch_ts = []
    batch_created = []

    total_inserted = 0
    t_start = time.time()
    last_log = time.time()

    for jsonl in jsonl_files:
        with open(jsonl) as f:
            for line in f:
                if args.max and total_inserted + len(batch_pks) >= args.max:
                    break

                rec = json.loads(line)
                pk = rec["image_pk"]
                vec = rec["dinov2_vec"]

                # 加载图片算 hash
                if args.skip_hash:
                    phash = b'\x00' * 8
                    dhash = b'\x00' * 8
                else:
                    img_path = os.path.join(IMAGE_DIR, f"{pk}.jpg")
                    try:
                        img = Image.open(img_path).convert("RGB")
                        phash, dhash = compute_hashes(img)
                    except Exception:
                        phash = b'\x00' * 8
                        dhash = b'\x00' * 8

                # 元数据
                meta = fake_metadata(pk)

                batch_pks.append(pk)
                batch_phashes.append(phash)
                batch_dhashes.append(dhash)
                batch_vecs.append(vec)
                batch_l1.append(meta["category_l1"])
                batch_l2.append(meta["category_l2"])
                batch_brand.append(meta["brand_id"])
                batch_merchants.append(meta["merchant_ids"])
                batch_ts.append(meta["ts_month"])
                batch_created.append(meta["created_at"])

                if len(batch_pks) >= BATCH_SIZE:
                    # 插入
                    try:
                        coll.insert([
                            batch_pks, batch_phashes, batch_dhashes, batch_vecs,
                            batch_l1, batch_l2, batch_brand, batch_merchants,
                            batch_ts, batch_created
                        ])
                        total_inserted += len(batch_pks)
                    except Exception as e:
                        print(f"  插入错误: {str(e)[:100]}")

                    batch_pks.clear()
                    batch_phashes.clear()
                    batch_dhashes.clear()
                    batch_vecs.clear()
                    batch_l1.clear()
                    batch_l2.clear()
                    batch_brand.clear()
                    batch_merchants.clear()
                    batch_ts.clear()
                    batch_created.clear()

                    # 日志
                    now = time.time()
                    if now - last_log > 5:
                        elapsed = now - t_start
                        rate = total_inserted / max(elapsed, 1)
                        print(f"  [{total_inserted:,}] {rate:.0f}/s elapsed {elapsed:.0f}s")
                        last_log = now

            if args.max and total_inserted + len(batch_pks) >= args.max:
                break

    # 末批
    if batch_pks:
        coll.insert([
            batch_pks, batch_phashes, batch_dhashes, batch_vecs,
            batch_l1, batch_l2, batch_brand, batch_merchants,
            batch_ts, batch_created
        ])
        total_inserted += len(batch_pks)

    coll.flush()
    elapsed = time.time() - t_start
    print(f"\n✅ 导入完成: {total_inserted:,} 条, {elapsed:.0f}s ({total_inserted/elapsed:.0f}/s)")
    print(f"   Collection: {coll.num_entities:,} 条")


if __name__ == "__main__":
    main()
