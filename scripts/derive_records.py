#!/usr/bin/env python3
"""
从真实记录衍生 100 万条 Milvus/PG 记录。

策略: 对每条真实记录生成 N 个变体
  - 向量: 原始 256 维 + 高斯噪声 (σ=0.03), 重新 L2 归一化
  - image_pk: sha256(original_pk + "_v{i}")[:16].hex()
  - product_id: P1{index:07d}
  - merchant_id: 扩展商家范围 10001-15999
  - 品类/标签: 80% 不变, 20% 微调

Usage:
    python3 scripts/derive_records.py --target 1000000
    python3 scripts/derive_records.py --target 1000000 --extra-dir data/batch2_backup
"""
import argparse
import glob
import hashlib
import json
import logging
import os
import random
import sys
import time

import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("derive_records")

# ── 配置 ──
MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))
PG_DSN = os.getenv("PG_DSN", "postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search")
COLLECTION_NAME = "global_images_hot"
PARTITION_NAME = "p_202603"
TS_MONTH = 202603
MILVUS_BATCH_SIZE = 5000
PG_BATCH_SIZE = 2000
NOISE_SIGMA = 0.03
MERCHANT_RANGE = (10001, 15999)

CATEGORY_NAME_TO_ID = {
    "服装": 0, "鞋帽": 1, "箱包": 2, "珠宝玉器": 3, "首饰配饰": 4,
}
CATEGORY_ID_TO_NAME = {v: k for k, v in CATEGORY_NAME_TO_ID.items()}


def load_source_records(enriched_dir: str, extra_dir: str = None) -> list[dict]:
    """加载原始 enriched JSONL + 可选的 batch2 backup"""
    records = []

    # 主数据源: enriched JSONL
    files = sorted(glob.glob(os.path.join(enriched_dir, "batch_*.jsonl")))
    for f in files:
        with open(f) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    log.info(f"Loaded {len(records)} from {enriched_dir} ({len(files)} files)")

    # 额外数据源: batch2 backup
    if extra_dir:
        meta_file = os.path.join(extra_dir, "metadata.jsonl")
        if os.path.exists(meta_file):
            count = 0
            with open(meta_file) as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        r = json.loads(line)
                        if r.get("global_vec"):
                            records.append(r)
                            count += 1
            log.info(f"Loaded {count} from {meta_file}")
        else:
            log.warning(f"Extra dir specified but {meta_file} not found")

    log.info(f"Total source records: {len(records)}")
    return records


def generate_variant_pk(original_pk: str, variant_idx: int) -> str:
    """生成变体的唯一 image_pk"""
    raw = f"{original_pk}_v{variant_idx}"
    return hashlib.sha256(raw.encode()).digest()[:16].hex()


def generate_variants(records: list[dict], target_total: int) -> list[dict]:
    """生成足够的变体记录达到 target_total"""
    existing_count = len(records)
    need = target_total - existing_count
    if need <= 0:
        log.info(f"Already have {existing_count} >= target {target_total}, no variants needed")
        return []

    variants_per_record = need // existing_count
    extra = need % existing_count
    log.info(f"Generating {need} variants: {variants_per_record} per record "
             f"+ {extra} extra from first records")

    rng = np.random.RandomState(2026)
    py_rng = random.Random(2026)
    all_tag_ids = list(range(100, 4096))

    variants = []
    global_idx = 0
    t0 = time.time()

    for rec_idx, rec in enumerate(records):
        # 这条记录要生成几个变体
        n_variants = variants_per_record + (1 if rec_idx < extra else 0)
        if n_variants == 0:
            continue

        vec = np.array(rec["global_vec"], dtype=np.float32)

        # 获取原始品类和标签
        cat_id = rec.get("category_l1_pred")
        if cat_id is None:
            cat_name = rec.get("category_l1", "")
            cat_id = CATEGORY_NAME_TO_ID.get(cat_name, 0)
        tag_ids = rec.get("tag_ids") or rec.get("tags_pred") or []
        if tag_ids and isinstance(tag_ids[0], str):
            tag_ids = [hash(t) % 4096 for t in tag_ids]
        tag_ids = [int(t) for t in tag_ids[:32]]

        original_pk = rec["image_pk"]

        for vi in range(n_variants):
            # 1. 向量微扰动
            noise = rng.normal(0, NOISE_SIGMA, size=256).astype(np.float32)
            perturbed = vec + noise
            norm = np.linalg.norm(perturbed)
            if norm > 1e-8:
                perturbed = perturbed / norm
            perturbed_list = perturbed.tolist()

            # 2. 唯一 PK
            vpk = generate_variant_pk(original_pk, vi)

            # 3. 品类/标签微调 (20% 概率)
            v_cat_id = cat_id
            v_tags = list(tag_ids)
            if py_rng.random() < 0.2:
                # 随机增删 1 个标签
                if v_tags and py_rng.random() < 0.5:
                    v_tags.pop(py_rng.randint(0, len(v_tags) - 1))
                else:
                    v_tags.append(py_rng.choice(all_tag_ids))
                v_tags = v_tags[:32]

            # 4. 商家
            merchant_num = py_rng.randint(*MERCHANT_RANGE)

            variants.append({
                "image_pk": vpk,
                "global_vec": perturbed_list,
                "category_l1": int(v_cat_id),
                "tags": v_tags,
                "product_id": f"P1{global_idx:07d}",
                "merchant_str": f"T20260101{merchant_num}",
                "synthetic_uri": f"synthetic://{original_pk}/v{vi}",
            })
            global_idx += 1

        if (rec_idx + 1) % 20000 == 0:
            elapsed = time.time() - t0
            log.info(f"  Generated variants for {rec_idx + 1}/{len(records)} records "
                     f"({len(variants)} variants, {elapsed:.1f}s)")

    log.info(f"Generated {len(variants)} variant records in {time.time()-t0:.1f}s")
    return variants


def insert_milvus(variants: list[dict]):
    """批量 upsert 变体到 Milvus"""
    from pymilvus import connections, Collection

    log.info(f"Connecting to Milvus at {MILVUS_HOST}:{MILVUS_PORT}...")
    connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT)
    coll = Collection(COLLECTION_NAME)

    if not coll.has_partition(PARTITION_NAME):
        coll.create_partition(PARTITION_NAME)

    total = len(variants)
    now_ms = int(time.time() * 1000)
    t0 = time.time()

    log.info(f"Upserting {total} variants to Milvus (batch={MILVUS_BATCH_SIZE})...")
    for i in range(0, total, MILVUS_BATCH_SIZE):
        batch = variants[i: i + MILVUS_BATCH_SIZE]
        rows = [{
            "image_pk": v["image_pk"],
            "global_vec": v["global_vec"],
            "category_l1": v["category_l1"],
            "category_l2": 0,
            "category_l3": 0,
            "tags": v["tags"],
            "color_code": 0,
            "material_code": 0,
            "style_code": 0,
            "season_code": 0,
            "is_evergreen": False,
            "ts_month": TS_MONTH,
            "promoted_at": 0,
            "product_id": v["product_id"],
            "created_at": now_ms,
        } for v in batch]

        coll.upsert(rows, partition_name=PARTITION_NAME)
        done = min(i + MILVUS_BATCH_SIZE, total)
        if done % 50000 < MILVUS_BATCH_SIZE or done == total:
            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0
            log.info(f"  Milvus upsert {done}/{total} ({rate:.0f} rec/s)")

    log.info("Flushing Milvus...")
    coll.flush()
    log.info(f"Milvus done. Collection count: {coll.num_entities}")


def insert_pg(variants: list[dict]):
    """批量插入 uri_dedup + image_merchant_bitmaps"""
    import psycopg2
    from psycopg2.extras import execute_values

    log.info("Connecting to PostgreSQL...")
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = False
    cur = conn.cursor()

    total = len(variants)
    t0 = time.time()

    # ── uri_dedup ──
    log.info(f"Inserting {total} into uri_dedup...")
    for i in range(0, total, PG_BATCH_SIZE):
        batch = variants[i: i + PG_BATCH_SIZE]
        values = []
        for v in batch:
            uri = v["synthetic_uri"]
            uri_hash = hashlib.sha256(uri.encode()).hexdigest()
            values.append((v["image_pk"], uri_hash, uri, TS_MONTH))
        execute_values(
            cur,
            "INSERT INTO uri_dedup (image_pk, uri_hash, uri, ts_month) VALUES %s "
            "ON CONFLICT (image_pk) DO UPDATE SET uri = EXCLUDED.uri",
            values,
        )
        conn.commit()
        done = min(i + PG_BATCH_SIZE, total)
        if done % 100000 < PG_BATCH_SIZE or done == total:
            rate = done / (time.time() - t0) if time.time() > t0 else 0
            log.info(f"  uri_dedup {done}/{total} ({rate:.0f} rec/s)")

    # ── merchant_id_mapping (扩展商家范围) ──
    log.info("Expanding merchant_id_mapping...")
    merchant_strs = list({v["merchant_str"] for v in variants})
    # 插入新商家（已有的跳过）
    for i in range(0, len(merchant_strs), 1000):
        batch = [(m,) for m in merchant_strs[i:i+1000]]
        execute_values(
            cur,
            "INSERT INTO merchant_id_mapping (merchant_str) VALUES %s "
            "ON CONFLICT (merchant_str) DO NOTHING",
            batch,
        )
    conn.commit()

    # 读回所有 mapping
    cur.execute("SELECT merchant_str, bitmap_index FROM merchant_id_mapping")
    merchant_to_idx = {r[0]: r[1] for r in cur.fetchall()}
    log.info(f"  merchant_id_mapping total: {len(merchant_to_idx)}")

    # ── image_merchant_bitmaps ──
    log.info(f"Inserting {total} into image_merchant_bitmaps...")
    t1 = time.time()
    for i in range(0, total, PG_BATCH_SIZE):
        batch = variants[i: i + PG_BATCH_SIZE]
        values = []
        for v in batch:
            bmp_idx = merchant_to_idx.get(v["merchant_str"], 1)
            values.append((v["image_pk"], [bmp_idx]))
        execute_values(
            cur,
            "INSERT INTO image_merchant_bitmaps (image_pk, bitmap) VALUES %s "
            "ON CONFLICT (image_pk) DO UPDATE SET bitmap = EXCLUDED.bitmap, updated_at = now()",
            values,
            template="(%s, rb_build(%s::int[]))",
        )
        conn.commit()
        done = min(i + PG_BATCH_SIZE, total)
        if done % 100000 < PG_BATCH_SIZE or done == total:
            rate = done / (time.time() - t1) if time.time() > t1 else 0
            log.info(f"  bitmap {done}/{total} ({rate:.0f} rec/s)")

    cur.close()
    conn.close()
    log.info(f"PostgreSQL done in {time.time()-t0:.1f}s")


def print_final_stats():
    """输出最终统计"""
    from pymilvus import connections, Collection
    import psycopg2

    connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT)
    coll = Collection(COLLECTION_NAME)

    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()

    cur.execute("SELECT count(*) FROM uri_dedup")
    pg_dedup = cur.fetchone()[0]

    cur.execute("SELECT count(*) FROM image_merchant_bitmaps")
    pg_bitmap = cur.fetchone()[0]

    cur.execute("SELECT count(*) FROM merchant_id_mapping")
    pg_merchant = cur.fetchone()[0]

    log.info("=" * 50)
    log.info("最终数据规模:")
    log.info(f"  Milvus global_images_hot: {coll.num_entities:,}")
    log.info(f"  PG uri_dedup:             {pg_dedup:,}")
    log.info(f"  PG bitmap:                {pg_bitmap:,}")
    log.info(f"  PG merchants:             {pg_merchant:,}")
    log.info("=" * 50)

    cur.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Derive 1M records from real images")
    parser.add_argument("--target", type=int, default=1000000,
                        help="Target total record count (default: 1000000)")
    parser.add_argument("--enriched-dir", default="data/features_enriched",
                        help="Path to enriched JSONL directory")
    parser.add_argument("--extra-dir", default=None,
                        help="Path to extra backup dir (e.g. data/batch2_backup)")
    parser.add_argument("--skip-milvus", action="store_true")
    parser.add_argument("--skip-pg", action="store_true")
    args = parser.parse_args()

    t0 = time.time()

    # 1. 加载源数据
    records = load_source_records(args.enriched_dir, args.extra_dir)

    # 2. 生成变体
    variants = generate_variants(records, args.target)
    if not variants:
        log.info("No variants to generate")
        print_final_stats()
        return

    # 3. 写入 Milvus
    if not args.skip_milvus:
        insert_milvus(variants)
    else:
        log.info("Skipping Milvus (--skip-milvus)")

    # 4. 写入 PG
    if not args.skip_pg:
        insert_pg(variants)
    else:
        log.info("Skipping PG (--skip-pg)")

    # 5. 统计
    total_time = time.time() - t0
    log.info(f"Total time: {total_time:.1f}s ({total_time/60:.1f}min)")
    print_final_stats()


if __name__ == "__main__":
    main()
