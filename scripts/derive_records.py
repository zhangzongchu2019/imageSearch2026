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
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "global_images_hot")
PARTITION_NAME = os.getenv("PARTITION_NAME", "p_202604")
TS_MONTH = int(os.getenv("TS_MONTH", "202604"))
MILVUS_BATCH_SIZE = 2000  # 降低避免 Milvus/etcd 过载
PG_BATCH_SIZE = 2000
NOISE_SIGMA = 0.03
MERCHANT_RANGE = (10001, 15999)
IS_EVERGREEN = False

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


def _drop_indexes_for_bulk_write():
    """写入前 drop 所有索引, 避免 indexing task 风暴"""
    from pymilvus import connections, Collection
    try:
        connections.disconnect("default")
    except Exception:
        pass
    connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT, timeout=30)
    coll = Collection(COLLECTION_NAME)

    # release (可能需要多次尝试)
    for attempt in range(3):
        try:
            coll.release()
            log.info("Collection released")
            time.sleep(5)
            break
        except Exception as e:
            log.warning(f"Release attempt {attempt+1} failed: {e}")
            time.sleep(10)

    for idx in list(coll.indexes):
        for attempt in range(3):
            try:
                log.info(f"  Dropping index: {idx.field_name} ({idx.params.get('index_type', '?')})")
                coll.drop_index(index_name=idx.index_name)
                break
            except Exception as e:
                log.warning(f"  Drop index failed (attempt {attempt+1}): {e}")
                time.sleep(5)
    log.info("All indexes dropped")


def _rebuild_indexes():
    """写入完成后重建所有索引"""
    from pymilvus import connections, Collection
    try:
        connections.disconnect("default")
    except Exception:
        pass
    _ensure_milvus_running()
    connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT, timeout=15)
    coll = Collection(COLLECTION_NAME)

    # HNSW 向量索引
    log.info("Rebuilding HNSW index on global_vec...")
    coll.create_index("global_vec", {
        "index_type": "HNSW",
        "metric_type": "COSINE",
        "params": {"M": 24, "efConstruction": 200},
    })
    log.info("HNSW index created (building in background)")

    # 标量索引
    scalar_fields = ["category_l1", "category_l2", "category_l3",
                     "color_code", "material_code", "style_code",
                     "season_code", "is_evergreen", "tags", "ts_month"]
    for field in scalar_fields:
        try:
            coll.create_index(field, {"index_type": "INVERTED"})
            log.info(f"  INVERTED index on {field} created")
        except Exception as e:
            log.warning(f"  Failed to create index on {field}: {e}")

    log.info("All indexes rebuild initiated. HNSW will build in background.")


def _ensure_milvus_running():
    """检查 Milvus 是否运行, 如果挂了自动重启 etcd + Milvus"""
    import subprocess
    try:
        r = subprocess.run(["docker", "inspect", "imgsrch-milvus", "--format", "{{.State.Status}}"],
                           capture_output=True, text=True, timeout=5)
        status = r.stdout.strip()
        if status == "running":
            return True
    except Exception:
        pass

    log.warning("Milvus is down! Restarting etcd + Milvus...")
    subprocess.run(["docker", "restart", "imgsrch-etcd"], capture_output=True, timeout=15)
    time.sleep(5)
    subprocess.run(["docker", "restart", "imgsrch-milvus"], capture_output=True, timeout=15)
    log.info("Waiting 30s for Milvus to be ready...")
    time.sleep(30)

    # 验证
    try:
        from pymilvus import connections
        connections.disconnect("default")
    except Exception:
        pass
    try:
        from pymilvus import connections
        connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT, timeout=15)
        log.info("Milvus reconnected successfully")
        return True
    except Exception as e:
        log.error(f"Milvus still not available after restart: {e}")
        return False


def insert_milvus(variants: list[dict]):
    """批量 upsert 变体到 Milvus (带自动重连+重试)"""
    from pymilvus import connections, Collection

    def _connect():
        try:
            connections.disconnect("default")
        except Exception:
            pass
        connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT, timeout=15)
        return Collection(COLLECTION_NAME)

    # 确保 Milvus 运行
    _ensure_milvus_running()
    coll = _connect()

    if not coll.has_partition(PARTITION_NAME):
        coll.create_partition(PARTITION_NAME)

    total = len(variants)
    now_ms = int(time.time() * 1000)
    t0 = time.time()

    log.info(f"Upserting {total} variants to Milvus (batch={MILVUS_BATCH_SIZE})...")
    i = 0
    while i < total:
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
            "is_evergreen": IS_EVERGREEN,
            "ts_month": TS_MONTH,
            "promoted_at": 0,
            "product_id": v["product_id"],
            "created_at": now_ms,
        } for v in batch]

        for attempt in range(3):
            try:
                coll.upsert(rows, partition_name=PARTITION_NAME)
                break
            except Exception as e:
                log.warning(f"Milvus upsert failed (attempt {attempt+1}/3): {e}")
                if attempt < 2:
                    _ensure_milvus_running()
                    coll = _connect()
                    time.sleep(5)
                else:
                    log.error(f"Milvus upsert failed after 3 attempts at offset {i}, skipping batch")

        i += MILVUS_BATCH_SIZE
        done = min(i, total)
        if done % 50000 < MILVUS_BATCH_SIZE or done == total:
            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0
            log.info(f"  Milvus upsert {done}/{total} ({rate:.0f} rec/s)")

    log.info(f"Milvus upsert done. (auto-flush, no explicit flush)")


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
    parser.add_argument("--partition", type=str, default=None,
                        help="Milvus partition name (e.g. p_202604_svip)")
    parser.add_argument("--ts-month", type=int, default=None,
                        help="ts_month value (e.g. 202604)")
    parser.add_argument("--collection", type=str, default=None,
                        help="Milvus collection name")
    parser.add_argument("--evergreen", action="store_true",
                        help="Set is_evergreen=True for evergreen partition")
    args = parser.parse_args()

    # 命令行参数覆盖全局常量
    global PARTITION_NAME, TS_MONTH, COLLECTION_NAME, IS_EVERGREEN
    if args.partition:
        PARTITION_NAME = args.partition
    if args.ts_month:
        TS_MONTH = args.ts_month
    if args.collection:
        COLLECTION_NAME = args.collection
    if args.evergreen:
        IS_EVERGREEN = True

    t0 = time.time()

    # 1. 加载源数据
    records = load_source_records(args.enriched_dir, args.extra_dir)

    # 2. 分批衍生+写入 (避免 OOM, 每批 200 万变体)
    BATCH_VARIANTS = 1_000_000  # 100万/批 (索引已 drop, 无 etcd 压力)
    total_need = args.target - len(records)
    if total_need <= 0:
        log.info(f"Already have {len(records)} >= target {args.target}")
        print_final_stats()
        return

    variants_per_record = total_need // len(records)
    extra = total_need % len(records)
    log.info(f"Need {total_need:,} variants ({variants_per_record} per record + {extra} extra)")
    log.info(f"Will process in batches of {BATCH_VARIANTS:,} to stay within memory limits")

    # 索引已手动 drop, 直接开始写入 (无 indexing task 风暴)
    log.info("Starting bulk write (indexes should be dropped beforehand)")

    rng = np.random.RandomState(2026)
    py_rng = random.Random(2026)
    all_tag_ids = list(range(100, 4096))

    global_variant_idx = 0
    total_written = 0
    batch_num = 0
    rec_cursor = 0  # 当前处理到第几条源记录

    while total_written < total_need:
        batch_variants = []
        batch_target = min(BATCH_VARIANTS, total_need - total_written)
        t_batch = time.time()

        while len(batch_variants) < batch_target and rec_cursor < len(records):
            rec = records[rec_cursor]
            n_variants = variants_per_record + (1 if rec_cursor < extra else 0)
            vec = np.array(rec["global_vec"], dtype=np.float32)
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
                if len(batch_variants) >= batch_target:
                    break
                noise = rng.normal(0, NOISE_SIGMA, size=256).astype(np.float32)
                perturbed = vec + noise
                norm = np.linalg.norm(perturbed)
                if norm > 1e-8:
                    perturbed = perturbed / norm

                vpk = generate_variant_pk(original_pk, global_variant_idx)
                v_cat_id = cat_id
                v_tags = list(tag_ids)
                if py_rng.random() < 0.2:
                    if v_tags and py_rng.random() < 0.5:
                        v_tags.pop(py_rng.randint(0, len(v_tags) - 1))
                    else:
                        v_tags.append(py_rng.choice(all_tag_ids))
                    v_tags = v_tags[:32]

                merchant_num = py_rng.randint(*MERCHANT_RANGE)
                batch_variants.append({
                    "image_pk": vpk,
                    "global_vec": perturbed.tolist(),
                    "category_l1": int(v_cat_id),
                    "tags": v_tags,
                    "product_id": f"P1{global_variant_idx:07d}",
                    "merchant_str": f"T20260101{merchant_num}",
                    "synthetic_uri": f"synthetic://{original_pk}/v{global_variant_idx}",
                })
                global_variant_idx += 1

            rec_cursor += 1

        if not batch_variants:
            break

        batch_num += 1
        gen_time = time.time() - t_batch
        log.info(f"Batch {batch_num}: generated {len(batch_variants):,} variants "
                 f"in {gen_time:.1f}s (rec_cursor={rec_cursor:,}/{len(records):,})")

        # 备份当前批次到 JSONL 文件
        backup_dir = os.path.join(os.getenv("BACKUP_DIR", "/data/imgsrch/backup_20260403"), "derive_batches")
        os.makedirs(backup_dir, exist_ok=True)
        backup_file = os.path.join(backup_dir, f"batch_{batch_num:03d}.jsonl")
        t_backup = time.time()
        with open(backup_file, "w") as bf:
            for v in batch_variants:
                # 只保存必要字段 (不含 global_vec list 以节省空间, 可从种子重算)
                bf.write(json.dumps({
                    "image_pk": v["image_pk"],
                    "global_vec": v["global_vec"],
                    "category_l1": v["category_l1"],
                    "tags": v["tags"],
                    "product_id": v["product_id"],
                    "merchant_str": v["merchant_str"],
                    "synthetic_uri": v["synthetic_uri"],
                }, ensure_ascii=False) + "\n")
        backup_size = os.path.getsize(backup_file) / (1024**2)
        log.info(f"Batch {batch_num} backed up: {backup_file} ({backup_size:.0f} MB, {time.time()-t_backup:.1f}s)")

        # 写入 (带自动重试)
        if not args.skip_milvus:
            try:
                insert_milvus(batch_variants)
            except Exception as e:
                log.error(f"Milvus insert failed for batch {batch_num}: {e}")
                log.info("Will retry after Milvus restart...")
                _ensure_milvus_running()
                time.sleep(30)
                try:
                    insert_milvus(batch_variants)
                except Exception as e2:
                    log.error(f"Milvus retry also failed: {e2}, skipping this batch's Milvus write")
        if not args.skip_pg:
            insert_pg(batch_variants)

        total_written += len(batch_variants)
        log.info(f"Batch {batch_num} done. Total written: {total_written:,}/{total_need:,}")

        # 释放内存
        del batch_variants

        # 等待 Milvus indexing 完成, 避免 etcd 超载崩溃
        if not args.skip_milvus and total_written < total_need:
            log.info("Waiting 10s between batches...")
            time.sleep(10)

    # 5. 重建索引
    if not args.skip_milvus:
        log.info("All batches done. Rebuilding indexes...")
        _rebuild_indexes()

    # 6. 统计
    total_time = time.time() - t0
    log.info(f"Total time: {total_time:.1f}s ({total_time/60:.1f}min)")
    log.info(f"Total variants written: {total_written:,}")
    print_final_stats()


if __name__ == "__main__":
    main()
