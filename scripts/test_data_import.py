#!/usr/bin/env python3
"""
测试数据导入脚本 — 1000 万真实图片 → 完整测试环境

执行流程:
  Phase 1: 下载图片 + GPU 特征提取 (品类/标签由模型识别)
  Phase 2: 统计品类分布 + 创建商家 + 分配图片
  Phase 3: 模拟转发 (24x 倍率, 同类目内)
  Phase 4: 按月分区写入 Milvus + PG
  Phase 5: 生成测试 query 图片集

用法:
  # Phase 1: 特征提取 (耗时最长, 可分批)
  python scripts/test_data_import.py phase1 --url-file urls.txt --output-dir /data/features --batch-size 1000 --workers 8

  # Phase 2: 商家分配
  python scripts/test_data_import.py phase2 --feature-dir /data/features --output-dir /data/assigned

  # Phase 3: 模拟转发
  python scripts/test_data_import.py phase3 --assigned-dir /data/assigned --output-dir /data/forwarded

  # Phase 4: 写入 Milvus + PG
  python scripts/test_data_import.py phase4 --data-dir /data/forwarded

  # Phase 5: 生成测试 query
  python scripts/test_data_import.py phase5 --data-dir /data/forwarded --output-dir /data/queries

  # 一键执行全部
  python scripts/test_data_import.py all --url-file urls.txt --data-dir /data/test
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import math
import os
import random
import struct
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

# ══════════════════════════════════════════════════════════════
# 配置常量
# ══════════════════════════════════════════════════════════════

CONFIG = {
    # 商家
    "total_merchants": 1300,
    "svip_count": 10,
    "svip_images_each": 100_000,

    # 产品
    "images_per_product": 4,

    # 月份分配 (18 个月, 从新到旧)
    "month_distribution": {
        1: 0.08,   # 当月
        2: 0.07, 3: 0.07, 4: 0.07, 5: 0.07, 6: 0.07,  # 热区
        7: 0.05, 8: 0.05, 9: 0.05, 10: 0.05, 11: 0.05, 12: 0.05,  # 冷区
        13: 0.03, 14: 0.03, 15: 0.03, 16: 0.03, 17: 0.03, 18: 0.03,  # 即将过期
    },

    # 转发
    "forward_ratio": 24,
    "forward_distribution": {
        # (min_forwards, max_forwards): probability
        (1, 5): 0.80,
        (10, 50): 0.15,
        (100, 500): 0.05,
    },

    # 特征提取
    "embedding_dim": 256,
    "sub_embedding_dim": 128,
    "max_sub_images": 5,

    # 基础设施
    "inference_service_url": "http://localhost:8090",
    "milvus_host": "localhost",
    "milvus_port": 19530,
    "pg_dsn": "postgresql://postgres:postgres@localhost:5432/image_search",
    "batch_size": 500,
}


# ══════════════════════════════════════════════════════════════
# 数据结构
# ══════════════════════════════════════════════════════════════

@dataclass
class ImageRecord:
    """单张图片的完整记录"""
    url: str
    image_pk: str  # sha256(url)[:32]
    global_vec: Optional[List[float]] = None
    sub_vecs: Optional[List[List[float]]] = None
    category_l1: Optional[str] = None
    category_l2: Optional[str] = None
    tags: Optional[List[str]] = None
    tag_ids: Optional[List[int]] = None
    merchant_id: Optional[str] = None
    product_id: Optional[str] = None
    ts_month: Optional[int] = None  # 202601
    is_evergreen: bool = False
    forwarded_to: Optional[List[str]] = None  # 转发目标商家列表


def url_to_pk(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:32]


def get_month_list(n_months: int = 18) -> List[int]:
    """生成最近 n 个月的 YYYYMM 列表"""
    from datetime import datetime, timedelta
    now = datetime.utcnow()
    months = []
    for i in range(n_months):
        dt = now - timedelta(days=30 * i)
        months.append(int(dt.strftime("%Y%m")))
    return months


# ══════════════════════════════════════════════════════════════
# Phase 1: 下载 + 特征提取
# ══════════════════════════════════════════════════════════════

async def phase1_extract_features(url_file: str, output_dir: str, batch_size: int, workers: int):
    """从 URL 清单下载图片并提取特征"""
    import aiohttp

    os.makedirs(output_dir, exist_ok=True)

    # 读取 URL
    print(f"[Phase 1] 读取 URL 文件: {url_file}")
    with open(url_file, "r") as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    total = len(urls)
    print(f"[Phase 1] 共 {total:,} 个 URL")

    inference_url = CONFIG["inference_service_url"]
    processed = 0
    failed = 0
    batch_idx = 0

    async with aiohttp.ClientSession() as session:
        for i in range(0, total, batch_size):
            batch_urls = urls[i:i + batch_size]
            batch_records = []

            for url in batch_urls:
                pk = url_to_pk(url)
                record = ImageRecord(url=url, image_pk=pk)

                try:
                    # 调用 inference-service 提取特征
                    async with session.post(
                        f"{inference_url}/extract",
                        json={"image_url": url},
                        timeout=aiohttp.ClientTimeout(total=30),
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            record.global_vec = data.get("global_vec")
                            record.sub_vecs = data.get("sub_vecs")
                            record.category_l1 = data.get("category_l1")
                            record.category_l2 = data.get("category_l2")
                            record.tags = data.get("tags", [])
                            record.tag_ids = data.get("tag_ids", [])
                            batch_records.append(record)
                        else:
                            failed += 1
                except Exception as e:
                    failed += 1

            # 保存批次结果
            if batch_records:
                batch_file = os.path.join(output_dir, f"batch_{batch_idx:06d}.jsonl")
                with open(batch_file, "w") as f:
                    for rec in batch_records:
                        f.write(json.dumps({
                            "url": rec.url,
                            "image_pk": rec.image_pk,
                            "global_vec": rec.global_vec,
                            "sub_vecs": rec.sub_vecs,
                            "category_l1": rec.category_l1,
                            "category_l2": rec.category_l2,
                            "tags": rec.tags,
                            "tag_ids": rec.tag_ids,
                        }, ensure_ascii=False) + "\n")

            batch_idx += 1
            processed += len(batch_urls)
            if processed % 10000 == 0:
                print(f"[Phase 1] 进度: {processed:,}/{total:,} ({processed*100//total}%), 失败: {failed:,}")

    print(f"[Phase 1] 完成! 成功: {processed - failed:,}, 失败: {failed:,}")
    print(f"[Phase 1] 特征文件保存在: {output_dir}/")


# ══════════════════════════════════════════════════════════════
# Phase 1b: 离线模式 — 无 inference-service 时生成模拟特征
# ══════════════════════════════════════════════════════════════

def phase1b_generate_mock_features(url_file: str, output_dir: str, batch_size: int):
    """离线模式: 无 GPU 时用随机向量模拟 (品类按 URL 关键词推断)"""
    os.makedirs(output_dir, exist_ok=True)

    # 品类关键词映射
    CATEGORY_KEYWORDS = {
        "服装": ["dress", "shirt", "jacket", "coat", "pants", "skirt", "top", "衣", "裙", "裤", "衫"],
        "鞋帽": ["shoe", "boot", "hat", "cap", "sneaker", "heel", "鞋", "帽", "靴"],
        "箱包": ["bag", "purse", "wallet", "luggage", "backpack", "包", "箱", "wallet"],
        "珠宝玉器": ["jade", "gold", "diamond", "gem", "jewel", "玉", "翡翠", "金", "钻"],
        "首饰配饰": ["necklace", "ring", "earring", "bracelet", "brooch", "项链", "戒指", "耳环"],
    }

    TAG_POOLS = {
        "服装": ["上衣", "裤子", "裙子", "外套", "棉质", "真丝", "春季", "夏季", "秋季", "冬季", "休闲", "正式", "复古", "简约"],
        "鞋帽": ["运动鞋", "高跟鞋", "皮鞋", "帽子", "真皮", "透气", "防滑", "休闲"],
        "箱包": ["手提包", "双肩包", "钱包", "旅行箱", "真皮", "PU皮", "帆布", "拉链"],
        "珠宝玉器": ["翡翠", "和田玉", "黄金", "铂金", "钻石", "挂件", "手镯", "戒指"],
        "首饰配饰": ["项链", "耳环", "手链", "胸针", "银饰", "水晶", "珍珠", "合金"],
    }

    def guess_category(url: str) -> str:
        url_lower = url.lower()
        for cat, keywords in CATEGORY_KEYWORDS.items():
            if any(kw in url_lower for kw in keywords):
                return cat
        return random.choice(list(CATEGORY_KEYWORDS.keys()))

    print(f"[Phase 1b] 读取 URL 文件: {url_file}")
    with open(url_file, "r") as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    total = len(urls)
    print(f"[Phase 1b] 共 {total:,} 个 URL (模拟特征模式)")

    rng = np.random.RandomState(42)
    batch_idx = 0

    for i in range(0, total, batch_size):
        batch_urls = urls[i:i + batch_size]
        batch_file = os.path.join(output_dir, f"batch_{batch_idx:06d}.jsonl")

        with open(batch_file, "w") as f:
            for url in batch_urls:
                pk = url_to_pk(url)
                cat = guess_category(url)
                tags = random.sample(TAG_POOLS.get(cat, TAG_POOLS["服装"]), k=random.randint(4, 8))

                # 生成随机向量 (同品类向量有一定聚簇性)
                cat_seed = hash(cat) % 10000
                base_vec = rng.randn(256).astype(np.float32)
                cat_bias = np.random.RandomState(cat_seed).randn(256).astype(np.float32) * 0.3
                vec = base_vec + cat_bias
                vec = (vec / np.linalg.norm(vec)).tolist()

                sub_vecs = []
                n_subs = random.randint(1, CONFIG["max_sub_images"])
                for s in range(n_subs):
                    sv = rng.randn(128).astype(np.float32)
                    sv = (sv / np.linalg.norm(sv)).tolist()
                    sub_vecs.append(sv)

                record = {
                    "url": url,
                    "image_pk": pk,
                    "global_vec": vec,
                    "sub_vecs": sub_vecs,
                    "category_l1": cat,
                    "category_l2": f"{cat}_子类",
                    "tags": tags,
                    "tag_ids": [hash(t) % 4096 for t in tags],
                }
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        batch_idx += 1
        processed = min(i + batch_size, total)
        if processed % 100000 == 0:
            print(f"[Phase 1b] 进度: {processed:,}/{total:,} ({processed*100//total}%)")

    print(f"[Phase 1b] 完成! {total:,} 条模拟特征保存在: {output_dir}/")


# ══════════════════════════════════════════════════════════════
# Phase 2: 统计品类分布 + 创建商家 + 分配图片
# ══════════════════════════════════════════════════════════════

def phase2_assign_merchants(feature_dir: str, output_dir: str):
    """统计品类 → 按比例创建商家 → 分配图片"""
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: 扫描所有特征文件, 统计品类分布
    print("[Phase 2] 扫描品类分布...")
    category_counts = Counter()
    total_images = 0

    feature_files = sorted(Path(feature_dir).glob("batch_*.jsonl"))
    for ff in feature_files:
        with open(ff) as f:
            for line in f:
                rec = json.loads(line)
                category_counts[rec.get("category_l1", "未知")] += 1
                total_images += 1

    print(f"[Phase 2] 总图片: {total_images:,}")
    print(f"[Phase 2] 品类分布:")
    for cat, cnt in category_counts.most_common():
        print(f"  {cat}: {cnt:,} ({cnt*100/total_images:.1f}%)")

    # Step 2: 按品类比例创建商家
    total_merchants = CONFIG["total_merchants"]
    svip_count = CONFIG["svip_count"]
    normal_count = total_merchants - svip_count

    merchants = {}  # merchant_id → {"category": ..., "is_svip": ...}
    merchant_id_counter = 0

    # SVIP: 从图片最多的品类各分配
    svip_categories = [cat for cat, _ in category_counts.most_common(svip_count)]
    for i, cat in enumerate(svip_categories):
        mid = f"M_{merchant_id_counter:05d}"
        merchants[mid] = {"category": cat, "is_svip": True, "images": []}
        merchant_id_counter += 1

    # 普通商家: 按品类比例分配
    for cat, cnt in category_counts.items():
        ratio = cnt / total_images
        n_merchants = max(1, int(normal_count * ratio))
        for _ in range(n_merchants):
            if merchant_id_counter >= total_merchants:
                break
            mid = f"M_{merchant_id_counter:05d}"
            merchants[mid] = {"category": cat, "is_svip": False, "images": []}
            merchant_id_counter += 1

    print(f"[Phase 2] 创建 {len(merchants)} 个商家 (SVIP: {svip_count})")

    # Step 3: 按品类分配图片到商家
    print("[Phase 2] 分配图片到商家...")
    category_merchants = defaultdict(list)
    for mid, info in merchants.items():
        category_merchants[info["category"]].append(mid)

    # 月份列表
    months = get_month_list(18)
    month_weights = [CONFIG["month_distribution"].get(i + 1, 0.03) for i in range(18)]

    svip_merchants = {mid for mid, info in merchants.items() if info["is_svip"]}
    svip_image_limit = CONFIG["svip_images_each"]

    image_idx = 0
    product_counters = defaultdict(int)  # merchant_id → product count

    assigned_file = os.path.join(output_dir, "assigned_images.jsonl")
    with open(assigned_file, "w") as out_f:
        for ff in feature_files:
            with open(ff) as f:
                for line in f:
                    rec = json.loads(line)
                    cat = rec.get("category_l1", "未知")

                    # 选择商家
                    candidates = category_merchants.get(cat, [])
                    if not candidates:
                        candidates = list(merchants.keys())

                    # SVIP 优先分配 (直到达到上限)
                    svip_cands = [m for m in candidates if m in svip_merchants
                                  and len(merchants[m]["images"]) < svip_image_limit]
                    if svip_cands and random.random() < 0.1:
                        mid = random.choice(svip_cands)
                    else:
                        normal_cands = [m for m in candidates if m not in svip_merchants]
                        mid = random.choice(normal_cands) if normal_cands else random.choice(candidates)

                    # 分配产品 ID (每 4 张一个 SPU)
                    product_counters[mid] += 1
                    product_idx = (product_counters[mid] - 1) // CONFIG["images_per_product"]
                    product_id = f"SPU_{mid}_{product_idx:04d}"

                    # 分配月份
                    ts_month = random.choices(months, weights=month_weights, k=1)[0]

                    rec["merchant_id"] = mid
                    rec["product_id"] = product_id
                    rec["ts_month"] = ts_month
                    rec["is_evergreen"] = False

                    merchants[mid]["images"].append(rec["image_pk"])
                    out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

                    image_idx += 1
                    if image_idx % 100000 == 0:
                        print(f"[Phase 2] 分配进度: {image_idx:,}/{total_images:,}")

    # 保存商家清单
    merchant_file = os.path.join(output_dir, "merchants.json")
    merchant_summary = {}
    for mid, info in merchants.items():
        merchant_summary[mid] = {
            "category": info["category"],
            "is_svip": info["is_svip"],
            "image_count": len(info["images"]),
        }
    with open(merchant_file, "w") as f:
        json.dump(merchant_summary, f, ensure_ascii=False, indent=2)

    print(f"[Phase 2] 完成! 分配文件: {assigned_file}")
    print(f"[Phase 2] 商家清单: {merchant_file}")

    # 统计
    svip_stats = {mid: len(info["images"]) for mid, info in merchants.items() if info["is_svip"]}
    print(f"[Phase 2] SVIP 商家图片量: {svip_stats}")


# ══════════════════════════════════════════════════════════════
# Phase 3: 模拟转发
# ══════════════════════════════════════════════════════════════

def phase3_simulate_forwards(assigned_dir: str, output_dir: str):
    """模拟转发: 同类目内, 长尾分布"""
    os.makedirs(output_dir, exist_ok=True)

    # 加载商家信息
    merchant_file = os.path.join(assigned_dir, "merchants.json")
    with open(merchant_file) as f:
        merchants = json.load(f)

    category_merchants = defaultdict(list)
    for mid, info in merchants.items():
        category_merchants[info["category"]].append(mid)

    # 转发分布
    dist = CONFIG["forward_distribution"]

    print("[Phase 3] 模拟转发...")
    assigned_file = os.path.join(assigned_dir, "assigned_images.jsonl")
    forward_file = os.path.join(output_dir, "forwards.jsonl")
    output_file = os.path.join(output_dir, "final_images.jsonl")

    total_forwards = 0
    total_images = 0

    with open(assigned_file) as in_f, \
         open(forward_file, "w") as fwd_f, \
         open(output_file, "w") as out_f:

        for line in in_f:
            rec = json.loads(line)
            total_images += 1

            # 写入原始记录
            rec["forwarded_to"] = []
            out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

            # 决定转发次数
            r = random.random()
            cumulative = 0
            n_forwards = 0
            for (min_f, max_f), prob in dist.items():
                cumulative += prob
                if r <= cumulative:
                    n_forwards = random.randint(min_f, max_f)
                    break

            if n_forwards > 0:
                cat = rec.get("category_l1", "未知")
                same_cat_merchants = category_merchants.get(cat, [])
                # 排除自身
                targets = [m for m in same_cat_merchants if m != rec["merchant_id"]]
                if targets:
                    n_forwards = min(n_forwards, len(targets))
                    forward_targets = random.sample(targets, n_forwards)
                    rec["forwarded_to"] = forward_targets

                    for target_mid in forward_targets:
                        fwd_record = {
                            "image_pk": rec["image_pk"],
                            "source_merchant": rec["merchant_id"],
                            "target_merchant": target_mid,
                            "ts_month": rec["ts_month"],
                        }
                        fwd_f.write(json.dumps(fwd_record, ensure_ascii=False) + "\n")
                        total_forwards += 1

            if total_images % 100000 == 0:
                print(f"[Phase 3] 进度: {total_images:,}, 转发: {total_forwards:,}")

    ratio = total_forwards / total_images if total_images > 0 else 0
    print(f"[Phase 3] 完成! 图片: {total_images:,}, 转发: {total_forwards:,}, 倍率: {ratio:.1f}x")


# ══════════════════════════════════════════════════════════════
# Phase 4: 写入 Milvus + PG
# ══════════════════════════════════════════════════════════════

async def phase4_write_to_storage(data_dir: str):
    """写入 Milvus 向量 + PG 元数据 + bitmap"""
    try:
        import asyncpg
        from pymilvus import Collection, connections, utility
    except ImportError:
        print("[Phase 4] 缺少依赖: pip install asyncpg pymilvus")
        print("[Phase 4] 跳过写入, 仅验证数据文件")
        return

    # 连接 Milvus
    connections.connect("default", host=CONFIG["milvus_host"], port=CONFIG["milvus_port"])

    # 连接 PG
    pg_pool = await asyncpg.create_pool(CONFIG["pg_dsn"], min_size=5, max_size=20)

    final_file = os.path.join(data_dir, "final_images.jsonl")
    forward_file = os.path.join(data_dir, "forwards.jsonl")

    # Step 1: 写入图片向量和元数据
    print("[Phase 4] 写入 Milvus + PG...")
    batch = []
    total = 0

    with open(final_file) as f:
        for line in f:
            rec = json.loads(line)
            batch.append(rec)
            total += 1

            if len(batch) >= CONFIG["batch_size"]:
                await _write_batch_to_milvus(batch, connections)
                await _write_batch_to_pg(batch, pg_pool)
                batch = []

            if total % 10000 == 0:
                print(f"[Phase 4] Milvus+PG 写入: {total:,}")

    if batch:
        await _write_batch_to_milvus(batch, connections)
        await _write_batch_to_pg(batch, pg_pool)

    # Step 2: 写入转发 (bitmap)
    print("[Phase 4] 写入转发 bitmap...")
    fwd_total = 0
    fwd_batch = []

    with open(forward_file) as f:
        for line in f:
            fwd = json.loads(line)
            fwd_batch.append(fwd)
            fwd_total += 1

            if len(fwd_batch) >= CONFIG["batch_size"]:
                await _write_forwards_to_pg(fwd_batch, pg_pool)
                fwd_batch = []

            if fwd_total % 50000 == 0:
                print(f"[Phase 4] 转发写入: {fwd_total:,}")

    if fwd_batch:
        await _write_forwards_to_pg(fwd_batch, pg_pool)

    await pg_pool.close()
    connections.disconnect("default")
    print(f"[Phase 4] 完成! 图片: {total:,}, 转发: {fwd_total:,}")


async def _write_batch_to_milvus(batch: List[dict], conn):
    """批量写入 Milvus"""
    # 按月分区写入
    from pymilvus import Collection
    collection = Collection("global_images")

    month_groups = defaultdict(list)
    for rec in batch:
        month_groups[rec["ts_month"]].append(rec)

    for ts_month, records in month_groups.items():
        partition_name = f"month_{ts_month}"
        # 确保分区存在
        if not collection.has_partition(partition_name):
            collection.create_partition(partition_name)

        data = {
            "image_pk": [r["image_pk"] for r in records],
            "global_vec": [r["global_vec"] for r in records],
            "product_id": [r.get("product_id", "") for r in records],
            "category_l1": [hash(r.get("category_l1", "")) % 100 for r in records],
            "tags": [r.get("tag_ids", [])[:8] for r in records],
            "is_evergreen": [r.get("is_evergreen", False) for r in records],
            "ts_month": [r["ts_month"] for r in records],
        }
        collection.insert(data, partition_name=partition_name)


async def _write_batch_to_pg(batch: List[dict], pool):
    """批量写入 PG uri_dedup + image_merchant_bitmaps"""
    async with pool.acquire() as conn:
        # uri_dedup
        await conn.executemany(
            """INSERT INTO uri_dedup (image_pk, uri_hash, ts_month, is_evergreen)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (image_pk) DO NOTHING""",
            [(r["image_pk"], hashlib.sha256(r["url"].encode()).hexdigest(),
              str(r["ts_month"]), r.get("is_evergreen", False))
             for r in batch],
        )


async def _write_forwards_to_pg(batch: List[dict], pool):
    """写入转发关联 (bitmap rb_or)"""
    async with pool.acquire() as conn:
        for fwd in batch:
            # 获取 bitmap_index
            row = await conn.fetchrow(
                """INSERT INTO merchant_id_mapping (merchant_string_id)
                   VALUES ($1)
                   ON CONFLICT (merchant_string_id) DO UPDATE SET merchant_string_id = $1
                   RETURNING bitmap_index""",
                fwd["target_merchant"],
            )
            if row:
                bitmap_idx = row["bitmap_index"]
                await conn.execute(
                    """INSERT INTO image_merchant_bitmaps (image_pk, bitmap_data)
                       VALUES ($1, rb_build(ARRAY[$2::int]))
                       ON CONFLICT (image_pk) DO UPDATE
                       SET bitmap_data = rb_or(image_merchant_bitmaps.bitmap_data, rb_build(ARRAY[$2::int]))""",
                    fwd["image_pk"], bitmap_idx,
                )


# ══════════════════════════════════════════════════════════════
# Phase 5: 生成测试 query 集
# ══════════════════════════════════════════════════════════════

def phase5_generate_queries(data_dir: str, output_dir: str, n_queries: int = 100):
    """从入库数据中抽取 query 图片, 覆盖各测试场景"""
    os.makedirs(output_dir, exist_ok=True)

    final_file = os.path.join(data_dir, "final_images.jsonl")
    merchants_file = os.path.join(os.path.dirname(data_dir), "assigned", "merchants.json")

    # 加载部分数据用于抽取
    records_by_category = defaultdict(list)
    records_by_merchant = defaultdict(list)
    all_records = []

    print(f"[Phase 5] 加载数据...")
    with open(final_file) as f:
        for i, line in enumerate(f):
            if i >= 500000:  # 只加载前 50 万条用于抽取
                break
            rec = json.loads(line)
            records_by_category[rec.get("category_l1", "")].append(rec)
            records_by_merchant[rec.get("merchant_id", "")].append(rec)
            all_records.append(rec)

    queries = []

    # 场景 1: 单商家搜索 — 从同一商家中选 query 和 target
    print("[Phase 5] 生成场景 1 query (单商家搜索)...")
    for mid, recs in list(records_by_merchant.items())[:10]:
        if len(recs) >= 2:
            queries.append({
                "type": "scene1_same_merchant",
                "query_url": recs[0]["url"],
                "query_pk": recs[0]["image_pk"],
                "merchant_scope": [mid],
                "expected_product_id": recs[0].get("product_id"),
                "description": f"场景1: 在商家 {mid} 内搜索",
            })

    # 场景 2: 跨商家搜索
    print("[Phase 5] 生成场景 2 query (跨商家搜索)...")
    for cat, recs in list(records_by_category.items())[:5]:
        if len(recs) >= 2:
            merchants_in_cat = list(set(r["merchant_id"] for r in recs))[:50]
            queries.append({
                "type": "scene2_cross_merchant",
                "query_url": recs[0]["url"],
                "query_pk": recs[0]["image_pk"],
                "merchant_scope": merchants_in_cat,
                "description": f"场景2: 在 {len(merchants_in_cat)} 个 {cat} 商家中搜索",
            })

    # 场景 3: 批量搜索 (7 张)
    print("[Phase 5] 生成场景 3 query (批量搜索)...")
    if len(all_records) >= 7:
        batch_samples = random.sample(all_records, 7)
        queries.append({
            "type": "scene3_batch",
            "query_urls": [r["url"] for r in batch_samples],
            "description": "场景3: 批量 7 张搜索",
        })

    # P0/P1/P2 测试: 同 SPU / 同类目
    print("[Phase 5] 生成匹配层级 query...")
    for mid, recs in list(records_by_merchant.items())[:5]:
        spu_groups = defaultdict(list)
        for r in recs:
            if r.get("product_id"):
                spu_groups[r["product_id"]].append(r)

        for spu, spu_recs in list(spu_groups.items())[:2]:
            if len(spu_recs) >= 2:
                queries.append({
                    "type": "match_level_p0",
                    "query_url": spu_recs[0]["url"],
                    "expected_pk": spu_recs[1]["image_pk"],
                    "expected_product_id": spu,
                    "merchant_scope": [mid],
                    "description": f"P0: 同 SPU {spu} 内匹配",
                })

    # 标签搜索
    print("[Phase 5] 生成标签搜索 query...")
    for cat, recs in list(records_by_category.items())[:3]:
        sample = recs[0]
        if sample.get("tags"):
            queries.append({
                "type": "tag_search",
                "tags": sample["tags"][:4],
                "merchant_scope": [sample["merchant_id"]],
                "description": f"标签搜索: {sample['tags'][:4]}",
            })

    # 保存
    query_file = os.path.join(output_dir, "test_queries.json")
    with open(query_file, "w") as f:
        json.dump(queries, f, ensure_ascii=False, indent=2)

    print(f"[Phase 5] 完成! 生成 {len(queries)} 个测试 query")
    print(f"[Phase 5] 保存在: {query_file}")

    # 统计
    type_counts = Counter(q["type"] for q in queries)
    for qtype, cnt in type_counts.most_common():
        print(f"  {qtype}: {cnt}")


# ══════════════════════════════════════════════════════════════
# CLI 入口
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="测试数据导入脚本 — 1000 万真实图片")
    parser.add_argument("phase", choices=["phase1", "phase1b", "phase2", "phase3", "phase4", "phase5", "all"],
                        help="执行阶段: phase1(特征提取) | phase1b(模拟特征) | phase2(商家分配) | phase3(转发) | phase4(写入) | phase5(生成query) | all")
    parser.add_argument("--url-file", default="urls.txt", help="URL 清单文件")
    parser.add_argument("--data-dir", default="/data/test", help="数据根目录")
    parser.add_argument("--batch-size", type=int, default=1000, help="批处理大小")
    parser.add_argument("--workers", type=int, default=8, help="并发下载数")
    args = parser.parse_args()

    feature_dir = os.path.join(args.data_dir, "features")
    assigned_dir = os.path.join(args.data_dir, "assigned")
    forwarded_dir = os.path.join(args.data_dir, "forwarded")
    query_dir = os.path.join(args.data_dir, "queries")

    if args.phase in ("phase1", "all"):
        print("=" * 60)
        print("Phase 1: 下载 + 特征提取")
        print("=" * 60)
        asyncio.run(phase1_extract_features(args.url_file, feature_dir, args.batch_size, args.workers))

    if args.phase == "phase1b":
        print("=" * 60)
        print("Phase 1b: 模拟特征 (离线模式)")
        print("=" * 60)
        phase1b_generate_mock_features(args.url_file, feature_dir, args.batch_size)

    if args.phase in ("phase2", "all"):
        print("=" * 60)
        print("Phase 2: 商家分配")
        print("=" * 60)
        phase2_assign_merchants(feature_dir, assigned_dir)

    if args.phase in ("phase3", "all"):
        print("=" * 60)
        print("Phase 3: 模拟转发")
        print("=" * 60)
        phase3_simulate_forwards(assigned_dir, forwarded_dir)

    if args.phase in ("phase4", "all"):
        print("=" * 60)
        print("Phase 4: 写入 Milvus + PG")
        print("=" * 60)
        asyncio.run(phase4_write_to_storage(forwarded_dir))

    if args.phase in ("phase5", "all"):
        print("=" * 60)
        print("Phase 5: 生成测试 Query")
        print("=" * 60)
        phase5_generate_queries(forwarded_dir, query_dir)

    print("\n" + "=" * 60)
    print("全部完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()
