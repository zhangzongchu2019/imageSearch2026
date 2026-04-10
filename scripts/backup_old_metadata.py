#!/usr/bin/env python3
"""
备份旧 Milvus Collections 的元数据到外部文件
- 跳过向量数据 (CLIP 256d 不再用)
- 仅导出 image_pk, category_l1/l2, brand_id, ts_month, created_at
- 输出: parquet 文件 (压缩, 高效)

用法:
  python3 backup_old_metadata.py
"""
import os
import time
import json
from datetime import datetime
from pymilvus import connections, utility, Collection

BACKUP_DIR = "/data/imgsrch/backup/h3_old_metadata"
COLLECTIONS_TO_BACKUP = [
    "global_images_hot",
    "img_202603_svip",
    "img_202603_vip",
    "img_202604_svip",
    "img_202604_vip",
    "img_999999",
]
SKIP_COLLECTIONS = ["test_dinov2_h3"]
BATCH_SIZE = 50000


def backup_collection(coll_name, output_path):
    """流式导出 Collection 元数据 (跳过向量字段)"""
    coll = Collection(coll_name)
    coll.load()
    n_total = coll.num_entities
    print(f"\n[{coll_name}] 总条数: {n_total:,}")

    if n_total == 0:
        print(f"  跳过空 collection")
        return 0

    # 检测可用字段 (不同 collection 字段不同)
    fields = [f.name for f in coll.schema.fields if f.dtype.name != "FLOAT_VECTOR"]
    print(f"  导出字段: {fields}")

    # 用 query_iterator 流式拉取 (不卡内存)
    try:
        iterator = coll.query_iterator(
            batch_size=BATCH_SIZE,
            output_fields=fields,
            expr="",
        )
    except Exception as e:
        print(f"  ⚠️  query_iterator 失败: {e}")
        # fallback: 用 query 分页
        return fallback_query(coll, fields, output_path)

    # 写 jsonl (不需要 pandas/parquet 依赖)
    t0 = time.time()
    written = 0
    with open(output_path, "w") as fp:
        while True:
            batch = iterator.next()
            if not batch:
                break
            for row in batch:
                # 移除可能的 vector 字段 (保险)
                clean = {k: v for k, v in row.items() if not k.endswith("_vec") and k != "global_vec"}
                fp.write(json.dumps(clean, default=str) + "\n")
                written += 1

            if written % 100000 == 0:
                elapsed = time.time() - t0
                rate = written / max(elapsed, 1)
                eta = (n_total - written) / max(rate, 1) / 60
                print(f"  [{written:,}/{n_total:,}] {rate:.0f}/s ETA={eta:.0f}min")
        iterator.close()

    coll.release()
    elapsed = time.time() - t0
    print(f"  ✅ 写入 {written:,} 条, {elapsed:.0f}s ({written/max(elapsed,1):.0f}/s)")
    return written


def fallback_query(coll, fields, output_path):
    """fallback: 用 query 限制 16384 分页"""
    n_total = coll.num_entities
    written = 0
    LIMIT = 16384
    with open(output_path, "w") as fp:
        # 用 image_pk 排序分页
        last_pk = ""
        while True:
            try:
                if last_pk:
                    expr = f'image_pk > "{last_pk}"'
                else:
                    expr = ""
                rows = coll.query(
                    expr=expr,
                    output_fields=fields,
                    limit=LIMIT,
                    sort_field="image_pk"
                )
            except Exception as e:
                print(f"  ⚠️  fallback 失败: {e}")
                break
            if not rows:
                break
            for row in rows:
                clean = {k: v for k, v in row.items() if not k.endswith("_vec") and k != "global_vec"}
                fp.write(json.dumps(clean, default=str) + "\n")
                written += 1
            last_pk = rows[-1].get("image_pk", "")
            if written % 100000 == 0:
                print(f"  [{written:,}/{n_total:,}]")
    return written


def main():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    print(f"=== 旧 Collections 元数据备份 ===")
    print(f"输出目录: {BACKUP_DIR}")
    print(f"开始时间: {datetime.now().isoformat()}")

    connections.connect(host="localhost", port="19530")
    all_collections = utility.list_collections()
    print(f"\n所有 collections: {sorted(all_collections)}")

    summary = {
        "backup_dir": BACKUP_DIR,
        "started_at": datetime.now().isoformat(),
        "collections": {},
    }

    total_backed = 0
    for coll_name in COLLECTIONS_TO_BACKUP:
        if coll_name not in all_collections:
            print(f"\n[{coll_name}] 不存在, 跳过")
            continue
        output_path = os.path.join(BACKUP_DIR, f"{coll_name}.jsonl")
        n = backup_collection(coll_name, output_path)
        size_mb = os.path.getsize(output_path) / 1024 / 1024 if os.path.exists(output_path) else 0
        summary["collections"][coll_name] = {
            "count": n,
            "file": output_path,
            "size_mb": round(size_mb, 1),
        }
        total_backed += n

    summary["total_records"] = total_backed
    summary["finished_at"] = datetime.now().isoformat()

    # 写 manifest
    with open(os.path.join(BACKUP_DIR, "manifest.json"), "w") as fp:
        json.dump(summary, fp, indent=2)

    print(f"\n=== 备份完成 ===")
    print(f"总条数: {total_backed:,}")
    for name, info in summary["collections"].items():
        print(f"  {name}: {info['count']:,} 条, {info['size_mb']:.1f} MB")
    print(f"Manifest: {os.path.join(BACKUP_DIR, 'manifest.json')}")


if __name__ == "__main__":
    main()
