#!/usr/bin/env python3
"""衍生数据 L2 继承: 从原始 PK 的 L2 传播到衍生记录"""
import json, time, logging
from pymilvus import connections, Collection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("inherit")

MILVUS_HOST, MILVUS_PORT = "localhost", 19530
COLLECTION = "img_202603_svip"
BATCH_SIZE = 500

def main():
    log.info("=== 衍生 L2 继承 ===")

    # 加载映射
    log.info("加载 orig_l2_map...")
    with open('/data/imgsrch/task_logs/orig_l2_map.json') as f:
        orig_l2 = json.load(f)
    log.info(f"原始 L2 映射: {len(orig_l2):,}")

    log.info("加载 derive_to_orig...")
    with open('/data/imgsrch/task_logs/derive_to_orig.json') as f:
        derive_map = json.load(f)
    log.info(f"衍生→原始映射: {len(derive_map):,}")

    # 预计算: 衍生PK → L1/L2/L3
    log.info("计算衍生 L2...")
    derive_l2 = {}
    matched = 0
    for dpk, opk in derive_map.items():
        if opk in orig_l2:
            derive_l2[dpk] = orig_l2[opk]
            matched += 1
    log.info(f"可继承: {matched:,} / {len(derive_map):,} ({matched/len(derive_map)*100:.1f}%)")
    del derive_map  # 释放内存

    # Milvus 批量更新
    connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT, timeout=30)
    coll = Collection(COLLECTION)
    coll.load()

    updated = 0
    skipped = 0
    t0 = time.time()

    fields = ['image_pk','global_vec','category_l1','category_l2','category_l3',
              'tags','color_code','material_code','style_code','season_code',
              'is_evergreen','ts_month','promoted_at','product_id','created_at']

    it = coll.query_iterator(expr='category_l2 == 0', output_fields=fields, batch_size=BATCH_SIZE)
    batch_num = 0
    while True:
        batch = it.next()
        if not batch: break
        batch_num += 1

        upsert_rows = []
        for row in batch:
            pk = row['image_pk']
            if pk in derive_l2:
                cls = derive_l2[pk]
                row['global_vec'] = [float(x) for x in row['global_vec']]
                row['category_l1'] = cls['l1']
                row['category_l2'] = cls['l2']
                row['category_l3'] = cls['l3']
                upsert_rows.append(row)
            else:
                skipped += 1

        if upsert_rows:
            try:
                coll.upsert(upsert_rows)
                updated += len(upsert_rows)
            except Exception as e:
                log.warning(f"Upsert failed: {e}")

        if batch_num % 100 == 0:
            elapsed = time.time() - t0
            rate = updated / elapsed if elapsed > 0 else 0
            log.info(f"Batch {batch_num}: updated={updated:,} skipped={skipped:,} rate={rate:.0f}/s")

    it.close()
    coll.flush()

    elapsed = time.time() - t0
    log.info(f"=== 完成: updated={updated:,} skipped={skipped:,} {elapsed:.0f}s ({updated/elapsed:.0f}/s) ===")

if __name__ == "__main__":
    main()
