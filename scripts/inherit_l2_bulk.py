#!/usr/bin/env python3
"""衍生 L2 继承 — 批量 PK 查询 + 批量 upsert"""
import json, time, logging
from concurrent.futures import ThreadPoolExecutor
from pymilvus import connections, Collection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("bulk")

MILVUS_HOST, MILVUS_PORT = "localhost", 19530
COLLECTION = "img_202603_svip"
QUERY_BATCH = 500
WRITE_WORKERS = 4

def process_batch(pks_with_l2, worker_id):
    alias = f"bw{worker_id}"
    try: connections.connect(alias, host=MILVUS_HOST, port=MILVUS_PORT, timeout=10)
    except: pass
    coll = Collection(COLLECTION, using=alias)
    pks = [p[0] for p in pks_with_l2]
    l2_map = {p[0]: p[1] for p in pks_with_l2}
    pk_str = '","'.join(pks)
    expr = f'image_pk in ["{pk_str}"]'
    fields = ['image_pk','global_vec','category_l1','category_l2','category_l3',
              'tags','color_code','material_code','style_code','season_code',
              'is_evergreen','ts_month','promoted_at','product_id','created_at']
    try:
        rows = coll.query(expr=expr, output_fields=fields, limit=len(pks)+10)
    except:
        return 0, len(pks)
    upsert_rows = []
    for row in rows:
        pk = row['image_pk']
        if pk in l2_map:
            cls = l2_map[pk]
            row['global_vec'] = [float(x) for x in row['global_vec']]
            row['category_l1'] = cls['l1']
            row['category_l2'] = cls['l2']
            row['category_l3'] = cls['l3']
            upsert_rows.append(row)
    if upsert_rows:
        try:
            coll.upsert(upsert_rows)
            return len(upsert_rows), 0
        except:
            return 0, len(upsert_rows)
    return 0, 0

def main():
    log.info("=== 衍生 L2 继承 (bulk) ===")
    with open('/data/imgsrch/task_logs/orig_l2_map.json') as f:
        orig_l2 = json.load(f)
    with open('/data/imgsrch/task_logs/derive_to_orig.json') as f:
        derive_map = json.load(f)
    todo = []
    for dpk, opk in derive_map.items():
        if opk in orig_l2:
            todo.append((dpk, orig_l2[opk]))
    del derive_map
    log.info(f"待更新: {len(todo):,}")

    updated = failed = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=WRITE_WORKERS) as pool:
        futures = []
        for i in range(0, len(todo), QUERY_BATCH):
            batch = todo[i:i+QUERY_BATCH]
            f = pool.submit(process_batch, batch, i // QUERY_BATCH % WRITE_WORKERS)
            futures.append(f)
            if len(futures) >= WRITE_WORKERS * 10:
                for ff in futures:
                    ok, err = ff.result()
                    updated += ok; failed += err
                futures = []
                done = i + QUERY_BATCH
                if done % 50000 == 0:
                    elapsed = time.time() - t0
                    rate = updated / elapsed if elapsed > 0 else 0
                    eta = (len(todo) - done) / rate / 60 if rate > 0 else 0
                    log.info(f"[{done:,}/{len(todo):,}] {rate:.0f}/s updated={updated:,} fail={failed:,} ETA={eta:.0f}min")
        for ff in futures:
            ok, err = ff.result()
            updated += ok; failed += err

    connections.connect("flush", host=MILVUS_HOST, port=MILVUS_PORT, timeout=30)
    Collection(COLLECTION, using="flush").flush()
    elapsed = time.time() - t0
    log.info(f"=== 完成: updated={updated:,} fail={failed:,} {elapsed:.0f}s ({updated/elapsed:.0f}/s) ===")

if __name__ == "__main__":
    main()
