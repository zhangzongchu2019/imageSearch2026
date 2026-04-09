#!/usr/bin/env python3
"""衍生 L2 继承 — 多线程读写分离 + 大批量"""
import json, time, logging, threading, queue
from pymilvus import connections, Collection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("inherit")

MILVUS_HOST, MILVUS_PORT = "localhost", 19530
COLLECTION = "img_202603_svip"
READ_BATCH = 2000
WRITE_WORKERS = 4
WRITE_BATCH = 1000

# 共享队列: reader → writers
write_queue = queue.Queue(maxsize=20)
stats = {"updated": 0, "skipped": 0, "write_errors": 0}
stats_lock = threading.Lock()
done_reading = threading.Event()


def writer_thread(worker_id):
    """写线程: 从队列取批次, upsert 到 Milvus"""
    alias = f"wr{worker_id}"
    connections.connect(alias, host=MILVUS_HOST, port=MILVUS_PORT, timeout=30)
    coll = Collection(COLLECTION, using=alias)

    while True:
        try:
            batch = write_queue.get(timeout=5)
        except queue.Empty:
            if done_reading.is_set():
                break
            continue

        if batch is None:  # poison pill
            break

        try:
            coll.upsert(batch)
            with stats_lock:
                stats["updated"] += len(batch)
        except Exception as e:
            with stats_lock:
                stats["write_errors"] += len(batch)
            log.warning(f"Writer {worker_id} upsert failed: {str(e)[:60]}")

        write_queue.task_done()

    connections.disconnect(alias)


def main():
    log.info(f"=== 衍生 L2 继承 (fast: {WRITE_WORKERS} write threads, batch={READ_BATCH}) ===")

    # 加载映射
    log.info("加载映射...")
    with open('/data/imgsrch/task_logs/orig_l2_map.json') as f:
        orig_l2 = json.load(f)
    with open('/data/imgsrch/task_logs/derive_to_orig.json') as f:
        derive_map = json.load(f)
    log.info(f"orig_l2: {len(orig_l2):,}, derive_map: {len(derive_map):,}")

    # 预计算
    derive_l2 = {}
    for dpk, opk in derive_map.items():
        if opk in orig_l2:
            derive_l2[dpk] = orig_l2[opk]
    log.info(f"可继承: {len(derive_l2):,}")
    del derive_map

    # 启动写线程
    writers = []
    for i in range(WRITE_WORKERS):
        t = threading.Thread(target=writer_thread, args=(i,), daemon=True)
        t.start()
        writers.append(t)

    # 读线程 (主线程)
    connections.connect("reader", host=MILVUS_HOST, port=MILVUS_PORT, timeout=30)
    coll = Collection(COLLECTION, using="reader")
    coll.load()

    fields = ['image_pk', 'global_vec', 'category_l1', 'category_l2', 'category_l3',
              'tags', 'color_code', 'material_code', 'style_code', 'season_code',
              'is_evergreen', 'ts_month', 'promoted_at', 'product_id', 'created_at']

    t0 = time.time()
    read_count = 0

    it = coll.query_iterator(expr='category_l2 == 0', output_fields=fields, batch_size=READ_BATCH)
    while True:
        batch = it.next()
        if not batch:
            break
        read_count += len(batch)

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
                with stats_lock:
                    stats["skipped"] += 1

        # 分批放入写队列
        for i in range(0, len(upsert_rows), WRITE_BATCH):
            write_queue.put(upsert_rows[i:i+WRITE_BATCH])

        if read_count % 20000 == 0:
            elapsed = time.time() - t0
            with stats_lock:
                u = stats["updated"]
            rate = u / elapsed if elapsed > 0 else 0
            remaining = len(derive_l2) - u
            eta = remaining / rate / 60 if rate > 0 else 0
            log.info(f"read={read_count:,} updated={u:,} skip={stats['skipped']:,} err={stats['write_errors']:,} rate={rate:.0f}/s ETA={eta:.0f}min")

    it.close()
    done_reading.set()

    # 等写队列清空
    write_queue.join()
    for _ in range(WRITE_WORKERS):
        write_queue.put(None)
    for t in writers:
        t.join(timeout=30)

    coll.flush()
    elapsed = time.time() - t0
    log.info(f"=== 完成: updated={stats['updated']:,} skip={stats['skipped']:,} err={stats['write_errors']:,} {elapsed:.0f}s ({stats['updated']/elapsed:.0f}/s) ===")


if __name__ == "__main__":
    main()
