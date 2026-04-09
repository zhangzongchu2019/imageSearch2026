#!/usr/bin/env python3
"""
g2-g10 全自动编排: 传输 → 4GPU快速推理 → 写入Milvus+PG
流水线: 传输全部 → 逐批推理+写入
"""
import os, sys, json, time, subprocess, hashlib, logging, glob

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("orchestrator")

GPUS = [0, 1, 2, 3]
WORKERS_PER_GPU = 16
BATCH_SIZE = 64
MILVUS_BATCH = 500
PG_DSN = "postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search"
COLLECTION = "img_202603_svip"
TS_MONTH = 202603

# 批次定义
BATCHES = [
    {"id": "g2",  "src": "/tmp/import_batch2",  "dst": "/data/imgsrch/dl_g2",  "url_file": "/data/imgsrch/urls/urls_new_batch2.txt"},
    {"id": "g3",  "src": "/tmp/import_batch3",  "dst": "/data/imgsrch/dl_g3",  "url_file": "/data/imgsrch/urls/urls_new_batch3.txt"},
    {"id": "g4",  "src": "/tmp/import_g4",      "dst": "/data/imgsrch/dl_g4",  "url_file": "/data/imgsrch/urls/urls_batch_g4.txt"},
    {"id": "g5",  "src": "/tmp/import_g5",      "dst": "/data/imgsrch/dl_g5",  "url_file": "/data/imgsrch/urls/urls_batch_g5.txt"},
    {"id": "g6",  "src": "/tmp/import_g6",      "dst": "/data/imgsrch/dl_g6",  "url_file": "/data/imgsrch/urls/urls_batch_g6.txt"},
    {"id": "g7",  "src": "/tmp/import_g7",      "dst": "/data/imgsrch/dl_g7",  "url_file": "/data/imgsrch/urls/urls_batch_g7.txt"},
    {"id": "g8",  "src": "/tmp/import_g8",      "dst": "/data/imgsrch/dl_g8",  "url_file": "/data/imgsrch/urls/urls_batch_g8.txt"},
    {"id": "g9",  "src": "/tmp/import_g9",      "dst": "/data/imgsrch/dl_g9",  "url_file": "/data/imgsrch/urls/urls_batch_g9.txt"},
    {"id": "g10", "src": "/tmp/import_g10",     "dst": "/data/imgsrch/dl_g10", "url_file": "/data/imgsrch/urls/urls_batch_g10.txt"},
]


def transfer_batch(batch):
    """tar pipe 传输图片到共享目录"""
    src, dst = batch["src"], batch["dst"]
    if os.path.isdir(dst):
        cnt = len(os.listdir(dst))
        if cnt > 900000:
            log.info(f"  [{batch['id']}] 已存在 {cnt:,} 文件, 跳过传输")
            return cnt
        # 目录存在但不完整, 清除重来
    os.makedirs(dst, exist_ok=True)

    if not os.path.isdir(src):
        log.warning(f"  [{batch['id']}] 源目录不存在: {src}")
        return 0

    t0 = time.time()
    cmd = f"cd {src} && find . -maxdepth 1 -name '*.jpg' -print0 | tar cf - --null -T - | (cd {dst} && tar xf -)"
    ret = subprocess.run(["bash", "-c", cmd], capture_output=True, timeout=600)
    cnt = len([f for f in os.listdir(dst) if f.endswith(".jpg")])
    elapsed = time.time() - t0
    log.info(f"  [{batch['id']}] 传输完成: {cnt:,} 文件, {elapsed:.0f}s ({cnt/max(elapsed,1):.0f}/s)")
    return cnt


def infer_batch(batch, infer_dir):
    """4GPU 并行快速推理"""
    dst = batch["dst"]
    os.makedirs(infer_dir, exist_ok=True)

    log.info(f"  [{batch['id']}] 启动 {len(GPUS)} GPU 推理 (workers={WORKERS_PER_GPU}, batch={BATCH_SIZE})")

    procs = []
    for gpu in GPUS:
        out = os.path.join(infer_dir, f"infer_gpu{gpu}.jsonl")
        # 清空旧文件 (通过 docker exec, 因为文件可能由 root 创建)
        subprocess.run(["docker", "exec", "gpu_worker", "bash", "-c",
                        f"mkdir -p {infer_dir} && : > {out}"], timeout=10)
        cmd = (f"CUDA_VISIBLE_DEVICES={gpu} python3 /workspace/scripts/infer_gpu_fast.py "
               f"--image-dir {dst} --output {out} "
               f"--gpu {gpu} --shard {GPUS.index(gpu)} --total-shards {len(GPUS)} "
               f"--batch-size {BATCH_SIZE} --workers {WORKERS_PER_GPU}")
        p = subprocess.Popen(
            ["docker", "exec", "gpu_worker", "bash", "-c", cmd],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        procs.append((gpu, p, out))

    # 监控进度
    t0 = time.time()
    last_report = 0
    while True:
        alive = [p for _, p, _ in procs if p.poll() is None]
        if not alive:
            break

        elapsed = time.time() - t0
        if elapsed - last_report >= 30:
            total = sum(
                sum(1 for _ in open(out)) if os.path.exists(out) else 0
                for _, _, out in procs
            )
            rate = total / max(elapsed, 1)
            log.info(f"  [{batch['id']}] 推理中: {total:,} ({rate:.0f}/s)")
            last_report = elapsed

        time.sleep(5)

    # 读取日志
    for gpu, p, out in procs:
        for line in iter(p.stdout.readline, b''):
            line = line.decode().strip()
            if line and ("[GPU" in line):
                log.info(f"    {line}")
        p.wait()

    # 合并结果
    all_results = []
    for gpu, p, out in procs:
        if os.path.exists(out):
            with open(out) as f:
                for line in f:
                    try: all_results.append(json.loads(line))
                    except: pass

    elapsed = time.time() - t0
    log.info(f"  [{batch['id']}] 推理完成: {len(all_results):,} 条, {elapsed:.0f}s ({len(all_results)/max(elapsed,1):.0f}/s)")
    return all_results


def write_batch(batch, results):
    """写入 Milvus + PG"""
    import psycopg2
    from pymilvus import connections, Collection

    # 加载 URL 映射
    url_map = {}
    if os.path.exists(batch["url_file"]):
        with open(batch["url_file"]) as f:
            for line in f:
                url = line.strip()
                if url:
                    url_map[hashlib.md5(url.encode()).hexdigest()] = url

    log.info(f"  [{batch['id']}] 写入 {len(results):,} → {COLLECTION} (url_map: {len(url_map):,})")

    connections.connect("w", host="localhost", port=19530, timeout=30)
    coll = Collection(COLLECTION, using="w")

    mv = pg = 0
    t0 = time.time()

    for i in range(0, len(results), MILVUS_BATCH):
        chunk = results[i:i+MILVUS_BATCH]
        rows = []
        pg_rows = []
        for r in chunk:
            r["ts_month"] = TS_MONTH
            r["promoted_at"] = int(time.time())
            r["product_id"] = ""
            r["created_at"] = int(time.time())
            rows.append(r)
            url = url_map.get(r["image_pk"], "")
            if url:
                pg_rows.append((r["image_pk"], hashlib.sha256(url.encode()).hexdigest(), url, TS_MONTH))

        try:
            coll.insert(rows)
            mv += len(rows)
        except Exception as e:
            log.warning(f"  Milvus err: {str(e)[:80]}")

        if pg_rows:
            try:
                conn = psycopg2.connect(PG_DSN, connect_timeout=3)
                cur = conn.cursor()
                cur.executemany(
                    "INSERT INTO uri_dedup (image_pk,uri_hash,uri,ts_month) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    pg_rows)
                conn.commit(); cur.close(); conn.close()
                pg += len(pg_rows)
            except: pass

        done = i + len(chunk)
        if done % 100000 == 0:
            elapsed = time.time() - t0
            log.info(f"  [{batch['id']}] 写入: {done:,}/{len(results):,} mv={mv:,} pg={pg:,} ({done/max(elapsed,1):.0f}/s)")

    coll.flush()
    connections.disconnect("w")
    elapsed = time.time() - t0
    log.info(f"  [{batch['id']}] 写入完成: mv={mv:,} pg={pg:,} {elapsed:.0f}s")
    return mv


def cleanup_batch(batch):
    """推理+写入完成后清理共享目录释放磁盘"""
    dst = batch["dst"]
    if os.path.isdir(dst):
        subprocess.run(["bash", "-c", f"rm -rf {dst}"], timeout=300)
        log.info(f"  [{batch['id']}] 已清理 {dst}")


def main():
    log.info("=" * 70)
    log.info(f"g2-g10 全自动编排 | {len(BATCHES)} 批 | {len(GPUS)} GPU | workers={WORKERS_PER_GPU}")
    log.info("=" * 70)

    t_global = time.time()
    total_written = 0

    # Phase 1: 传输前 2 批 (流水线准备)
    log.info("\n[Phase 1] 批量传输...")
    for batch in BATCHES[:2]:
        transfer_batch(batch)

    # Phase 2: 逐批推理+写入, 同时传输下一批
    for i, batch in enumerate(BATCHES):
        log.info(f"\n{'='*50}")
        log.info(f"[{batch['id']}] 开始 ({i+1}/{len(BATCHES)})")
        log.info(f"{'='*50}")

        t_batch = time.time()

        # 确保当前批次已传输
        if not os.path.isdir(batch["dst"]) or len(os.listdir(batch["dst"])) < 100:
            transfer_batch(batch)

        # 预传输下一批 (后台)
        next_transfer = None
        if i + 2 < len(BATCHES):
            next_batch = BATCHES[i + 2]
            if not os.path.isdir(next_batch["dst"]) or len(os.listdir(next_batch["dst"])) < 100:
                log.info(f"  预传输 {next_batch['id']}...")
                cmd = (f"cd {next_batch['src']} && find . -maxdepth 1 -name '*.jpg' -print0 | "
                       f"tar cf - --null -T - | (mkdir -p {next_batch['dst']} && cd {next_batch['dst']} && tar xf -)")
                next_transfer = subprocess.Popen(["bash", "-c", cmd])

        # 推理
        infer_dir = f"/data/imgsrch/infer_{batch['id']}"
        results = infer_batch(batch, infer_dir)

        if not results:
            log.warning(f"  [{batch['id']}] 推理0条, 跳过写入!")
            continue

        # 写入
        mv = write_batch(batch, results)
        total_written += mv

        # 等预传输完成
        if next_transfer:
            next_transfer.wait()

        # 清理当前批次图片 (释放磁盘)
        cleanup_batch(batch)

        elapsed = time.time() - t_batch
        total_elapsed = time.time() - t_global
        batches_left = len(BATCHES) - i - 1
        eta = total_elapsed / (i + 1) * batches_left
        log.info(f"  [{batch['id']}] 本批: {mv:,} 条, {elapsed:.0f}s | 累计: {total_written:,} | 剩余: {batches_left}批 ETA {eta/3600:.1f}h")

    total_elapsed = time.time() - t_global
    log.info(f"\n{'='*70}")
    log.info(f"全部完成! {total_written:,} 条, {total_elapsed:.0f}s ({total_elapsed/3600:.1f}h)")
    log.info(f"速率: {total_written/max(total_elapsed,1):.0f}/s")
    log.info(f"{'='*70}")


if __name__ == "__main__":
    main()
