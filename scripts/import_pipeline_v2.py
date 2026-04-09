#!/usr/bin/env python3
"""
300万导入 v2 — 流水线: asyncio下载 → 4GPU直接推理(batch32) → 批量写入
"""
import argparse, asyncio, aiohttp, os, json, time, hashlib, logging, glob, subprocess
import psycopg2
from pymilvus import connections, Collection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("pipeline")

IMG_SIZE = 224
DOWNLOAD_CONCURRENCY = 300
MILVUS_BATCH = 500
PG_DSN = "postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search"
GPUS = [0, 1, 2, 3]


# ═════════════════════════════════════════════
# Phase 1: asyncio 下载
# ═════════════════════════════════════════════
async def download_all(urls, download_dir):
    os.makedirs(download_dir, exist_ok=True)
    sem = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
    ok = fail = skip = 0
    t0 = time.time()

    async def dl(session, url):
        nonlocal ok, fail, skip
        pk = hashlib.md5(url.encode()).hexdigest()
        fpath = os.path.join(download_dir, f"{pk}.jpg")
        if os.path.exists(fpath) and os.path.getsize(fpath) > 500:
            skip += 1; return
        dl_url = url
        if "myqcloud.com" in url and "imageMogr2" not in url:
            dl_url += ("&" if "?" in url else "?") + f"imageMogr2/thumbnail/{IMG_SIZE}x{IMG_SIZE}"
        async with sem:
            for _ in range(2):
                try:
                    async with session.get(dl_url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status == 200:
                            data = await r.read()
                            if len(data) > 500:
                                with open(fpath, "wb") as f: f.write(data)
                                ok += 1; return
                    break
                except: await asyncio.sleep(0.3)
            fail += 1

    conn = aiohttp.TCPConnector(limit=DOWNLOAD_CONCURRENCY, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=conn) as session:
        for i in range(0, len(urls), 5000):
            await asyncio.gather(*[dl(session, u) for u in urls[i:i+5000]])
            total = ok + fail + skip
            elapsed = time.time() - t0
            log.info(f"[下载] {total:,}/{len(urls):,} ({total/elapsed:.0f}/s) ok={ok:,} fail={fail:,} skip={skip:,}")

    log.info(f"[下载完成] ok={ok:,} fail={fail:,} skip={skip:,} {time.time()-t0:.0f}s")
    return ok


# ═════════════════════════════════════════════
# Phase 2: 4 GPU 直接推理 (在 gpu_worker 中)
# ═════════════════════════════════════════════
def infer_all_gpus(download_dir, output_dir):
    """在 gpu_worker 容器中启动 4 个 GPU 进程并行推理"""
    os.makedirs(output_dir, exist_ok=True)
    log.info(f"[推理] 启动 {len(GPUS)} GPU 并行推理...")

    procs = []
    for gpu in GPUS:
        out = os.path.join(output_dir, f"infer_gpu{gpu}.jsonl")
        cmd = (f"CUDA_VISIBLE_DEVICES={gpu} python3 /workspace/scripts/infer_gpu_direct.py "
               f"--image-dir {download_dir} --output {out} "
               f"--gpu {gpu} --shard {GPUS.index(gpu)} --total-shards {len(GPUS)} --batch-size 32")
        p = subprocess.Popen(
            ["docker", "exec", "gpu_worker", "bash", "-c", cmd],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        procs.append((gpu, p, out))
        log.info(f"  GPU {gpu} started")

    # 等待完成, 实时输出日志
    t0 = time.time()
    for gpu, p, out in procs:
        for line in iter(p.stdout.readline, b''):
            line = line.decode().strip()
            if line:
                log.info(f"  [GPU{gpu}] {line}")
        p.wait()
        log.info(f"  GPU {gpu} done (exit={p.returncode})")

    # 合并输出
    all_results = []
    for gpu, p, out in procs:
        if os.path.exists(out):
            with open(out) as f:
                for line in f:
                    all_results.append(json.loads(line))

    log.info(f"[推理完成] {len(all_results):,} 条, {time.time()-t0:.0f}s ({len(all_results)/(time.time()-t0):.0f}/s)")
    return all_results


# ═════════════════════════════════════════════
# Phase 3: 批量写入
# ═════════════════════════════════════════════
def write_all(results, url_map, collection_name, ts_month):
    log.info(f"[写入] {len(results):,} → {collection_name}")
    connections.connect("w", host="localhost", port=19530, timeout=30)
    coll = Collection(collection_name, using="w")

    mv = pg = 0
    t0 = time.time()

    for i in range(0, len(results), MILVUS_BATCH):
        batch = results[i:i+MILVUS_BATCH]
        rows = []
        pg_rows = []
        for r in batch:
            r["ts_month"] = ts_month
            r["promoted_at"] = int(time.time())
            r["product_id"] = ""
            r["created_at"] = int(time.time())
            rows.append(r)
            url = url_map.get(r["image_pk"], "")
            if url:
                pg_rows.append((r["image_pk"], hashlib.sha256(url.encode()).hexdigest(), url, ts_month))

        try:
            coll.insert(rows)
            mv += len(rows)
        except Exception as e:
            log.warning(f"Milvus: {str(e)[:60]}")

        if pg_rows:
            try:
                conn = psycopg2.connect(PG_DSN, connect_timeout=3)
                cur = conn.cursor()
                cur.executemany("INSERT INTO uri_dedup (image_pk,uri_hash,uri,ts_month) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", pg_rows)
                conn.commit(); cur.close(); conn.close()
                pg += len(pg_rows)
            except: pass

        done = i + len(batch)
        if done % 50000 == 0:
            log.info(f"[写入] {done:,}/{len(results):,} mv={mv:,} pg={pg:,}")

    coll.flush()
    connections.disconnect("w")
    log.info(f"[写入完成] mv={mv:,} pg={pg:,} {time.time()-t0:.0f}s")


# ═════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url-file", required=True)
    parser.add_argument("--collection", default="img_202603_svip")
    parser.add_argument("--ts-month", type=int, default=202603)
    parser.add_argument("--download-dir", required=True)
    parser.add_argument("--infer-dir", default="/data/imgsrch/infer_output")
    parser.add_argument("--batch-id", default="1")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-infer", action="store_true")
    args = parser.parse_args()

    log.info(f"{'='*60}")
    log.info(f"批次 {args.batch_id} | {args.url_file} | {len(GPUS)} GPU")
    log.info(f"{'='*60}")

    with open(args.url_file) as f:
        urls = [l.strip() for l in f if l.strip()]
    log.info(f"URL: {len(urls):,}")
    url_map = {hashlib.md5(u.encode()).hexdigest(): u for u in urls}

    t_total = time.time()

    # Phase 1
    if not args.skip_download:
        asyncio.run(download_all(urls, args.download_dir))
    else:
        log.info("[下载] 跳过 (--skip-download)")

    # Phase 2
    if not args.skip_infer:
        results = infer_all_gpus(args.download_dir, args.infer_dir)
    else:
        log.info("[推理] 跳过 (--skip-infer), 从文件加载")
        results = []
        for f in glob.glob(os.path.join(args.infer_dir, "infer_gpu*.jsonl")):
            with open(f) as fh:
                for line in fh: results.append(json.loads(line))
        log.info(f"  加载 {len(results):,} 条")

    # Phase 3
    write_all(results, url_map, args.collection, args.ts_month)

    elapsed = time.time() - t_total
    log.info(f"{'='*60}")
    log.info(f"批次 {args.batch_id} 完成: {len(results):,} 条, {elapsed:.0f}s ({len(results)/elapsed:.1f}/s)")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    main()
