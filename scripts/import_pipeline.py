#!/usr/bin/env python3
"""
300万商品图导入 — 流水线架构
Phase 1: asyncio 500并发下载 → 本地磁盘
Phase 2: 3GPU 批量推理 (含L2/L3)
Phase 3: 批量写入 Milvus + PG
"""
import argparse, asyncio, aiohttp, os, json, time, base64, hashlib, logging, glob
from concurrent.futures import ThreadPoolExecutor, as_completed
import httpx, psycopg2
from pymilvus import connections, Collection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("pipeline")

IMG_SIZE = 224
DOWNLOAD_CONCURRENCY = 300
GPU_ENDPOINTS = ["http://172.21.0.8:8090", "http://172.21.0.8:8091", "http://172.21.0.8:8092"]
GPU_WORKERS = 30
MILVUS_BATCH = 500
PG_DSN = "postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search"


# ═══════════════════════════════════════════════════════
# Phase 1: 异步批量下载
# ═══════════════════════════════════════════════════════
async def download_all(urls, download_dir):
    """asyncio 300并发下载, COS服务端缩图"""
    os.makedirs(download_dir, exist_ok=True)
    sem = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
    ok = fail = skip = 0
    t0 = time.time()

    async def dl_one(session, url, idx):
        nonlocal ok, fail, skip
        pk = hashlib.md5(url.encode()).hexdigest()
        fpath = os.path.join(download_dir, f"{pk}.jpg")
        if os.path.exists(fpath) and os.path.getsize(fpath) > 500:
            skip += 1
            return

        dl_url = url
        if "myqcloud.com" in url and "imageMogr2" not in url:
            dl_url += ("&" if "?" in url else "?") + f"imageMogr2/thumbnail/{IMG_SIZE}x{IMG_SIZE}"

        async with sem:
            for attempt in range(2):
                try:
                    async with session.get(dl_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            if len(data) > 500:
                                with open(fpath, "wb") as f:
                                    f.write(data)
                                ok += 1
                                return
                    fail += 1; return
                except:
                    if attempt == 0:
                        await asyncio.sleep(0.5)
            fail += 1

    connector = aiohttp.TCPConnector(limit=DOWNLOAD_CONCURRENCY, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [dl_one(session, url, i) for i, url in enumerate(urls)]
        # 分批执行, 每 5000 个报一次进度
        batch_size = 5000
        for i in range(0, len(tasks), batch_size):
            await asyncio.gather(*tasks[i:i+batch_size])
            total = ok + fail + skip
            elapsed = time.time() - t0
            rate = total / elapsed if elapsed > 0 else 0
            log.info(f"[下载] {total:,}/{len(urls):,} ({rate:.0f}/s) ok={ok:,} fail={fail:,} skip={skip:,}")

    log.info(f"[下载完成] ok={ok:,} fail={fail:,} skip={skip:,} {time.time()-t0:.0f}s")
    return ok


# ═══════════════════════════════════════════════════════
# Phase 2: 3GPU 批量推理
# ═══════════════════════════════════════════════════════
def infer_one(fpath, gpu_url):
    """单张推理"""
    try:
        with open(fpath, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        r = httpx.post(f"{gpu_url}/api/v1/extract", json={"image_b64": b64}, timeout=15)
        if r.status_code != 200:
            return None
        d = r.json()
        vec = d.get("global_vec")
        if not vec or len(vec) < 256:
            return None
        pk = os.path.basename(fpath).replace(".jpg", "")
        return {
            "image_pk": pk,
            "global_vec": vec[:256],
            "category_l1": d.get("category_l1_pred", 0) or 0,
            "category_l2": d.get("category_l2_pred", 0) or 0,
            "category_l3": d.get("category_l3_pred", 0) or 0,
            "tags": d.get("tags_pred", []) or [],
        }
    except:
        return None


def infer_all(download_dir, url_map):
    """多线程批量推理"""
    files = sorted(glob.glob(os.path.join(download_dir, "*.jpg")))
    log.info(f"[推理] {len(files):,} 张图片, {GPU_WORKERS} 线程, {len(GPU_ENDPOINTS)} GPU")

    results = []
    ok = fail = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=GPU_WORKERS) as pool:
        futs = {pool.submit(infer_one, fp, GPU_ENDPOINTS[i % len(GPU_ENDPOINTS)]): fp
                for i, fp in enumerate(files)}
        for f in as_completed(futs):
            r = f.result()
            if r:
                # 补充完整字段
                pk = r["image_pk"]
                r["color_code"] = 0
                r["material_code"] = 0
                r["style_code"] = 0
                r["season_code"] = 0
                r["is_evergreen"] = False
                r["ts_month"] = 0  # 后面统一设置
                r["promoted_at"] = int(time.time())
                r["product_id"] = ""
                r["created_at"] = int(time.time())
                r["url"] = url_map.get(pk, "")
                results.append(r)
                ok += 1
            else:
                fail += 1

            total = ok + fail
            if total % 5000 == 0 and total > 0:
                elapsed = time.time() - t0
                rate = total / elapsed
                log.info(f"[推理] {total:,}/{len(files):,} ({rate:.1f}/s) ok={ok:,} fail={fail:,}")

    log.info(f"[推理完成] ok={ok:,} fail={fail:,} {time.time()-t0:.0f}s ({ok/(time.time()-t0):.1f}/s)")
    return results


# ═══════════════════════════════════════════════════════
# Phase 3: 批量写入 Milvus + PG
# ═══════════════════════════════════════════════════════
def write_all(results, collection_name, ts_month):
    """批量写入"""
    log.info(f"[写入] {len(results):,} 条 → {collection_name}")

    connections.connect("writer", host="localhost", port=19530, timeout=30)
    coll = Collection(collection_name, using="writer")

    mv_written = pg_written = 0
    t0 = time.time()

    for i in range(0, len(results), MILVUS_BATCH):
        batch = results[i:i+MILVUS_BATCH]

        # Milvus
        rows = []
        for r in batch:
            row = {k: v for k, v in r.items() if k != "url"}
            row["ts_month"] = ts_month
            rows.append(row)
        try:
            coll.insert(rows)
            mv_written += len(rows)
        except Exception as e:
            log.warning(f"Milvus insert failed: {str(e)[:60]}")

        # PG
        try:
            conn = psycopg2.connect(PG_DSN, connect_timeout=5)
            cur = conn.cursor()
            values = [(r["image_pk"], hashlib.sha256(r["url"].encode()).hexdigest(), r["url"], ts_month)
                      for r in batch if r.get("url")]
            if values:
                cur.executemany(
                    "INSERT INTO uri_dedup (image_pk, uri_hash, uri, ts_month) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    values)
                conn.commit()
                pg_written += len(values)
            cur.close(); conn.close()
        except Exception as e:
            log.warning(f"PG write failed: {str(e)[:60]}")

        done = i + len(batch)
        if done % 10000 == 0:
            elapsed = time.time() - t0
            log.info(f"[写入] {done:,}/{len(results):,} mv={mv_written:,} pg={pg_written:,}")

    coll.flush()
    connections.disconnect("writer")
    log.info(f"[写入完成] mv={mv_written:,} pg={pg_written:,} {time.time()-t0:.0f}s")


# ═══════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url-file", required=True)
    parser.add_argument("--collection", default="img_202603_svip")
    parser.add_argument("--ts-month", type=int, default=202603)
    parser.add_argument("--download-dir", default="/tmp/import_images")
    parser.add_argument("--batch-id", default="1")
    args = parser.parse_args()

    log.info(f"{'='*60}")
    log.info(f"导入批次 {args.batch_id}")
    log.info(f"URL: {args.url_file}")
    log.info(f"Collection: {args.collection}")
    log.info(f"下载目录: {args.download_dir}")
    log.info(f"{'='*60}")

    # 读 URL + 建 pk→url 映射
    with open(args.url_file) as f:
        urls = [l.strip() for l in f if l.strip()]
    log.info(f"URL 数量: {len(urls):,}")

    url_map = {hashlib.md5(url.encode()).hexdigest(): url for url in urls}

    # Phase 1: 下载
    t_total = time.time()
    dl_count = asyncio.run(download_all(urls, args.download_dir))

    # Phase 2: 推理
    results = infer_all(args.download_dir, url_map)

    # Phase 3: 写入
    write_all(results, args.collection, args.ts_month)

    elapsed = time.time() - t_total
    log.info(f"{'='*60}")
    log.info(f"批次 {args.batch_id} 完成")
    log.info(f"  下载: {dl_count:,}")
    log.info(f"  推理: {len(results):,}")
    log.info(f"  总耗时: {elapsed:.0f}s ({len(results)/elapsed:.1f}/s)")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    main()
