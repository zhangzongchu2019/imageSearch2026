#!/usr/bin/env python3
"""
导入新批次商品图: COS缩图下载 → GPU推理(L1/L2/L3) → Milvus + PG
3 GPU 并发, 每张图完整品类标签
"""
import argparse, asyncio, aiohttp, json, time, logging, base64, hashlib, re, os
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from pymilvus import connections, Collection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("import")

GPU_ENDPOINTS = [
    "http://172.21.0.8:8090",
    "http://172.21.0.8:8091",
    "http://172.21.0.8:8092",
]
MILVUS_HOST, MILVUS_PORT = "localhost", 19530
PG_DSN = "postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search"
IMG_SIZE = 224
WORKERS = 40

L1_MAP = {0:0,1001:1001,1002:1002,1003:1003,1004:1004,1005:1005,1006:1006,1007:1007,
           1008:1008,1009:1009,1010:1010,1011:1011,1012:1012,1013:1013,1014:1014,
           1015:1015,1016:1016,1017:1017,1018:1018}

def process_one(url, worker_id, collection, ts_month):
    """单条: 下载 → 推理 → 写入"""
    gpu_url = GPU_ENDPOINTS[worker_id % len(GPU_ENDPOINTS)]
    try:
        import httpx
        # 1. COS 缩图下载
        dl_url = url
        if "myqcloud.com" in url and "imageMogr2" not in url:
            sep = "&" if "?" in url else "?"
            dl_url = f"{url}{sep}imageMogr2/thumbnail/{IMG_SIZE}x{IMG_SIZE}"
        r = httpx.get(dl_url, timeout=10, follow_redirects=True)
        if r.status_code != 200: return None
        img_bytes = r.content
        if len(img_bytes) < 500: return None  # 太小, 可能是错误页

        b64 = base64.b64encode(img_bytes).decode()

        # 2. GPU 推理 (含 L1/L2/L3 + tags)
        r2 = httpx.post(f"{gpu_url}/api/v1/extract", json={"image_b64": b64}, timeout=20)
        if r2.status_code != 200: return None
        d = r2.json()
        vec = d.get("global_vec")
        if not vec or len(vec) < 256: return None
        vec = vec[:256]

        # 3. 构建记录
        image_pk = hashlib.md5(url.encode()).hexdigest()
        return {
            "image_pk": image_pk,
            "global_vec": vec,
            "category_l1": d.get("category_l1_pred", 0) or 0,
            "category_l2": d.get("category_l2_pred", 0) or 0,
            "category_l3": d.get("category_l3_pred", 0) or 0,
            "tags": d.get("tags_pred", []) or [],
            "color_code": 0,
            "material_code": 0,
            "style_code": 0,
            "season_code": 0,
            "is_evergreen": False,
            "ts_month": ts_month,
            "promoted_at": int(time.time()),
            "product_id": "",
            "created_at": int(time.time()),
            "url": url,
        }
    except:
        return None

def write_batch_milvus(coll, records):
    """批量写入 Milvus"""
    rows = []
    for r in records:
        row = dict(r)
        del row["url"]
        rows.append(row)
    if rows:
        coll.insert(rows)
    return len(rows)

def write_batch_pg(records, ts_month):
    """批量写入 PG uri_dedup"""
    try:
        conn = psycopg2.connect(PG_DSN, connect_timeout=5)
        cur = conn.cursor()
        values = []
        for r in records:
            uri_hash = hashlib.sha256(r["url"].encode()).hexdigest()
            values.append((r["image_pk"], uri_hash, r["url"], ts_month))
        cur.executemany(
            "INSERT INTO uri_dedup (image_pk, uri_hash, uri, ts_month) VALUES (%s, %s, %s, %s) ON CONFLICT (image_pk) DO NOTHING",
            values
        )
        conn.commit()
        cur.close(); conn.close()
        return len(values)
    except Exception as e:
        print(f"PG write failed: {e}")
        return 0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url-file", required=True)
    parser.add_argument("--collection", default="img_202603_svip")
    parser.add_argument("--ts-month", type=int, default=202603)
    parser.add_argument("--batch-id", default="1")
    args = parser.parse_args()

    print(f"=== 导入批次 {args.batch_id}: {args.url_file} → {args.collection} ===")

    # 读 URL
    with open(args.url_file) as f:
        urls = [line.strip() for line in f if line.strip()]
    print(f"URL 数量: {len(urls):,}")

    # Milvus
    connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT, timeout=30)
    coll = Collection(args.collection)

    ok = fail = milvus_written = pg_written = 0
    t0 = time.time()
    buffer = []
    BUFFER_SIZE = 200

    CHUNK = WORKERS * 5  # 每次提交 200 个任务
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        from concurrent.futures import as_completed
        for chunk_start in range(0, len(urls), CHUNK):
            chunk = urls[chunk_start:chunk_start+CHUNK]
            futs = [pool.submit(process_one, url, (chunk_start+j) % WORKERS, args.collection, args.ts_month) for j, url in enumerate(chunk)]
            for f in as_completed(futs):
                result = f.result()
                if result:
                    buffer.append(result)
                    ok += 1
                else:
                    fail += 1

                # 批量写入
                if len(buffer) >= BUFFER_SIZE:
                    milvus_written += write_batch_milvus(coll, buffer)
                    pg_written += write_batch_pg(buffer, args.ts_month)
                    buffer = []

                total = ok + fail
                if total % 5000 == 0 and total > 0:
                    elapsed = time.time() - t0
                    rate = total / elapsed
                    eta = (len(urls) - total) / rate / 60 if rate > 0 else 0
                    print(f"[{total:,}/{len(urls):,}] {rate:.1f}/s ok={ok:,} fail={fail:,} milvus={milvus_written:,} pg={pg_written:,} ETA={eta:.0f}min")

        # (分块提交, 无需额外收集)

    # 最后一批
    if buffer:
        milvus_written += write_batch_milvus(coll, buffer)
        pg_written += write_batch_pg(buffer, args.ts_month)

    coll.flush()
    elapsed = time.time() - t0
    print(f"=== 批次 {args.batch_id} 完成 ===")
    print(f"  总数: {len(urls):,}")
    print(f"  成功: {ok:,} ({ok/len(urls)*100:.1f}%)")
    print(f"  失败: {fail:,}")
    print(f"  Milvus: {milvus_written:,}")
    print(f"  PG: {pg_written:,}")
    print(f"  耗时: {elapsed:.0f}s ({ok/elapsed:.1f}/s)")

if __name__ == "__main__":
    main()
