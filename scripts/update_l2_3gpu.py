#!/usr/bin/env python3
"""Task 17: 补全 L2/L3 — 3 GPU 60 线程并发"""
import json, time, logging, base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import httpx, psycopg2
from pymilvus import connections, Collection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger("l2")

GPU_ENDPOINTS = [
    "http://172.21.0.8:8090",
    "http://172.21.0.8:8091",
    "http://172.21.0.8:8092",
]
WORKERS = 60

def do_one(pk, url, wid):
    gpu_url = GPU_ENDPOINTS[wid % len(GPU_ENDPOINTS)]
    try:
        img = httpx.get(url, timeout=8, follow_redirects=True)
        if img.status_code != 200: return 'fail', 0
        b64 = base64.b64encode(img.content).decode()
        r = httpx.post(f"{gpu_url}/api/v1/extract", json={"image_b64": b64}, timeout=15)
        if r.status_code != 200: return 'fail', 0
        d = r.json()
        l1 = d.get("category_l1_pred", 0) or 0
        l2 = d.get("category_l2_pred", 0) or 0
        l3 = d.get("category_l3_pred", 0) or 0

        alias = f"w{wid}"
        try: connections.connect(alias, host="localhost", port=19530, timeout=5)
        except: pass
        c = Collection("img_202603_svip", using=alias)
        rows = c.query(expr=f'image_pk == "{pk}"',
                       output_fields=['global_vec','tags','color_code','material_code','style_code',
                                     'season_code','is_evergreen','ts_month','promoted_at','product_id','created_at'],
                       limit=1)
        if not rows: return 'skip', 0
        row = rows[0]
        row['image_pk'] = pk
        row['global_vec'] = [float(x) for x in row['global_vec']]
        row['category_l1'] = l1; row['category_l2'] = l2; row['category_l3'] = l3
        c.upsert([row])
        return 'ok', l2
    except:
        return 'fail', 0

def main():
    log.info(f"=== Task 17: L2 (3 GPU, {WORKERS} threads) ===")
    conn = psycopg2.connect("postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search", connect_timeout=5)
    cur = conn.cursor()
    cur.execute("SELECT image_pk, uri FROM uri_dedup WHERE uri LIKE 'http%%' AND ts_month = 202603")
    recs = cur.fetchall()
    cur.close(); conn.close()
    log.info(f"待处理: {len(recs):,} (全量, upsert 幂等)")

    ok = fail = l2n = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futs = {pool.submit(do_one, pk, url, i % WORKERS): i for i, (pk, url) in enumerate(recs)}
        for f in as_completed(futs):
            s, l2 = f.result()
            if s == 'ok': ok += 1
            else: fail += 1
            if l2: l2n += 1
            total = ok + fail
            if total % 500 == 0:
                elapsed = time.time() - t0
                rate = total / elapsed
                eta = (len(recs) - total) / rate / 60 if rate > 0 else 0
                log.info(f"[{total:,}/{len(recs):,}] {rate:.1f}/s ok={ok:,} l2={l2n:,} fail={fail:,} ETA={eta:.0f}min")

    log.info(f"=== 完成: ok={ok:,} l2={l2n:,} fail={fail:,} {time.time()-t0:.0f}s ===")

if __name__ == "__main__":
    main()
