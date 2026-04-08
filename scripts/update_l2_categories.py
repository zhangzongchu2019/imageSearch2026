#!/usr/bin/env python3
"""Task 17: 补全 L2/L3 品类 — 从原始图片重新推理"""
import json, time, logging, base64, re, sys
import httpx, psycopg2
from pymilvus import connections, Collection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("update_l2")

INFERENCE_URL = "http://172.21.0.8:8090"
MILVUS_HOST, MILVUS_PORT = "localhost", 19530
COLLECTION = "img_202603_svip"
PG_DSN = "postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search"
BATCH_SIZE = 50

def extract_from_url(url):
    """下载图片 + GPU 推理, 返回分类结果"""
    try:
        r = httpx.get(url, timeout=8, follow_redirects=True)
        if r.status_code != 200: return None
        b64 = base64.b64encode(r.content).decode()
        r2 = httpx.post(f"{INFERENCE_URL}/api/v1/extract", json={"image_b64": b64}, timeout=15)
        if r2.status_code != 200: return None
        return r2.json()
    except:
        return None

def main():
    log.info("=== Task 17: 补全 L2 品类 (图片重推理) ===")

    # 检查 FashionSigLIP
    health = httpx.get(f"{INFERENCE_URL}/healthz", timeout=5).json()
    log.info(f"FashionSigLIP: {health.get('fashion_siglip')}, L2: {health.get('l2_fashion_groups')}")

    # 从 PG 获取有真实 URL 的原始记录
    conn = psycopg2.connect(PG_DSN, connect_timeout=5)
    cur = conn.cursor()
    cur.execute("SELECT image_pk, uri FROM uri_dedup WHERE uri LIKE 'http%%' AND ts_month = 202603")
    url_records = cur.fetchall()
    cur.close(); conn.close()
    log.info(f"有图片 URL 的原始记录: {len(url_records):,}")

    # Milvus 连接
    connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT, timeout=30)
    coll = Collection(COLLECTION)
    coll.load()

    updated = 0
    failed = 0
    l2_found = 0
    t0 = time.time()

    for i, (pk, url) in enumerate(url_records):
        # 下载 + 推理
        d = extract_from_url(url)
        if not d:
            failed += 1
            continue

        l1_code = d.get("category_l1_pred", 0) or 0
        l2_code = d.get("category_l2_pred", 0) or 0
        l3_code = d.get("category_l3_pred", 0) or 0

        if l2_code != 0:
            l2_found += 1

        # 查 Milvus 中该 PK 的当前数据
        try:
            rows = coll.query(expr=f'image_pk == "{pk}"',
                             output_fields=['global_vec','tags','color_code','material_code',
                                           'style_code','season_code','is_evergreen','ts_month',
                                           'promoted_at','product_id','created_at'],
                             limit=1)
            if not rows:
                continue

            row = rows[0]
            row['image_pk'] = pk
            row['global_vec'] = [float(x) for x in row['global_vec']]
            row['category_l1'] = l1_code
            row['category_l2'] = l2_code
            row['category_l3'] = l3_code

            coll.upsert([row])
            updated += 1
        except Exception as e:
            log.warning(f"Upsert {pk[:16]} failed: {e}")
            failed += 1

        if (i + 1) % 100 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            eta = (len(url_records) - i - 1) / rate / 60
            log.info(f"Progress: {i+1}/{len(url_records)} ({rate:.1f}/s) updated={updated} l2_found={l2_found} failed={failed} ETA={eta:.0f}min")

    coll.flush()
    elapsed = time.time() - t0
    log.info(f"=== 完成: {updated:,} updated, {l2_found:,} with L2, {failed:,} failed, {elapsed:.0f}s ===")

if __name__ == "__main__":
    main()
