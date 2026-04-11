#!/usr/bin/env python3
"""
下载 ABO 多视角产品图片 (用于同款不同视角评测)
- 从 listings_0.json 选 3000 个多视角产品 (覆盖多品类)
- 每产品下载 main + 2-4 other 视角
- 从 S3 直接下载图片
- 输出: /data/datasets/abo/products/
"""
import json, csv, os, time, hashlib
import asyncio, aiohttp

OUT_DIR = "/data/datasets/abo/products"
S3_BASE = "https://amazon-berkeley-objects.s3.amazonaws.com/images/original"
MAX_PRODUCTS = 3000
MAX_VIEWS = 5
CONCURRENCY = 10  # 控制速度
LOG = "/data/imgsrch/task_logs/download_abo.log"

def main():
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s",
        handlers=[logging.FileHandler(LOG), logging.StreamHandler()])
    log = logging.getLogger("abo")

    # 加载 image_id → path
    img_paths = {}
    with open("/data/datasets/abo/images.csv") as f:
        for row in csv.DictReader(f):
            img_paths[row["image_id"]] = row["path"]
    log.info(f"图片映射: {len(img_paths):,}")

    # 加载产品 (多视角)
    products = []
    with open("/data/datasets/abo/listings_0.json") as f:
        for line in f:
            d = json.loads(line)
            main = d.get("main_image_id", "")
            others = d.get("other_image_id", [])
            all_imgs = ([main] + others) if main else others
            valid = [img for img in all_imgs if img in img_paths]
            if len(valid) >= 2:
                products.append({
                    "item_id": d["item_id"],
                    "images": valid[:MAX_VIEWS],
                    "type": d.get("product_type", [{}])[0].get("value", "") if d.get("product_type") else "",
                })

    # 按品类多样性选择
    import random
    random.seed(42)
    random.shuffle(products)
    selected = products[:MAX_PRODUCTS]
    log.info(f"选中: {len(selected)} 产品, {sum(len(p['images']) for p in selected)} 图片")

    os.makedirs(OUT_DIR, exist_ok=True)

    async def download_all():
        sem = asyncio.Semaphore(CONCURRENCY)
        ok = fail = 0
        manifest = []

        async def dl(session, item_id, img_id, view_idx):
            nonlocal ok, fail
            path = img_paths[img_id]
            url = f"{S3_BASE}/{path}"
            fname = f"{item_id}_v{view_idx}.jpg"
            fpath = os.path.join(OUT_DIR, fname)
            if os.path.exists(fpath) and os.path.getsize(fpath) > 1000:
                ok += 1
                return fname

            async with sem:
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                        if r.status == 200:
                            data = await r.read()
                            if len(data) > 1000:
                                with open(fpath, "wb") as f: f.write(data)
                                ok += 1
                                return fname
                except:
                    pass
                fail += 1
                return None

        conn = aiohttp.TCPConnector(limit=CONCURRENCY)
        async with aiohttp.ClientSession(connector=conn) as session:
            for i, prod in enumerate(selected):
                tasks = []
                for vi, img_id in enumerate(prod["images"]):
                    tasks.append(dl(session, prod["item_id"], img_id, vi+1))
                results = await asyncio.gather(*tasks)
                valid_files = [r for r in results if r]
                if len(valid_files) >= 2:
                    manifest.append({
                        "item_id": prod["item_id"],
                        "type": prod["type"],
                        "files": valid_files,
                        "views": len(valid_files),
                    })
                if (i+1) % 200 == 0:
                    log.info(f"  [{i+1}/{len(selected)}] ok={ok} fail={fail} products={len(manifest)}")

        # 保存 manifest
        with open(os.path.join(OUT_DIR, "manifest.json"), "w") as f:
            json.dump({"products": manifest, "total": len(manifest),
                       "total_images": sum(p["views"] for p in manifest)}, f, indent=2)
        log.info(f"\n完成: {len(manifest)} 产品, {sum(p['views'] for p in manifest)} 图, ok={ok} fail={fail}")

    asyncio.run(download_all())

if __name__ == "__main__":
    main()
