#!/usr/bin/env python3
"""
下载第 2 批 100万 图片 (接着第 1 批的 URL)
- 使用之前未下载的 URL 文件: batch3, g4-g7
- 跳过已存在的图片 (去重)
- 512×512 分辨率
- 存到 /data/test-images/ (和第 1 批同目录)
"""
import asyncio, aiohttp, os, hashlib, time, logging, random, sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[
        logging.FileHandler("/data/imgsrch/task_logs/download_batch2.log"),
        logging.StreamHandler()
    ])
log = logging.getLogger("dl-b2")

TOTAL = 1_000_000
IMG_SIZE = 512
CONCURRENCY = 500
DST_DIR = "/data/test-images"
FAIL_LOG = "/data/imgsrch/task_logs/download_batch2_fail.txt"

# 第 2 批: 使用之前没用过的 URL 文件
URL_SOURCES = [
    ("/data/imgsrch/urls/urls_new_batch3.txt", 300_000, "hot_svip_3"),
    ("/data/imgsrch/urls/urls_batch_g4.txt", 250_000, "g4"),
    ("/data/imgsrch/urls/urls_batch_g5.txt", 250_000, "g5"),
    ("/data/imgsrch/urls/urls_batch_g6.txt", 200_000, "g6"),
]


def collect_urls():
    existing_pks = set()
    log.info(f"扫描已有图片...")
    for f in os.listdir(DST_DIR):
        if f.endswith(".jpg"):
            existing_pks.add(f.replace(".jpg", ""))
    log.info(f"  已有: {len(existing_pks):,}")

    all_urls = []
    seen = set()
    for fpath, target_n, label in URL_SOURCES:
        if not os.path.exists(fpath):
            log.warning(f"  [{label}] 不存在: {fpath}")
            continue
        with open(fpath) as f:
            urls = [l.strip() for l in f if l.strip()]
        log.info(f"  [{label}] 总: {len(urls):,}, 目标: {target_n:,}")
        random.seed(42 + hash(label) % 1000)
        if len(urls) > target_n:
            urls = random.sample(urls, target_n)
        new = []
        for u in urls:
            pk = hashlib.md5(u.encode()).hexdigest()
            if pk not in seen and pk not in existing_pks:
                seen.add(pk)
                new.append(u)
        all_urls.extend(new)
        log.info(f"  [{label}] 新增: {len(new):,} (累计 {len(all_urls):,})")

    if len(all_urls) > TOTAL:
        all_urls = all_urls[:TOTAL]
    log.info(f"  最终: {len(all_urls):,}")
    return all_urls


async def download_all(urls):
    os.makedirs(DST_DIR, exist_ok=True)
    sem = asyncio.Semaphore(CONCURRENCY)
    state = {"ok": 0, "fail": 0, "skip": 0, "bytes": 0}
    fail_fp = open(FAIL_LOG, "w")
    t0 = time.time()

    async def dl(session, url):
        pk = hashlib.md5(url.encode()).hexdigest()
        fpath = os.path.join(DST_DIR, f"{pk}.jpg")
        if os.path.exists(fpath) and os.path.getsize(fpath) > 1000:
            state["skip"] += 1; return
        dl_url = url
        if "myqcloud.com" in url and "imageMogr2" not in url:
            dl_url += ("&" if "?" in url else "?") + f"imageMogr2/thumbnail/{IMG_SIZE}x{IMG_SIZE}"
        async with sem:
            for _ in range(3):
                try:
                    async with session.get(dl_url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                        if r.status == 200:
                            data = await r.read()
                            if len(data) > 1000:
                                with open(fpath, "wb") as f: f.write(data)
                                state["ok"] += 1; state["bytes"] += len(data); return
                        elif r.status in (404, 403): break
                except: await asyncio.sleep(0.3)
            state["fail"] += 1; fail_fp.write(url + "\n")

    conn = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=conn) as session:
        for i in range(0, len(urls), 5000):
            await asyncio.gather(*[dl(session, u) for u in urls[i:i+5000]])
            done = state["ok"] + state["fail"] + state["skip"]
            elapsed = time.time() - t0
            rate = done / max(elapsed, 1)
            disk_gb = state["bytes"] / 1024**3
            eta = (len(urls) - done) / max(rate, 1) / 60
            log.info(f"[{done:,}/{len(urls):,}] ok={state['ok']:,} fail={state['fail']:,} skip={state['skip']:,} | {rate:.0f}/s | {disk_gb:.1f}GB | ETA {eta:.0f}min")

    fail_fp.close()
    elapsed = time.time() - t0
    log.info(f"完成: ok={state['ok']:,} fail={state['fail']:,} skip={state['skip']:,} | {elapsed/60:.1f}min | {state['bytes']/1024**3:.1f}GB")


def main():
    log.info("=" * 60)
    log.info(f"第 2 批下载 | TOTAL={TOTAL:,} | SIZE={IMG_SIZE} | CONC={CONCURRENCY}")
    log.info("=" * 60)
    urls = collect_urls()
    if not urls:
        log.error("无可用 URL!")
        sys.exit(1)
    asyncio.run(download_all(urls))

if __name__ == "__main__":
    main()
