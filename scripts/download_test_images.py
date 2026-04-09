#!/usr/bin/env python3
"""
下载 100 万张测试图片到 /data/test-images
- 768×768 分辨率 (覆盖所有测试模型: CLIP/DINOv2/SSCD/SigLIP/YOLO)
- 多源混合 (热/常青/冷) 保证多样性
- 详细日志: 参数, 速率, 失败URL

存储: ~100 GB
预计: 30-60 min (300 并发)
"""
import asyncio, aiohttp, os, hashlib, time, logging, random, json, sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/data/imgsrch/task_logs/download_test_images.log"),
        logging.StreamHandler()
    ])
log = logging.getLogger("test-dl")

# === 配置 ===
TOTAL = 1_000_000
IMG_SIZE = 512  # 512×512: 覆盖大多数模型(224/256/384输入), 单文件~51KB, 100万=51GB
CONCURRENCY = 500  # 提高并发应对带宽限制
DST_DIR = "/data/test-images"
FAIL_LOG = "/data/imgsrch/task_logs/download_test_images_fail.txt"
URL_SOURCES = [
    # (file, sample_count, label)
    ("/data/imgsrch/urls/urls_new_batch1.txt", 350_000, "hot_svip_1"),
    ("/data/imgsrch/urls/urls_new_batch2.txt", 350_000, "hot_svip_2"),
    ("/data/imgsrch/urls/urls_evergreen.txt",   150_000, "evergreen"),
    ("/data/imgsrch/urls/urls_cold_svip.txt",   100_000, "cold_svip"),
    ("/data/imgsrch/urls/urls_cold_vip.txt",     50_000, "cold_vip"),
]


def collect_urls():
    """从多个 URL 源采样, 保证多样性"""
    all_urls = []
    seen = set()
    for fpath, target_n, label in URL_SOURCES:
        if not os.path.exists(fpath):
            log.warning(f"  [{label}] 文件不存在: {fpath}")
            continue
        with open(fpath) as f:
            urls = [l.strip() for l in f if l.strip()]
        log.info(f"  [{label}] 总URL: {len(urls):,}, 目标: {target_n:,}")
        if len(urls) > target_n:
            random.seed(42 + hash(label) % 1000)
            urls = random.sample(urls, target_n)
        # 去重
        new_urls = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                new_urls.append(u)
        all_urls.extend(new_urls)
        log.info(f"  [{label}] 加入: {len(new_urls):,} (累计 {len(all_urls):,})")

    # 截断到 TOTAL
    if len(all_urls) > TOTAL:
        all_urls = all_urls[:TOTAL]
    log.info(f"  最终 URL 池: {len(all_urls):,}")
    return all_urls


async def download_all(urls):
    os.makedirs(DST_DIR, exist_ok=True)
    sem = asyncio.Semaphore(CONCURRENCY)
    state = {"ok": 0, "fail": 0, "skip": 0, "bytes": 0}
    fail_fp = open(FAIL_LOG, "w")
    t0 = time.time()
    last_log = 0

    async def dl(session, url):
        pk = hashlib.md5(url.encode()).hexdigest()
        fpath = os.path.join(DST_DIR, f"{pk}.jpg")
        if os.path.exists(fpath) and os.path.getsize(fpath) > 1000:
            state["skip"] += 1
            return

        # CDN 缩放参数
        dl_url = url
        if "myqcloud.com" in url and "imageMogr2" not in url:
            dl_url += ("&" if "?" in url else "?") + f"imageMogr2/thumbnail/{IMG_SIZE}x{IMG_SIZE}"

        async with sem:
            for attempt in range(3):
                try:
                    async with session.get(dl_url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                        if r.status == 200:
                            data = await r.read()
                            if len(data) > 1000:
                                with open(fpath, "wb") as f:
                                    f.write(data)
                                state["ok"] += 1
                                state["bytes"] += len(data)
                                return
                        elif r.status in (404, 403):
                            break
                except:
                    if attempt < 2:
                        await asyncio.sleep(0.3)
            state["fail"] += 1
            fail_fp.write(url + "\n")

    conn = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=conn) as session:
        for i in range(0, len(urls), 5000):
            chunk = urls[i:i+5000]
            await asyncio.gather(*[dl(session, u) for u in chunk])

            elapsed = time.time() - t0
            done = state["ok"] + state["fail"] + state["skip"]
            rate = done / max(elapsed, 1)
            avg_size = state["bytes"] / max(state["ok"], 1)
            disk_gb = state["bytes"] / 1024 / 1024 / 1024
            eta = (len(urls) - done) / max(rate, 1) / 60
            log.info(
                f"[{done:,}/{len(urls):,}] "
                f"ok={state['ok']:,} fail={state['fail']:,} skip={state['skip']:,} | "
                f"{rate:.0f}/s | avg {avg_size/1024:.0f}KB | "
                f"disk {disk_gb:.1f}GB | ETA {eta:.0f}min"
            )

    fail_fp.close()
    elapsed = time.time() - t0

    # 最终统计
    log.info("=" * 70)
    log.info(f"下载完成! 耗时: {elapsed/60:.1f}min")
    log.info(f"  ok:    {state['ok']:,}")
    log.info(f"  fail:  {state['fail']:,}")
    log.info(f"  skip:  {state['skip']:,}")
    log.info(f"  总流量: {state['bytes']/1024/1024/1024:.1f} GB")
    log.info(f"  平均速率: {state['ok']/elapsed:.0f}/s")
    log.info(f"  失败列表: {FAIL_LOG}")

    # 验证目录大小
    total_files = len([f for f in os.listdir(DST_DIR) if f.endswith(".jpg")])
    log.info(f"  目录文件数: {total_files:,}")


def main():
    log.info("=" * 70)
    log.info(f"测试图片下载 | TOTAL={TOTAL:,} | SIZE={IMG_SIZE}x{IMG_SIZE} | CONC={CONCURRENCY}")
    log.info(f"目标目录: {DST_DIR}")
    avg_kb = {224: 17, 384: 32, 512: 51, 768: 105}.get(IMG_SIZE, 50)
    log.info(f"预计存储: ~{TOTAL * avg_kb / 1024 / 1024:.0f} GB")
    log.info("=" * 70)

    log.info("\n[1/2] 收集 URL...")
    urls = collect_urls()

    if not urls:
        log.error("无可用 URL!")
        sys.exit(1)

    log.info(f"\n[2/2] 开始下载 {len(urls):,} 张图...")
    asyncio.run(download_all(urls))


if __name__ == "__main__":
    main()
