import asyncio, aiohttp, os, hashlib, time, logging, random, sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s",
    handlers=[logging.FileHandler("/data/imgsrch/task_logs/download_to_10m.log"),
              logging.StreamHandler()])
log = logging.getLogger("dl10m")

TARGET = 10_000_000
IMG_SIZE = 512
CONCURRENCY = 500
DST_DIR = "/data/test-images"

# 从大文件中取 URL (urls.txt 和 urls_cos.txt 各有 1000万)
URL_FILES = [
    "/data/imgsrch/urls/urls.txt",
    "/data/imgsrch/urls/urls_cos.txt",
]

def collect_urls():
    existing = set()
    log.info("扫描已有图片...")
    for f in os.listdir(DST_DIR):
        if f.endswith(".jpg"):
            existing.add(f.replace(".jpg", ""))
    log.info(f"  已有: {len(existing):,}")
    
    needed = TARGET - len(existing)
    if needed <= 0:
        log.info(f"  已达标 {len(existing):,} >= {TARGET:,}")
        return []
    
    log.info(f"  还需: {needed:,}")
    
    urls = []
    seen = set()
    for fpath in URL_FILES:
        if len(urls) >= needed: break
        if not os.path.exists(fpath): continue
        log.info(f"  读取 {fpath}...")
        with open(fpath) as f:
            for line in f:
                url = line.strip()
                if not url: continue
                pk = hashlib.md5(url.encode()).hexdigest()
                if pk not in existing and pk not in seen:
                    seen.add(pk)
                    urls.append(url)
                    if len(urls) >= needed: break
        log.info(f"    累计 {len(urls):,} URLs")
    
    log.info(f"  最终: {len(urls):,}")
    return urls

async def download_all(urls):
    sem = asyncio.Semaphore(CONCURRENCY)
    state = {"ok":0,"fail":0,"skip":0,"bytes":0}
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
                        elif r.status in (404,403): break
                except: await asyncio.sleep(0.3)
            state["fail"] += 1
    
    conn = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=conn) as session:
        for i in range(0, len(urls), 5000):
            await asyncio.gather(*[dl(session, u) for u in urls[i:i+5000]])
            done = state["ok"]+state["fail"]+state["skip"]
            elapsed = time.time()-t0
            rate = done/max(elapsed,1)
            gb = state["bytes"]/1024**3
            eta = (len(urls)-done)/max(rate,1)/60
            log.info(f"[{done:,}/{len(urls):,}] ok={state['ok']:,} fail={state['fail']:,} | {rate:.0f}/s | {gb:.1f}GB | ETA {eta:.0f}min")
    
    log.info(f"完成: ok={state['ok']:,} fail={state['fail']:,} | {(time.time()-t0)/60:.0f}min")

urls = collect_urls()
if urls:
    asyncio.run(download_all(urls))
