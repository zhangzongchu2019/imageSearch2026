#!/usr/bin/env python3
"""
多品牌网站并行爬取 (同一产品多视角图片)
- 每个网站独立线程, 独立浏览器
- 每站限速 5 req/s
- 先分析 DOM → 提取产品链接 → 进入详情页下载多视角图片
- 输出: /data/imgsrch/brand_products/{brand}/
"""
import os, json, time, hashlib, re, logging
import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(threadName)s] %(message)s",
    handlers=[logging.FileHandler("/data/imgsrch/task_logs/crawl_multisite.log"),
              logging.StreamHandler()])
log = logging.getLogger("crawl")

OUT_DIR = "/data/imgsrch/brand_products"
RATE = 0.2  # 5 req/s


def safe_goto(page, url, timeout=20000):
    try:
        page.goto(url, timeout=timeout, wait_until="domcontentloaded")
        time.sleep(1)
        return True
    except Exception as e:
        log.warning(f"  goto 失败: {url[:60]} — {str(e)[:60]}")
        return False


def scroll_load(page, times=5):
    for _ in range(times):
        page.evaluate("window.scrollBy(0, 1500)")
        time.sleep(0.8)


def download_image(page, img_url, fpath):
    try:
        resp = page.request.get(img_url, timeout=10000)
        if resp.status == 200 and len(resp.body()) > 3000:
            with open(fpath, "wb") as f:
                f.write(resp.body())
            return True
    except:
        pass
    return False


def extract_product_images(page, product_url, brand, out_dir, product_id):
    """进入产品详情页, 提取所有视角图片"""
    if not safe_goto(page, product_url):
        return []

    time.sleep(2)
    # 通用提取: 所有 >200px 的图片
    imgs = page.evaluate("""() => {
        const all = document.querySelectorAll('img[src]');
        const result = [];
        for (const img of all) {
            const src = img.src || img.getAttribute('data-src') || '';
            const w = img.naturalWidth || img.width || 0;
            const h = img.naturalHeight || img.height || 0;
            if (src && (w > 150 || h > 150 || src.includes('product') || src.includes('zoom'))) {
                if (!src.includes('logo') && !src.includes('icon') && !src.includes('banner')
                    && !src.includes('svg') && !src.includes('avatar')) {
                    result.push(src.startsWith('//') ? 'https:' + src : src);
                }
            }
        }
        return [...new Set(result)];
    }""")

    # 也抓 data-src, srcset 等懒加载图片
    lazy_imgs = page.evaluate("""() => {
        const all = document.querySelectorAll('[data-src], [data-original], [data-lazy-src]');
        const result = [];
        for (const el of all) {
            const src = el.getAttribute('data-src') || el.getAttribute('data-original') || el.getAttribute('data-lazy-src') || '';
            if (src && src.includes('http') && !src.includes('logo') && !src.includes('icon')) {
                result.push(src);
            }
        }
        return [...new Set(result)];
    }""")

    all_imgs = list(dict.fromkeys(imgs + lazy_imgs))  # 去重保序

    brand_dir = os.path.join(out_dir, brand)
    os.makedirs(brand_dir, exist_ok=True)

    downloaded = []
    for i, img_url in enumerate(all_imgs[:8]):
        fname = f"{product_id}_v{i+1}.jpg"
        fpath = os.path.join(brand_dir, fname)
        if download_image(page, img_url, fpath):
            downloaded.append({"file": fname, "url": img_url, "view": i + 1})
        time.sleep(RATE)

    return downloaded


# ========== 各品牌专用爬虫 ==========

def crawl_nike(browser, max_products=2000):
    """Nike 中国"""
    brand = "nike"
    log.info(f"[{brand}] 启动")
    page = browser.new_page()
    products = []

    for cat_url in [
        "https://www.nike.com.cn/w/men-shoes",
        "https://www.nike.com.cn/w/women-shoes",
        "https://www.nike.com.cn/w/men-clothing-tops",
    ]:
        if len(products) >= max_products: break
        if not safe_goto(page, cat_url, 30000): continue
        scroll_load(page, 8)

        links = page.evaluate("""() => {
            return [...new Set(Array.from(document.querySelectorAll('a[href*="/t/"]'))
                .map(a => a.href).filter(h => h.includes('/t/')))];
        }""")
        log.info(f"  [{brand}] {cat_url.split('/')[-1]}: {len(links)} 产品")

        for url in links[:700]:
            if len(products) >= max_products: break
            pid = hashlib.md5(url.encode()).hexdigest()[:12]
            imgs = extract_product_images(page, url, brand, OUT_DIR, pid)
            if imgs:
                products.append({"id": pid, "url": url, "brand": brand, "images": imgs})
            if len(products) % 20 == 0:
                log.info(f"  [{brand}] {len(products)} 产品, {sum(len(p['images']) for p in products)} 图")

    page.close()
    log.info(f"[{brand}] 完成: {len(products)} 产品")
    return products


def crawl_adidas(browser, max_products=2000):
    """Adidas 中国"""
    brand = "adidas"
    log.info(f"[{brand}] 启动")
    page = browser.new_page()
    products = []

    for cat_url in [
        "https://www.adidas.com.cn/men-shoes",
        "https://www.adidas.com.cn/women-shoes",
        "https://www.adidas.com.cn/men-clothing",
    ]:
        if len(products) >= max_products: break
        if not safe_goto(page, cat_url, 30000): continue
        scroll_load(page, 8)

        links = page.evaluate("""() => {
            return [...new Set(Array.from(document.querySelectorAll('a[href]'))
                .map(a => a.href)
                .filter(h => /\\/[A-Z]{2}\\d{4}/.test(h) || h.includes('/model/') || h.includes('/product/')))];
        }""")
        log.info(f"  [{brand}] {cat_url.split('/')[-1]}: {len(links)} 产品")

        for url in links[:700]:
            if len(products) >= max_products: break
            pid = hashlib.md5(url.encode()).hexdigest()[:12]
            imgs = extract_product_images(page, url, brand, OUT_DIR, pid)
            if imgs:
                products.append({"id": pid, "url": url, "brand": brand, "images": imgs})
            if len(products) % 20 == 0:
                log.info(f"  [{brand}] {len(products)} 产品, {sum(len(p['images']) for p in products)} 图")

    page.close()
    log.info(f"[{brand}] 完成: {len(products)} 产品")
    return products


def crawl_lining(browser, max_products=1500):
    """李宁"""
    brand = "lining"
    log.info(f"[{brand}] 启动")
    page = browser.new_page()
    products = []

    for cat_url in [
        "https://store.lining.com/shop/goodsCate-sale,desc,1,15s1-0-0-1.html",
        "https://store.lining.com/shop/goodsCate-sale,desc,1,15s2-0-0-1.html",
    ]:
        if len(products) >= max_products: break
        if not safe_goto(page, cat_url, 30000): continue
        scroll_load(page, 5)

        links = page.evaluate("""() => {
            return [...new Set(Array.from(document.querySelectorAll('a[href*="goodsDetail"]'))
                .map(a => a.href))];
        }""")
        if not links:
            links = page.evaluate("""() => {
                return [...new Set(Array.from(document.querySelectorAll('a[href]'))
                    .map(a => a.href)
                    .filter(h => h.includes('goods') || h.includes('product') || h.includes('item')))];
            }""")
        log.info(f"  [{brand}] {len(links)} 产品链接")

        for url in links[:500]:
            if len(products) >= max_products: break
            pid = hashlib.md5(url.encode()).hexdigest()[:12]
            imgs = extract_product_images(page, url, brand, OUT_DIR, pid)
            if imgs:
                products.append({"id": pid, "url": url, "brand": brand, "images": imgs})

    page.close()
    log.info(f"[{brand}] 完成: {len(products)} 产品")
    return products


def crawl_zappos(browser, max_products=3000):
    """Zappos (美国, API 友好)"""
    brand = "zappos"
    log.info(f"[{brand}] 启动")
    page = browser.new_page()
    products = []

    for cat_url in [
        "https://www.zappos.com/men-sneakers-athletic-shoes/CK_XARC81wFSAoICQQLiAgMYAQ.zso",
        "https://www.zappos.com/women-sneakers-athletic-shoes/CK_XARC81wFSAoIBQQLiAgMYAQ.zso",
        "https://www.zappos.com/men-boots/CKvXARDe1wFSAoICQQLiAgMYAQ.zso",
    ]:
        if len(products) >= max_products: break
        if not safe_goto(page, cat_url, 30000): continue
        scroll_load(page, 10)

        links = page.evaluate("""() => {
            return [...new Set(Array.from(document.querySelectorAll('a[href*="/p/"]'))
                .map(a => a.href))];
        }""")
        log.info(f"  [{brand}] {len(links)} 产品链接")

        for url in links[:1000]:
            if len(products) >= max_products: break
            pid = hashlib.md5(url.encode()).hexdigest()[:12]
            imgs = extract_product_images(page, url, brand, OUT_DIR, pid)
            if imgs:
                products.append({"id": pid, "url": url, "brand": brand, "images": imgs})
            if len(products) % 50 == 0:
                log.info(f"  [{brand}] {len(products)} 产品")

    page.close()
    log.info(f"[{brand}] 完成: {len(products)} 产品")
    return products


def crawl_generic(browser, brand, base_url, cat_paths, link_pattern, max_products=1000):
    """通用品牌爬虫"""
    log.info(f"[{brand}] 启动 (通用)")
    page = browser.new_page()
    products = []

    for path in cat_paths:
        if len(products) >= max_products: break
        url = urljoin(base_url, path)
        if not safe_goto(page, url, 30000): continue
        scroll_load(page, 5)

        links = page.evaluate(f"""() => {{
            return [...new Set(Array.from(document.querySelectorAll('a[href]'))
                .map(a => a.href)
                .filter(h => /{link_pattern}/.test(h)))];
        }}""")
        log.info(f"  [{brand}] {path}: {len(links)} 链接")

        for purl in links[:300]:
            if len(products) >= max_products: break
            pid = hashlib.md5(purl.encode()).hexdigest()[:12]
            imgs = extract_product_images(page, purl, brand, OUT_DIR, pid)
            if imgs:
                products.append({"id": pid, "url": purl, "brand": brand, "images": imgs})

    page.close()
    log.info(f"[{brand}] 完成: {len(products)} 产品")
    return products


def run_brand(brand_func, *args):
    """在独立 Playwright 实例中运行"""
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-gpu"])
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
            )
            result = brand_func(ctx, *args)
            browser.close()
            return result
    except Exception as e:
        log.error(f"品牌爬取异常: {e}")
        return []


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    log.info("=" * 60)
    log.info("多品牌并行爬取启动")
    log.info("=" * 60)

    all_products = []
    lock = threading.Lock()

    def run_and_collect(func, *args):
        result = run_brand(func, *args)
        with lock:
            all_products.extend(result)
        return len(result)

    # 并行启动所有品牌
    with ThreadPoolExecutor(max_workers=6, thread_name_prefix="brand") as pool:
        futures = {
            pool.submit(run_and_collect, crawl_nike, 2000): "nike",
            pool.submit(run_and_collect, crawl_adidas, 2000): "adidas",
            pool.submit(run_and_collect, crawl_lining, 1500): "lining",
            pool.submit(run_and_collect, crawl_zappos, 3000): "zappos",
            pool.submit(run_and_collect, crawl_generic,
                "anta", "https://www.anta.com", ["/category/shoes", "/category/apparel"],
                "product|item|detail|goods", 1000): "anta",
            pool.submit(run_and_collect, crawl_generic,
                "xtep", "https://www.xtep.com.cn", ["/category/shoes", "/category/clothing"],
                "product|item|detail|goods", 1000): "xtep",
        }

        for future in futures:
            brand = futures[future]
            try:
                count = future.result(timeout=1800)  # 30min timeout per brand
                log.info(f"✅ {brand}: {count} 产品")
            except Exception as e:
                log.error(f"❌ {brand}: {e}")

    # 保存 manifest
    total_images = sum(len(p.get("images", [])) for p in all_products)
    manifest = {
        "total_products": len(all_products),
        "total_images": total_images,
        "avg_views": round(total_images / max(len(all_products), 1), 1),
        "brands": {},
        "products": all_products,
    }
    for p in all_products:
        b = p["brand"]
        manifest["brands"][b] = manifest["brands"].get(b, 0) + 1

    with open(os.path.join(OUT_DIR, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    log.info(f"\n{'=' * 60}")
    log.info(f"爬取完成!")
    log.info(f"  产品: {len(all_products)}")
    log.info(f"  图片: {total_images}")
    log.info(f"  品牌: {json.dumps(manifest['brands'])}")
    log.info(f"  输出: {OUT_DIR}")


if __name__ == "__main__":
    main()
