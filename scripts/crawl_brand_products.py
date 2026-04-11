#!/usr/bin/env python3
"""
从品牌官网爬取同一产品多视角图片
- 目标: 1万+ 产品, 平均每产品 2.5 张 = 2.5万张
- 限速: 5 req/s
- 输出: /data/imgsrch/brand_products/
  ├── manifest.json (产品列表 + 视角映射)
  ├── nike/
  │   ├── product_001_view1.jpg
  │   ├── product_001_view2.jpg
  │   └── ...

在 gpu_worker 容器内运行 (有 Chrome)
"""
import json
import os
import time
import hashlib
import logging
import asyncio
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s",
    handlers=[logging.FileHandler("/data/imgsrch/task_logs/crawl_brands.log"),
              logging.StreamHandler()])
log = logging.getLogger("crawl")

OUT_DIR = "/data/imgsrch/brand_products"
RATE_LIMIT = 0.2  # 5 req/s = 0.2s between requests


def crawl_nike(page, max_products=2000):
    """Nike 中国官网: 通过 API 获取产品列表和图片"""
    log.info("[Nike] 开始...")
    products = []
    base_api = "https://api.nike.com/cic/browse/v2"

    # Nike 有多个品类
    categories = [
        ("men-shoes", "男子鞋类"),
        ("women-shoes", "女子鞋类"),
        ("men-clothing", "男子服装"),
        ("women-clothing", "女子服装"),
    ]

    for cat_slug, cat_name in categories:
        if len(products) >= max_products:
            break
        log.info(f"  [Nike] 品类: {cat_name}")

        # 直接访问列表页获取产品
        url = f"https://www.nike.com.cn/w/{cat_slug}"
        try:
            page.goto(url, timeout=30000, wait_until="networkidle")
            time.sleep(2)

            # 滚动加载更多
            for _ in range(5):
                page.evaluate("window.scrollBy(0, 2000)")
                time.sleep(1)

            # 提取产品卡片
            cards = page.query_selector_all("a[data-testid='product-card']") or \
                    page.query_selector_all(".product-card") or \
                    page.query_selector_all("[class*='product']  a[href*='/t/']")

            if not cards:
                # 备用: 提取所有产品链接
                cards = page.query_selector_all("a[href*='/t/']")

            log.info(f"    找到 {len(cards)} 个产品链接")

            for card in cards[:500]:  # 每品类最多 500
                if len(products) >= max_products:
                    break
                try:
                    href = card.get_attribute("href")
                    if href and "/t/" in href:
                        product_url = urljoin("https://www.nike.com.cn", href)
                        products.append({"brand": "nike", "url": product_url, "category": cat_name})
                except:
                    pass
                time.sleep(RATE_LIMIT)
        except Exception as e:
            log.warning(f"    Nike {cat_name} 失败: {str(e)[:80]}")

    log.info(f"[Nike] 收集 {len(products)} 个产品链接")
    return products


def crawl_adidas(page, max_products=2000):
    """Adidas 中国: 列表页提取产品"""
    log.info("[Adidas] 开始...")
    products = []

    categories = [
        ("https://www.adidas.com.cn/men-shoes", "男鞋"),
        ("https://www.adidas.com.cn/women-shoes", "女鞋"),
        ("https://www.adidas.com.cn/men-clothing", "男装"),
    ]

    for url, cat in categories:
        if len(products) >= max_products:
            break
        try:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            time.sleep(3)
            for _ in range(5):
                page.evaluate("window.scrollBy(0, 2000)")
                time.sleep(1)

            links = page.query_selector_all("a[href*='/model/']") or \
                    page.query_selector_all("a[href*='/product/']") or \
                    page.query_selector_all("[class*='product'] a")

            log.info(f"  [Adidas] {cat}: {len(links)} 链接")
            for link in links[:500]:
                if len(products) >= max_products:
                    break
                try:
                    href = link.get_attribute("href")
                    if href:
                        products.append({"brand": "adidas", "url": urljoin(url, href), "category": cat})
                except:
                    pass
                time.sleep(RATE_LIMIT)
        except Exception as e:
            log.warning(f"  Adidas {cat}: {str(e)[:80]}")

    log.info(f"[Adidas] 收集 {len(products)} 个产品链接")
    return products


def crawl_generic_brand(page, brand_name, base_url, list_paths, max_products=1000):
    """通用品牌爬虫: 列表页 → 产品链接"""
    log.info(f"[{brand_name}] 开始...")
    products = []

    for path in list_paths:
        if len(products) >= max_products:
            break
        url = urljoin(base_url, path)
        try:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            time.sleep(3)
            for _ in range(3):
                page.evaluate("window.scrollBy(0, 2000)")
                time.sleep(1)

            # 通用提取: 所有含产品关键词的链接
            all_links = page.query_selector_all("a[href]")
            for link in all_links:
                href = link.get_attribute("href") or ""
                if any(kw in href.lower() for kw in ["/product", "/item", "/detail", "/goods", "/p/"]):
                    products.append({"brand": brand_name, "url": urljoin(base_url, href), "category": path})
                    if len(products) >= max_products:
                        break
            time.sleep(RATE_LIMIT * 5)
            log.info(f"  [{brand_name}] {path}: +{len(products)} 产品")
        except Exception as e:
            log.warning(f"  [{brand_name}] {path}: {str(e)[:80]}")

    log.info(f"[{brand_name}] 收集 {len(products)} 个产品链接")
    return products


def download_product_images(page, product, out_dir):
    """进入产品详情页, 提取所有视角图片"""
    url = product["url"]
    brand = product["brand"]

    try:
        page.goto(url, timeout=20000, wait_until="domcontentloaded")
        time.sleep(2)

        # 提取所有图片 (通常产品页有 3-8 张)
        images = page.query_selector_all("img[src]")
        img_urls = []
        for img in images:
            src = img.get_attribute("src") or ""
            # 过滤: 只要产品图 (通常尺寸 > 200px 的)
            width = img.get_attribute("width") or ""
            if src and (src.startswith("http") or src.startswith("//")):
                if "logo" not in src.lower() and "icon" not in src.lower() and "banner" not in src.lower():
                    if src.startswith("//"):
                        src = "https:" + src
                    img_urls.append(src)

        # 去重
        img_urls = list(dict.fromkeys(img_urls))

        # 下载
        product_id = hashlib.md5(url.encode()).hexdigest()[:12]
        downloaded = []
        brand_dir = os.path.join(out_dir, brand)
        os.makedirs(brand_dir, exist_ok=True)

        for i, img_url in enumerate(img_urls[:8]):  # 最多 8 张
            try:
                # 用 page.request 下载 (带 cookie/referer)
                resp = page.request.get(img_url, timeout=10000)
                if resp.status == 200 and len(resp.body()) > 5000:
                    fname = f"{product_id}_view{i+1}.jpg"
                    fpath = os.path.join(brand_dir, fname)
                    with open(fpath, "wb") as f:
                        f.write(resp.body())
                    downloaded.append({"file": fname, "url": img_url, "view": i+1})
            except:
                pass
            time.sleep(RATE_LIMIT)

        return {"product_id": product_id, "url": url, "brand": brand,
                "images": downloaded, "total_views": len(downloaded)}
    except Exception as e:
        return {"product_id": "", "url": url, "brand": brand, "error": str(e)[:100],
                "images": [], "total_views": 0}


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs("/data/imgsrch/task_logs", exist_ok=True)

    log.info("=" * 60)
    log.info("品牌产品多视角图片爬取")
    log.info(f"输出: {OUT_DIR}")
    log.info(f"限速: {1/RATE_LIMIT:.0f} req/s")
    log.info("=" * 60)

    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()

        # === Phase 1: 收集产品链接 ===
        all_products = []

        # Nike
        try:
            nike = crawl_nike(page, max_products=2000)
            all_products.extend(nike)
        except Exception as e:
            log.error(f"Nike 爬取失败: {e}")

        # Adidas
        try:
            adidas = crawl_adidas(page, max_products=2000)
            all_products.extend(adidas)
        except Exception as e:
            log.error(f"Adidas 爬取失败: {e}")

        # 其他品牌 (通用爬虫)
        brands = [
            ("lining", "https://www.lining.com", ["/men/shoes", "/women/shoes", "/men/apparel"]),
            ("anta", "https://www.anta.com", ["/category/shoes", "/category/clothing"]),
            ("puma", "https://cn.puma.com", ["/men/shoes", "/women/shoes"]),
            ("newbalance", "https://www.newbalance.com.cn", ["/men/shoes", "/women/shoes"]),
        ]
        for bname, burl, bpaths in brands:
            try:
                prods = crawl_generic_brand(page, bname, burl, bpaths, max_products=1000)
                all_products.extend(prods)
            except Exception as e:
                log.error(f"{bname} 爬取失败: {e}")

        log.info(f"\n=== 共收集 {len(all_products)} 个产品链接 ===")

        # 去重
        seen = set()
        unique = []
        for p_item in all_products:
            if p_item["url"] not in seen:
                seen.add(p_item["url"])
                unique.append(p_item)
        all_products = unique
        log.info(f"去重后: {len(all_products)}")

        # === Phase 2: 下载产品图片 ===
        manifest = {"products": [], "total_products": 0, "total_images": 0}
        total_images = 0

        for i, product in enumerate(all_products):
            result = download_product_images(page, product, OUT_DIR)
            if result["total_views"] > 0:
                manifest["products"].append(result)
                total_images += result["total_views"]

            if (i + 1) % 50 == 0:
                log.info(f"  [{i+1}/{len(all_products)}] 产品={len(manifest['products'])}, 图片={total_images}")

            # 每 500 个产品保存一次 manifest
            if (i + 1) % 500 == 0:
                manifest["total_products"] = len(manifest["products"])
                manifest["total_images"] = total_images
                with open(os.path.join(OUT_DIR, "manifest.json"), "w") as f:
                    json.dump(manifest, f, indent=2, ensure_ascii=False)

        browser.close()

    # 最终保存
    manifest["total_products"] = len(manifest["products"])
    manifest["total_images"] = total_images
    with open(os.path.join(OUT_DIR, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    log.info(f"\n=== 爬取完成 ===")
    log.info(f"产品数: {manifest['total_products']}")
    log.info(f"图片数: {manifest['total_images']}")
    log.info(f"平均视角: {manifest['total_images']/max(manifest['total_products'],1):.1f}")
    log.info(f"输出: {OUT_DIR}")


if __name__ == "__main__":
    main()
