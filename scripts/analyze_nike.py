from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
    page = browser.new_page()
    
    page.goto("https://www.nike.com.cn/w/men-shoes", timeout=30000, wait_until="networkidle")
    
    # 找所有 a 标签的 href
    links = page.evaluate("""() => {
        return Array.from(document.querySelectorAll('a[href]')).map(a => a.href).filter(h => h.includes('/t/') || h.includes('/product'))
    }""")
    
    print(f"产品链接数: {len(links)}")
    for l in links[:10]:
        print(f"  {l}")
    
    # 找图片
    imgs = page.evaluate("""() => {
        return Array.from(document.querySelectorAll('img[src]')).map(i => ({src: i.src, w: i.width, h: i.height})).filter(i => i.w > 100)
    }""")
    print(f"\n大图数: {len(imgs)}")
    for i in imgs[:10]:
        print(f"  {i['w']}x{i['h']} {i['src'][:80]}")
    
    browser.close()
