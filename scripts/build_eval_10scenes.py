#!/usr/bin/env python3
"""
构建 10 场景评测集 (对齐用户验收标准)

场景清单:
  1. original         — 原图 (验证 Top1 = 原图)
  2. jpeg_compress    — JPEG 20% 质量 + 多次转存模拟
  3. crop_70          — 中心裁剪 70% (轻度)
  4. aspect_change    — 宽高比变化 ±10% (轻度形变)
  5. ui_overlay       — UI叠加/涂鸦 (≤30% 面积)
  6. crop_30          — 裁剪 30% (重度, 随机位置)
  7. embed_in_large   — 图中图: 原图裁剪部分嵌入大图
  8. reverse_embed    — 反图中图: 原图被包裹在更大图中
  9. graffiti_heavy   — 重度涂鸦 (30% 面积, 彩色线条)
  10. resize_scale    — 分辨率缩放 (同比例 0.5x~2x)

每场景 1000 张, 输出到 /data/imgsrch/eval_10scenes/
"""
import os, random, json
import numpy as np
from PIL import Image, ImageDraw, ImageFilter

SRC_DIR = "/data/test-images"
OUT_DIR = "/data/imgsrch/eval_10scenes"
NUM = 1000
SEED = 42


def make_jpeg_heavy(img):
    """多次 JPEG 压缩模拟社交转发"""
    from io import BytesIO
    for q in [30, 25, 20]:
        buf = BytesIO()
        img.save(buf, "JPEG", quality=q)
        buf.seek(0)
        img = Image.open(buf).convert("RGB")
    return img


def make_crop(img, ratio, position="random"):
    """裁剪指定比例, 支持随机位置"""
    w, h = img.size
    # 面积比 = ratio, 线性缩放 = sqrt(ratio)
    scale = ratio ** 0.5
    new_w = int(w * scale)
    new_h = int(h * scale)
    if position == "center":
        left = (w - new_w) // 2
        top = (h - new_h) // 2
    elif position == "random":
        left = random.randint(0, max(0, w - new_w))
        top = random.randint(0, max(0, h - new_h))
    else:
        left = top = 0
    return img.crop((left, top, left + new_w, top + new_h))


def make_aspect_change(img):
    """宽高比变化 ±10%"""
    w, h = img.size
    # 随机拉伸/压缩一个方向
    factor = random.uniform(0.9, 1.1)
    if random.random() > 0.5:
        new_w = int(w * factor)
        new_h = h
    else:
        new_w = w
        new_h = int(h * factor)
    return img.resize((new_w, new_h), Image.BICUBIC)


def make_ui_overlay(img):
    """UI 叠加 + 轻度涂鸦 (≤30% 面积)"""
    w, h = img.size
    canvas = img.copy()
    draw = ImageDraw.Draw(canvas)
    # 顶部状态栏
    bar_h = max(20, h // 15)
    draw.rectangle([(0, 0), (w, bar_h)], fill=(30, 30, 30))
    draw.text((10, bar_h // 4), "9:41 AM", fill=(255, 255, 255))
    draw.text((w - 60, bar_h // 4), "100%", fill=(255, 255, 255))
    # 底部导航
    draw.rectangle([(0, h - bar_h), (w, h)], fill=(240, 240, 240))
    # 小logo / 水印
    draw.text((w // 2 - 30, h // 2 - 10), "SAMPLE", fill=(200, 200, 200, 128))
    return canvas


def make_embed_in_large(img):
    """图中图: 原图裁剪 50% 嵌入 1280x960 白底"""
    # 先裁剪原图的 50%
    crop = make_crop(img, 0.5, "center")
    # 嵌入大图
    canvas = Image.new("RGB", (1280, 960), (245, 245, 245))
    # 缩放裁剪到 300x300
    small = crop.resize((300, 300), Image.BICUBIC)
    px = random.randint(100, 880)
    py = random.randint(100, 560)
    canvas.paste(small, (px, py))
    # 加网页元素
    draw = ImageDraw.Draw(canvas)
    draw.rectangle([(30, 30), (1250, 80)], fill=(66, 133, 244))
    draw.text((50, 45), "ProductPage - Detail View", fill=(255, 255, 255))
    draw.text((50, 700), "Price: $99.00", fill=(220, 20, 60))
    return canvas


def make_reverse_embed(img):
    """反图中图: 原图被包裹在更大图中 (模拟网页截图包含商品图)"""
    w, h = img.size
    # 创建比原图大 2-3 倍的画布
    factor = random.uniform(2.0, 3.0)
    cw, ch = int(w * factor), int(h * factor)
    canvas = Image.new("RGB", (cw, ch), (250, 250, 250))
    # 原图放在随机位置
    px = random.randint(50, max(51, cw - w - 50))
    py = random.randint(80, max(81, ch - h - 50))
    canvas.paste(img, (px, py))
    # 加网页 UI
    draw = ImageDraw.Draw(canvas)
    draw.rectangle([(0, 0), (cw, 60)], fill=(51, 51, 51))
    draw.text((20, 18), "ShopXYZ.com - Search Results", fill=(255, 255, 255))
    draw.rectangle([(20, 70), (200, 90)], fill=(200, 200, 200))
    draw.text((30, 72), "Categories", fill=(80, 80, 80))
    # 底部
    draw.text((20, ch - 40), "Showing 1 of 24 results", fill=(120, 120, 120))
    return canvas


def make_graffiti_heavy(img):
    """重度涂鸦 (~30% 面积, 彩色线条+方块)"""
    canvas = img.copy()
    draw = ImageDraw.Draw(canvas)
    w, h = canvas.size
    # 随机彩色线条
    for _ in range(15):
        color = (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
        x1, y1 = random.randint(0, w), random.randint(0, h)
        x2, y2 = random.randint(0, w), random.randint(0, h)
        draw.line([(x1, y1), (x2, y2)], fill=color, width=random.randint(3, 8))
    # 随机小方块
    for _ in range(5):
        color = (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
        x = random.randint(0, w - 60)
        y = random.randint(0, h - 40)
        draw.rectangle([(x, y), (x + random.randint(30, 80), y + random.randint(20, 50))], fill=color)
    # 文字涂鸦
    draw.text((w // 4, h // 3), "SALE!", fill=(255, 0, 0))
    draw.text((w // 2, h // 2), "50% OFF", fill=(0, 255, 0))
    return canvas


def make_resize_scale(img):
    """同比例分辨率缩放 (0.5x 或 2x)"""
    w, h = img.size
    scale = random.choice([0.5, 0.75, 1.5, 2.0])
    new_w, new_h = int(w * scale), int(h * scale)
    resized = img.resize((new_w, new_h), Image.BICUBIC)
    # 再转回来模拟质量损失
    if scale < 1:
        resized = resized.resize((w, h), Image.BICUBIC)
    return resized


SCENARIOS = [
    ("1_original",       lambda img: img),
    ("2_jpeg_compress",  make_jpeg_heavy),
    ("3_crop_70",        lambda img: make_crop(img, 0.7, "center")),
    ("4_aspect_change",  make_aspect_change),
    ("5_ui_overlay",     make_ui_overlay),
    ("6_crop_30",        lambda img: make_crop(img, 0.3, "random")),
    ("7_embed_in_large", make_embed_in_large),
    ("8_reverse_embed",  make_reverse_embed),
    ("9_graffiti_heavy", make_graffiti_heavy),
    ("10_resize_scale",  make_resize_scale),
]


def get_milvus_pks():
    """获取 Milvus h4_main 中已有的 pk 集合"""
    import glob, json as jjson
    pks = set()
    for f in glob.glob("/data/imgsrch/test_h4/dual_gpu*.jsonl"):
        with open(f) as fh:
            for line in fh:
                pks.add(jjson.loads(line)["image_pk"])
    return pks


def main():
    random.seed(SEED)
    all_files = sorted([f for f in os.listdir(SRC_DIR) if f.endswith(".jpg")])
    # 只从 Milvus 中已有的图片采样 (避免评测图不在库中)
    milvus_pks = get_milvus_pks()
    print(f"Milvus h4_main 已有: {len(milvus_pks):,}")
    eligible = [f for f in all_files if f.replace(".jpg", "") in milvus_pks]
    selected = random.sample(eligible, min(NUM, len(eligible)))
    print(f"源: {len(all_files):,}, 在 Milvus 中: {len(eligible):,}, 选中: {len(selected)}")

    for name, _ in SCENARIOS:
        os.makedirs(os.path.join(OUT_DIR, name), exist_ok=True)

    gt = {"scenarios": [s[0] for s in SCENARIOS], "num": NUM, "mapping": {}}
    fail = 0

    for i, fname in enumerate(selected):
        pk = fname.replace(".jpg", "")
        try:
            src = Image.open(os.path.join(SRC_DIR, fname)).convert("RGB")
        except:
            fail += 1; continue

        for name, fn in SCENARIOS:
            try:
                random.seed(SEED + hash(pk + name) % 10000)
                variant = fn(src.copy())
                variant.save(os.path.join(OUT_DIR, name, fname), "JPEG", quality=90)
                gt["mapping"][f"{name}/{pk}"] = pk
            except Exception as e:
                if i < 3: print(f"  {name} {fname}: {e}")

        if (i + 1) % 200 == 0:
            print(f"  {i+1}/{NUM}")

    with open(os.path.join(OUT_DIR, "ground_truth.json"), "w") as f:
        json.dump(gt, f, indent=2)

    print(f"\n✅ 完成: {NUM - fail} 张 × {len(SCENARIOS)} 场景 = {(NUM-fail)*len(SCENARIOS)} 查询")
    for name, _ in SCENARIOS:
        cnt = len(os.listdir(os.path.join(OUT_DIR, name)))
        print(f"  {name}: {cnt}")


if __name__ == "__main__":
    main()
