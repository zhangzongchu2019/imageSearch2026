#!/usr/bin/env python3
"""
构建 6 场景评测查询集
基础: 从 /data/test-images/ 随机抽 1000 张
变体:
  1. 原图 (近重复基线)
  2. JPEG 50% 压缩
  3. 中心裁剪 70%
  4. 中心裁剪 30% (重度截图)
  5. UI 叠加 (导航栏 + footer)
  6. 嵌入大图 (1920x1080 白底中心)

输出:
  /data/imgsrch/eval_queries/
    1_original/      *.jpg (1000 张)
    2_jpeg50/
    3_crop70/
    4_crop30/
    5_ui_overlay/
    6_embed_large/
    ground_truth.json (映射: query_pk → original_pk)
"""
import os, random, json
from PIL import Image, ImageDraw

SRC_DIR = "/data/test-images"
OUT_DIR = "/data/imgsrch/eval_queries"
NUM_QUERIES = 1000
SEED = 42


def load_image_files():
    files = sorted(os.listdir(SRC_DIR))
    return [f for f in files if f.endswith(".jpg")]


def make_jpeg_compressed(img, quality=50):
    """JPEG 重压缩"""
    from io import BytesIO
    buf = BytesIO()
    img.save(buf, "JPEG", quality=quality)
    buf.seek(0)
    return Image.open(buf).convert("RGB")


def make_center_crop(img, ratio):
    """中心裁剪指定比例"""
    w, h = img.size
    new_w = int(w * ratio)
    new_h = int(h * ratio)
    left = (w - new_w) // 2
    top = (h - new_h) // 2
    return img.crop((left, top, left + new_w, top + new_h))


def make_ui_overlay(img):
    """模拟手机截图: 顶部状态栏 + 底部 navbar"""
    w, h = img.size
    new_h = h + 80
    canvas = Image.new("RGB", (w, new_h), color=(255, 255, 255))
    canvas.paste(img, (0, 40))

    # 顶部状态栏 (黑色)
    draw = ImageDraw.Draw(canvas)
    draw.rectangle([(0, 0), (w, 40)], fill=(0, 0, 0))
    # 假信号图标
    draw.text((10, 12), "9:41", fill=(255, 255, 255))
    draw.text((w - 60, 12), "100%", fill=(255, 255, 255))

    # 底部 navbar (灰色)
    draw.rectangle([(0, new_h - 40), (w, new_h)], fill=(200, 200, 200))
    draw.text((w // 2 - 30, new_h - 30), "首页", fill=(50, 50, 50))

    return canvas


def make_embed_large(img):
    """商品图嵌入 1280x960 白底大图"""
    canvas = Image.new("RGB", (1280, 960), color=(245, 245, 245))
    # 缩放原图到 400x400
    small = img.resize((400, 400), Image.BICUBIC)
    # 居中嵌入
    paste_x = (1280 - 400) // 2
    paste_y = (960 - 400) // 2
    canvas.paste(small, (paste_x, paste_y))

    # 加一些"网页"元素
    draw = ImageDraw.Draw(canvas)
    draw.rectangle([(50, 50), (1230, 100)], fill=(70, 130, 180))  # header
    draw.text((100, 65), "ShopXYZ - Product Detail", fill=(255, 255, 255))
    draw.text((100, 150), "¥299.00", fill=(220, 20, 60))
    draw.text((100, 850), "Customer Reviews ★★★★☆", fill=(80, 80, 80))

    return canvas


def main():
    random.seed(SEED)
    print(f"扫描 {SRC_DIR}...")
    all_files = load_image_files()
    print(f"  总图片: {len(all_files):,}")

    selected = random.sample(all_files, NUM_QUERIES)
    print(f"  选中: {NUM_QUERIES}")

    # 创建输出目录
    scenarios = [
        ("1_original",     lambda img: img),
        ("2_jpeg50",       lambda img: make_jpeg_compressed(img, 50)),
        ("3_crop70",       lambda img: make_center_crop(img, 0.7)),
        ("4_crop30",       lambda img: make_center_crop(img, 0.3)),
        ("5_ui_overlay",   make_ui_overlay),
        ("6_embed_large",  make_embed_large),
    ]

    for name, _ in scenarios:
        os.makedirs(os.path.join(OUT_DIR, name), exist_ok=True)

    ground_truth = {}  # query_pk → original_pk
    fail_count = 0

    for i, fname in enumerate(selected):
        original_pk = fname.replace(".jpg", "")
        try:
            src_img = Image.open(os.path.join(SRC_DIR, fname)).convert("RGB")
        except Exception as e:
            print(f"  跳过 {fname}: {e}")
            fail_count += 1
            continue

        for name, fn in scenarios:
            try:
                variant = fn(src_img.copy())
                out_path = os.path.join(OUT_DIR, name, fname)
                variant.save(out_path, "JPEG", quality=90)
                ground_truth[f"{name}/{original_pk}"] = original_pk
            except Exception as e:
                print(f"  {name} 失败 {fname}: {e}")

        if (i + 1) % 100 == 0:
            print(f"  {i+1}/{NUM_QUERIES}")

    # 保存 ground truth
    with open(os.path.join(OUT_DIR, "ground_truth.json"), "w") as f:
        json.dump({
            "scenarios": [s[0] for s in scenarios],
            "num_queries": NUM_QUERIES - fail_count,
            "mapping": ground_truth,
            "seed": SEED,
        }, f, indent=2)

    print(f"\n✅ 完成: {NUM_QUERIES - fail_count} 张 × 6 场景 = {(NUM_QUERIES - fail_count) * 6} 个查询")
    print(f"   输出: {OUT_DIR}")
    for name, _ in scenarios:
        cnt = len(os.listdir(os.path.join(OUT_DIR, name)))
        print(f"     {name}: {cnt}")


if __name__ == "__main__":
    main()
