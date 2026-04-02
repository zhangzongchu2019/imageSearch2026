#!/usr/bin/env python3
"""
L1 品类分类验证 — 电商产品图 (Bing Images)

通过 Bing 图片搜索获取各品类的真实电商产品图,
每类 15 张 × 18 类 = 270 张确定性验证。

Usage:
    python3 scripts/validate_l1_ecommerce.py [--device cuda]
"""

import argparse
import io
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import httpx
import numpy as np
import torch
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "services" / "inference-service"))
from app.taxonomy import L1_CATEGORIES, L1_CODES

# 每个品类的 Bing 搜索词 (英文产品搜索, 白底产品图优先)
CATEGORY_SEARCHES = {
    "服装类": ["dress product photo white background", "men shirt product photo",
               "winter jacket product", "jeans pants product white background",
               "women blouse top product"],
    "箱包类": ["leather handbag product photo", "backpack product white background",
               "luggage suitcase product", "wallet purse product photo",
               "crossbody bag product"],
    "鞋帽类": ["sneakers shoes product photo", "high heels women shoes product",
               "leather boots product", "baseball cap hat product",
               "running shoes product white background"],
    "珠宝首饰类": ["diamond ring jewelry product", "gold necklace jewelry product",
                "pearl earrings product photo", "silver bracelet jewelry",
                "jade pendant product"],
    "房地产类": ["house exterior real estate photo", "modern apartment building exterior",
               "luxury villa property photo", "commercial building exterior",
               "residential house front view"],
    "五金建材类": ["power drill tool product", "screwdriver set product photo",
                "plumbing fittings hardware", "ceramic tiles building material",
                "door lock handle hardware product"],
    "家具类": ["modern sofa product photo", "wooden dining table product",
             "office chair product white background", "bookshelf furniture product",
             "bed frame bedroom furniture"],
    "化妆品类": ["lipstick cosmetics product photo", "skincare serum bottle product",
              "perfume bottle product photo", "eyeshadow palette makeup product",
              "face cream moisturizer product"],
    "小家电类": ["hair dryer appliance product", "blender kitchen appliance product",
              "coffee machine product photo", "robot vacuum cleaner product",
              "electric kettle product"],
    "手机类": ["iphone smartphone product photo", "samsung galaxy phone product",
             "phone case product white background", "tablet ipad product photo",
             "wireless charger phone accessory"],
    "电脑类": ["laptop computer product photo", "desktop monitor product",
             "mechanical keyboard product", "wireless mouse product photo",
             "webcam computer accessory product"],
    "食品类": ["chocolate box product photo", "coffee beans package product",
             "wine bottle product photo", "cookies snack packaging product",
             "tea box product packaging"],
    "玩具类": ["lego building blocks toy product", "teddy bear plush toy product",
             "action figure toy product photo", "board game product photo",
             "remote control car toy product"],
    "运动户外类": ["dumbbell fitness equipment product", "yoga mat product photo",
               "camping tent outdoor product", "basketball sports product",
               "bicycle cycling product photo"],
    "汽车配件类": ["car tire wheel product", "car seat cover product photo",
               "dash cam car camera product", "car floor mat product",
               "car phone mount holder product"],
    "办公用品类": ["pen stationery product photo", "notebook journal product",
               "stapler office supply product", "desk organizer product photo",
               "file folder binder product"],
    "钟表类": ["luxury wristwatch product photo", "men analog watch product",
             "smartwatch product white background", "wall clock product photo",
             "pocket watch vintage product"],
    "眼镜类": ["sunglasses product photo", "optical glasses frame product",
             "reading glasses product white background", "designer sunglasses product",
             "blue light blocking glasses product"],
}


def search_bing_images(query, count=3, timeout=12):
    """从 Bing 图片搜索获取产品图 URL"""
    client = httpx.Client(timeout=timeout, follow_redirects=True, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    })

    try:
        url = f"https://www.bing.com/images/search?q={query}&first=1&count={count * 3}&qft=+filterui:photo-photo"
        resp = client.get(url)
        # 提取 murl (媒体 URL)
        urls = re.findall(r'murl&quot;:&quot;(https?://[^&]+\.(?:jpg|jpeg|png|webp))', resp.text)
        # 去重
        urls = list(dict.fromkeys(urls))
        return urls[:count]
    except Exception:
        return []
    finally:
        client.close()


def download_image(url, timeout=10):
    """下载单张图片"""
    client = httpx.Client(timeout=timeout, follow_redirects=True, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    })
    try:
        resp = client.get(url)
        if resp.status_code == 200 and len(resp.content) > 2000:
            img = Image.open(io.BytesIO(resp.content)).convert("RGB")
            return img
    except Exception:
        pass
    finally:
        client.close()
    return None


def collect_all_images():
    """收集所有品类的电商产品图"""
    all_images = []

    for category, queries in CATEGORY_SEARCHES.items():
        cat_count = 0
        for query in queries:
            urls = search_bing_images(query, count=3)
            for url in urls:
                img = download_image(url)
                if img is not None:
                    all_images.append({
                        "image": img,
                        "expected_l1": category,
                        "query": query,
                        "url": url,
                    })
                    cat_count += 1
                    if cat_count >= 15:
                        break
            if cat_count >= 15:
                break
            time.sleep(0.5)  # 避免太快

        print(f"  {category}: {cat_count} 张")

    return all_images


@torch.no_grad()
def classify_all(model, preprocess, text_features, l1_names, images, device):
    """批量分类"""
    results = []
    batch_size = 32

    for i in range(0, len(images), batch_size):
        batch = images[i:i + batch_size]
        tensors = [preprocess(item["image"]) for item in batch]
        batch_t = torch.stack(tensors).to(device)
        features = model.encode_image(batch_t)
        features = features / features.norm(dim=-1, keepdim=True)
        sims = (features.float() @ text_features.T)

        for k, item in enumerate(batch):
            idx = sims[k].argmax().item()
            conf = sims[k][idx].item()
            top3_vals, top3_idx = sims[k].topk(3)
            top3 = [(l1_names[ti], round(tv.item(), 4)) for ti, tv in zip(top3_idx, top3_vals)]

            results.append({
                "expected_l1": item["expected_l1"],
                "predicted_l1": l1_names[idx],
                "confidence": round(conf, 4),
                "correct": item["expected_l1"] == l1_names[idx],
                "in_top3": item["expected_l1"] in [t[0] for t in top3],
                "top3": top3,
                "query": item["query"],
                "url": item["url"],
            })

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--device", default="cuda")
    args = parser.parse_args()

    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] L1 电商产品图验证 (Bing Images)")
    print(f"  品类: {len(CATEGORY_SEARCHES)}, 设备: {args.device}\n")

    # 1. 收集图片
    print("[1/3] 从 Bing 搜索电商产品图...")
    t0 = time.time()
    images = collect_all_images()
    print(f"  总计: {len(images)} 张, 耗时 {time.time()-t0:.0f}s\n")

    if len(images) < 10:
        print("ERROR: Too few images!")
        return

    # 2. 加载模型
    print("[2/3] 加载 ViT-L-14...")
    import open_clip
    _orig = torch.load
    torch.load = lambda *a, **kw: _orig(*a, **{**kw, "weights_only": False})
    model, _, preprocess = open_clip.create_model_and_transforms(
        "ViT-L-14", pretrained="/data/imgsrch/models/ViT-L-14.pt"
    )
    torch.load = _orig
    model.eval().to(args.device)
    tokenizer = open_clip.get_tokenizer("ViT-L-14")

    l1_names = [L1_CATEGORIES[c]["name"] for c in L1_CODES]
    all_texts, counts = [], []
    for code in L1_CODES:
        prompts = L1_CATEGORIES[code]["prompts"]
        all_texts.extend(prompts)
        counts.append(len(prompts))
    tokens = tokenizer(all_texts).to(args.device)
    with torch.no_grad():
        feats = model.encode_text(tokens)
    feats = feats / feats.norm(dim=-1, keepdim=True)
    averaged = []
    idx = 0
    for c in counts:
        avg = feats[idx:idx + c].mean(dim=0)
        avg = avg / avg.norm()
        averaged.append(avg)
        idx += c
    text_features = torch.stack(averaged).float().to(args.device)
    print(f"  模型就绪\n")

    # 3. 分类
    print(f"[3/3] 分类 {len(images)} 张...")
    t1 = time.time()
    results = classify_all(model, preprocess, text_features, l1_names, images, args.device)
    elapsed = time.time() - t1
    print(f"  完成: {elapsed:.1f}s\n")

    # 4. 统计
    total = len(results)
    correct = sum(1 for r in results if r["correct"])
    top3_correct = sum(1 for r in results if r["in_top3"])

    print("=" * 75)
    print(f"  Top-1 准确率: {correct}/{total} = {correct/total*100:.1f}%")
    print(f"  Top-3 准确率: {top3_correct}/{total} = {top3_correct/total*100:.1f}%")
    print("=" * 75)

    cat_stats = defaultdict(lambda: {"total": 0, "correct": 0, "top3": 0, "wrong_as": defaultdict(int)})
    for r in results:
        cat = r["expected_l1"]
        cat_stats[cat]["total"] += 1
        if r["correct"]:
            cat_stats[cat]["correct"] += 1
        else:
            cat_stats[cat]["wrong_as"][r["predicted_l1"]] += 1
        if r["in_top3"]:
            cat_stats[cat]["top3"] += 1

    print(f"\n{'品类':<12} {'Top1':>4} {'Top3':>4} {'总':>3} {'Top1%':>6} {'Top3%':>6}  主要误分")
    print("-" * 70)
    for cat in CATEGORY_SEARCHES:
        s = cat_stats[cat]
        t = s["total"]
        a1 = s["correct"] / t * 100 if t else 0
        a3 = s["top3"] / t * 100 if t else 0
        wrong = ""
        if s["wrong_as"]:
            top_wrong = sorted(s["wrong_as"].items(), key=lambda x: -x[1])[:2]
            wrong = ", ".join(f"{w}({n})" for w, n in top_wrong)
        mark = "✓" if a1 >= 80 else ("△" if a1 >= 60 else "✗")
        print(f"{mark} {cat:<11} {s['correct']:>4} {s['top3']:>4} {t:>3} {a1:>5.0f}% {a3:>5.0f}%  {wrong}")

    # 错误详情
    wrong_results = [r for r in results if not r["correct"]]
    if wrong_results:
        print(f"\n错误详情 ({len(wrong_results)} 条):")
        for r in wrong_results[:30]:
            print(f"  期望={r['expected_l1']:<10} 预测={r['predicted_l1']:<10} "
                  f"conf={r['confidence']:.3f} query={r['query'][:35]}")

    # 保存
    output_dir = "/data/imgsrch/task_logs"
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"l1_ecommerce_{ts}.json")
    with open(output_file, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "source": "bing_images_ecommerce",
            "total": total, "correct": correct, "top3_correct": top3_correct,
            "accuracy": round(correct / total * 100, 2),
            "top3_accuracy": round(top3_correct / total * 100, 2),
            "per_category": {cat: {
                "correct": s["correct"], "top3": s["top3"], "total": s["total"],
                "top1_pct": round(s["correct"] / s["total"] * 100, 1) if s["total"] else 0,
            } for cat, s in cat_stats.items()},
            "errors": [r for r in results if not r["correct"]],
        }, f, ensure_ascii=False, indent=2)
    print(f"\n结果保存: {output_file}")


if __name__ == "__main__":
    main()
