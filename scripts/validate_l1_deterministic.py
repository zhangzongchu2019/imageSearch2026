#!/usr/bin/env python3
"""
L1 品类分类确定性验证 — 固定产品图

使用 LoremFlickr lock 参数获取固定产品图,
每个品类 15 张 × 18 类 = 270 张, 结果完全可复现。

Usage:
    python3 scripts/validate_l1_deterministic.py [--device cuda]
"""

import argparse
import io
import json
import os
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
from app.taxonomy import L1_CATEGORIES, L1_CODES, L1_CODE_TO_NAME

# ---------------------------------------------------------------------------
# 每个品类的搜索关键词 + lock seeds (确保图片固定)
# LoremFlickr 会从 Flickr 搜索关键词相关的 CC 授权图片
# ---------------------------------------------------------------------------

CATEGORY_QUERIES = {
    "服装类": {
        "keywords": ["dress,clothing", "shirt,apparel", "jacket,coat",
                     "jeans,pants", "sweater,knitwear", "t-shirt,casual",
                     "skirt,fashion", "blouse,women+clothing", "suit,formal+wear",
                     "hoodie,sportswear", "vest,outerwear", "scarf,fashion+accessory",
                     "polo+shirt", "cardigan,knit", "uniform,work+wear"],
    },
    "箱包类": {
        "keywords": ["handbag,purse", "backpack,bag", "suitcase,luggage",
                     "wallet,leather", "crossbody+bag", "tote+bag,shopping",
                     "briefcase,business+bag", "clutch+bag", "duffel+bag,travel",
                     "messenger+bag", "fanny+pack,belt+bag", "cosmetic+bag",
                     "laptop+bag", "school+bag", "leather+bag,luxury"],
    },
    "鞋帽类": {
        "keywords": ["sneakers,shoes", "high+heels,pumps", "boots,footwear",
                     "sandals,summer+shoes", "loafer,leather+shoes", "running+shoes,athletic",
                     "baseball+cap,hat", "beanie,knit+hat", "oxford+shoes,formal",
                     "slippers,indoor+shoes", "canvas+shoes", "platform+shoes",
                     "cowboy+boots", "espadrilles", "bucket+hat"],
    },
    "珠宝首饰类": {
        "keywords": ["diamond+ring,jewelry", "gold+necklace", "pearl+earrings",
                     "silver+bracelet", "gemstone,ruby", "jade+pendant",
                     "brooch,pin", "engagement+ring", "gold+chain,jewelry",
                     "crystal+jewelry", "charm+bracelet", "hoop+earrings",
                     "tiara,crown", "cufflinks,men+jewelry", "anklet,jewelry"],
    },
    "房地产类": {
        "keywords": ["house,exterior", "apartment+building", "villa,mansion",
                     "modern+architecture", "residential+building", "skyscraper,office+building",
                     "real+estate,property", "townhouse", "condominium,condo",
                     "suburban+house", "commercial+building", "house+facade",
                     "cottage,country+house", "penthouse,luxury", "duplex,house"],
    },
    "五金建材类": {
        "keywords": ["screws,bolts,hardware", "power+drill,tool", "wrench,pliers",
                     "hammer,nail", "pipe+fittings,plumbing", "saw,carpentry",
                     "paint+bucket", "tiles,ceramic", "electrical+socket,wiring",
                     "door+handle,lock", "measuring+tape,ruler", "sandpaper,abrasive",
                     "spirit+level", "workbench,workshop", "toolbox,tools"],
    },
    "家具类": {
        "keywords": ["sofa,couch", "dining+table,wooden", "office+chair",
                     "bookshelf,shelving", "bed+frame,bedroom", "cabinet,wardrobe",
                     "desk,workspace", "armchair,recliner", "coffee+table",
                     "dresser,chest", "bar+stool", "nightstand,bedside",
                     "tv+stand,media+console", "shoe+rack", "dining+chair"],
    },
    "化妆品类": {
        "keywords": ["lipstick,makeup", "perfume+bottle", "face+cream,skincare",
                     "eyeshadow+palette", "foundation,cosmetics", "mascara,eyeliner",
                     "nail+polish", "blush,powder", "serum+bottle,skincare",
                     "makeup+brush", "sunscreen,lotion", "cleanser,face+wash",
                     "toner,skincare", "bb+cream", "lip+gloss"],
    },
    "小家电类": {
        "keywords": ["hair+dryer,blow+dryer", "blender,kitchen+appliance", "coffee+machine,espresso",
                     "toaster+oven", "electric+kettle", "vacuum+cleaner,robot",
                     "air+fryer", "iron,steam+iron", "electric+fan",
                     "rice+cooker", "juicer,electric", "humidifier",
                     "food+processor", "electric+shaver", "air+purifier"],
    },
    "手机类": {
        "keywords": ["smartphone,iphone", "android+phone,samsung", "mobile+phone,cellphone",
                     "phone+case,cover", "screen+protector", "phone+charger,cable",
                     "tablet,ipad", "phone+stand,holder", "wireless+earbuds",
                     "power+bank", "phone+grip", "sim+card+tray",
                     "phone+lens", "stylus+pen,tablet", "phone+ring+holder"],
    },
    "电脑类": {
        "keywords": ["laptop,notebook+computer", "desktop+computer,PC", "computer+monitor,screen",
                     "mechanical+keyboard", "computer+mouse,wireless", "webcam,camera",
                     "USB+drive,flash+drive", "external+hard+drive", "computer+headset",
                     "graphics+card,GPU", "RAM,memory+module", "motherboard,circuit",
                     "laptop+stand", "router,wifi", "SSD,storage"],
    },
    "食品类": {
        "keywords": ["chocolate,candy", "coffee+beans,bag", "wine+bottle",
                     "cookie,biscuit", "tea+box,tea+bag", "honey+jar",
                     "olive+oil,bottle", "cheese,dairy", "cereal+box",
                     "dried+fruit,nuts", "pasta,noodles", "sauce+bottle,condiment",
                     "protein+bar,snack", "jam,preserve", "spice+jar"],
    },
    "玩具类": {
        "keywords": ["lego,building+blocks", "teddy+bear,stuffed+animal", "action+figure,toy",
                     "puzzle,jigsaw", "toy+car,model+car", "board+game",
                     "doll,barbie", "rc+car,remote+control", "play+dough,clay",
                     "toy+train", "yo+yo,spinning+top", "building+blocks,kids",
                     "robot+toy", "toy+dinosaur", "fidget+spinner"],
    },
    "运动户外类": {
        "keywords": ["dumbbell,weight", "yoga+mat,fitness", "basketball,sports",
                     "bicycle,cycling", "camping+tent", "sleeping+bag,outdoor",
                     "tennis+racket", "football,soccer+ball", "ski+goggles,ski",
                     "fishing+rod,reel", "hiking+boots", "swimming+goggles",
                     "skateboard", "jump+rope,fitness", "boxing+gloves"],
    },
    "汽车配件类": {
        "keywords": ["car+tire,wheel", "car+seat+cover", "dash+cam,car+camera",
                     "car+floor+mat", "steering+wheel+cover", "car+charger,adapter",
                     "headlight,car+light", "car+wax,polish", "license+plate+frame",
                     "car+air+freshener", "windshield+wiper", "car+phone+mount",
                     "trunk+organizer", "car+vacuum", "side+mirror,car"],
    },
    "办公用品类": {
        "keywords": ["ballpoint+pen,stationery", "notebook,journal", "stapler,office",
                     "paper+clips,binder", "scissors,office+supply", "highlighter+pen",
                     "desk+organizer", "file+folder,document", "sticky+notes,post-it",
                     "tape+dispenser", "pencil+case", "whiteboard+marker",
                     "letter+opener", "desk+lamp,office", "rubber+stamp"],
    },
    "钟表类": {
        "keywords": ["wristwatch,luxury+watch", "analog+watch,classic", "digital+watch,sport",
                     "pocket+watch,vintage", "smartwatch,apple+watch", "wall+clock",
                     "alarm+clock,bedside", "watch+band,strap", "chronograph,diving+watch",
                     "skeleton+watch,mechanical", "dress+watch,formal", "field+watch,military",
                     "watch+box,case", "cuckoo+clock", "grandfather+clock"],
    },
    "眼镜类": {
        "keywords": ["sunglasses,aviator", "reading+glasses,optical", "cat+eye+sunglasses",
                     "round+glasses,frame", "sport+sunglasses,wrap", "blue+light+glasses",
                     "polarized+sunglasses", "rimless+glasses", "oversized+sunglasses",
                     "retro+glasses,vintage", "clip-on+sunglasses", "safety+glasses,goggles",
                     "bifocal+glasses", "designer+frames", "monocle,eyewear"],
    },
}


def download_images(timeout=12):
    """下载所有品类图片 (LoremFlickr + lock 确保确定性)"""
    client = httpx.Client(timeout=timeout, follow_redirects=True, headers={
        "User-Agent": "Mozilla/5.0 (compatible; ProductTest/1.0)"
    })

    all_images = []
    total_ok, total_fail = 0, 0

    for category, config in CATEGORY_QUERIES.items():
        cat_ok = 0
        for i, kw in enumerate(config["keywords"]):
            lock_id = hash(f"{category}_{kw}") % 100000
            url = f"https://loremflickr.com/400/400/{kw}?lock={lock_id}"
            try:
                resp = client.get(url)
                if resp.status_code == 200 and len(resp.content) > 2000:
                    img = Image.open(io.BytesIO(resp.content)).convert("RGB")
                    all_images.append({
                        "image": img,
                        "expected_l1": category,
                        "keyword": kw,
                        "url": url,
                    })
                    cat_ok += 1
                    total_ok += 1
                else:
                    total_fail += 1
            except Exception:
                total_fail += 1

        print(f"  {category}: {cat_ok}/{len(config['keywords'])}")

    client.close()
    print(f"  总计: {total_ok} 成功, {total_fail} 失败")
    return all_images


@torch.no_grad()
def classify_images(model, preprocess, text_features, l1_names, images, device):
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
                "keyword": item["keyword"],
            })

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--device", default="cuda")
    args = parser.parse_args()

    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] L1 确定性验证 (LoremFlickr 固定产品图)")
    print(f"  品类: {len(CATEGORY_QUERIES)}, 每类 15 张, 设备: {args.device}")
    print()

    # 1. 下载
    print("[1/3] 下载测试图片...")
    t0 = time.time()
    images = download_images()
    dl_time = time.time() - t0
    print(f"  下载耗时: {dl_time:.0f}s\n")

    if not images:
        print("ERROR: No images!")
        return

    # 2. 模型
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
    print(f"[3/3] 分类 {len(images)} 张图片...")
    t1 = time.time()
    results = classify_images(model, preprocess, text_features, l1_names, images, args.device)
    elapsed = time.time() - t1
    print(f"  完成: {elapsed:.1f}s ({len(results)/elapsed:.0f} img/s)\n")

    # 4. 统计
    total = len(results)
    correct = sum(1 for r in results if r["correct"])
    top3_correct = sum(1 for r in results if r["in_top3"])
    accuracy = correct / total * 100
    top3_accuracy = top3_correct / total * 100

    print("=" * 70)
    print(f"  Top-1 准确率: {correct}/{total} = {accuracy:.1f}%")
    print(f"  Top-3 准确率: {top3_correct}/{total} = {top3_accuracy:.1f}%")
    print("=" * 70)

    # 按品类
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

    print(f"\n{'品类':<12} {'Top-1':>6} {'Top-3':>6} {'总数':>4} {'Top-1%':>7} {'Top-3%':>7}  主要误分")
    print("-" * 75)
    for cat in CATEGORY_QUERIES.keys():
        s = cat_stats[cat]
        t = s["total"]
        a1 = s["correct"] / t * 100 if t else 0
        a3 = s["top3"] / t * 100 if t else 0
        wrong = ""
        if s["wrong_as"]:
            top_wrong = sorted(s["wrong_as"].items(), key=lambda x: -x[1])[:2]
            wrong = ", ".join(f"{w}({n})" for w, n in top_wrong)
        print(f"{cat:<12} {s['correct']:>6} {s['top3']:>6} {t:>4} {a1:>6.1f}% {a3:>6.1f}%  {wrong}")

    # 置信度
    confs = [r["confidence"] for r in results]
    correct_confs = [r["confidence"] for r in results if r["correct"]]
    wrong_confs = [r["confidence"] for r in results if not r["correct"]]
    print(f"\n置信度: 全部={np.mean(confs):.4f}, "
          f"正确={np.mean(correct_confs):.4f}, "
          f"错误={np.mean(wrong_confs):.4f}" if wrong_confs else "")

    # 保存
    output_dir = "/data/imgsrch/task_logs"
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"l1_deterministic_{ts}.json")

    summary = {
        "timestamp": datetime.now().isoformat(),
        "total": total, "correct": correct, "top3_correct": top3_correct,
        "accuracy": round(accuracy, 2), "top3_accuracy": round(top3_accuracy, 2),
        "per_category": {},
        "errors": [],
    }
    for cat in CATEGORY_QUERIES:
        s = cat_stats[cat]
        t = s["total"]
        summary["per_category"][cat] = {
            "correct": s["correct"], "top3": s["top3"], "total": t,
            "top1_acc": round(s["correct"] / t * 100, 1) if t else 0,
            "top3_acc": round(s["top3"] / t * 100, 1) if t else 0,
        }
    summary["errors"] = [r for r in results if not r["correct"]]

    with open(output_file, "w") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"\n结果保存: {output_file}")


if __name__ == "__main__":
    main()
