#!/usr/bin/env python3
"""
L1 品类分类准确率验证

用本地已有图片 + ViT-L-14 两级分类测试:
1. 从 /data/imgsrch/downloads/ 采样 (服装/箱包/鞋帽等 szwego 图片)
2. 从 /data/benchmark_images/ 采样 (家具类图片)
3. 统计各品类分类结果分布, 输出混淆矩阵和准确率

Usage:
    python3 scripts/validate_l1_accuracy.py [--count 2000]
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import torch
from PIL import Image

# 导入 taxonomy
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "services" / "inference-service"))
from app.taxonomy import (
    L1_CATEGORIES, L1_CODES, L1_CODE_TO_NAME, L1_NAMES,
    FASHION_L1_CODES, TAGS, TAG_NAMES,
)


def load_vitl14(device="cpu"):
    """加载 ViT-L-14 模型"""
    import open_clip

    _orig = torch.load
    torch.load = lambda *a, **kw: _orig(*a, **{**kw, "weights_only": False})

    model_path = os.environ.get("CLIP_MODEL_PATH", "/data/imgsrch/models/ViT-L-14.pt")
    print(f"[{datetime.now():%H:%M:%S}] Loading ViT-L-14 from {model_path}...")
    model, _, preprocess = open_clip.create_model_and_transforms(
        "ViT-L-14", pretrained=model_path
    )
    torch.load = _orig
    model.eval().to(device)
    tokenizer = open_clip.get_tokenizer("ViT-L-14")

    return model, preprocess, tokenizer


def compute_text_features(model, tokenizer, device="cpu"):
    """预计算 L1 品类文本特征"""
    l1_names = []
    all_texts = []
    counts = []

    for code in L1_CODES:
        info = L1_CATEGORIES[code]
        l1_names.append(info["name"])
        prompts = info["prompts"]
        all_texts.extend(prompts)
        counts.append(len(prompts))

    tokens = tokenizer(all_texts).to(device)
    with torch.no_grad():
        feats = model.encode_text(tokens)
    feats = feats / feats.norm(dim=-1, keepdim=True)

    # 多 prompt 取平均
    averaged = []
    idx = 0
    for c in counts:
        avg = feats[idx:idx + c].mean(dim=0)
        avg = avg / avg.norm()
        averaged.append(avg)
        idx += c

    text_features = torch.stack(averaged).float()  # (18, 768)
    return l1_names, text_features


def collect_images(count=2000):
    """收集本地图片, 标注预期品类"""
    images = []

    # 1. /data/imgsrch/downloads/ — szwego 产品图 (主要是服装/箱包/鞋帽)
    downloads_dir = "/data/imgsrch/downloads"
    if os.path.isdir(downloads_dir):
        dl_files = []
        for root, dirs, files in os.walk(downloads_dir):
            for f in files:
                if f.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
                    dl_files.append(os.path.join(root, f))

        # 随机采样
        n_download = min(len(dl_files), int(count * 0.6))
        sampled = random.sample(dl_files, n_download)
        for path in sampled:
            images.append({"path": path, "expected_l1": "szwego产品图", "source": "downloads"})
        print(f"  downloads/: sampled {n_download} from {len(dl_files)}")

    # 2. /data/benchmark_images/ — 家具类图片
    bench_dir = "/data/benchmark_images"
    if os.path.isdir(bench_dir):
        bench_files = []
        for root, dirs, files in os.walk(bench_dir):
            for f in files:
                if f.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
                    bench_files.append(os.path.join(root, f))

        n_bench = min(len(bench_files), int(count * 0.4))
        sampled = random.sample(bench_files, n_bench)
        for path in sampled:
            images.append({"path": path, "expected_l1": "家具类", "source": "benchmark"})
        print(f"  benchmark/: sampled {n_bench} from {len(bench_files)}")

    random.shuffle(images)
    return images[:count]


@torch.no_grad()
def classify_batch(model, preprocess, text_features, l1_names, paths, device="cpu", batch_size=64):
    """批量分类"""
    results = []
    total = len(paths)

    for i in range(0, total, batch_size):
        batch_paths = paths[i:i + batch_size]
        tensors = []
        valid_indices = []

        for j, p in enumerate(batch_paths):
            try:
                img = Image.open(p).convert("RGB")
                tensors.append(preprocess(img))
                valid_indices.append(i + j)
            except Exception:
                continue

        if not tensors:
            continue

        batch = torch.stack(tensors).to(device)
        features = model.encode_image(batch)
        features = features / features.norm(dim=-1, keepdim=True)

        sims = (features.float() @ text_features.T)  # (B, 18)

        for k in range(len(tensors)):
            idx = sims[k].argmax().item()
            conf = sims[k][idx].item()

            # top-3
            top3_vals, top3_idx = sims[k].topk(3)
            top3 = [(l1_names[ti], round(tv.item(), 4)) for ti, tv in zip(top3_idx, top3_vals)]

            results.append({
                "path": batch_paths[k] if k < len(batch_paths) else "?",
                "predicted_l1": l1_names[idx],
                "confidence": round(conf, 4),
                "top3": top3,
            })

        done = min(i + batch_size, total)
        if done % 200 == 0 or done == total:
            print(f"  [{datetime.now():%H:%M:%S}] Classified {done}/{total}")

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--device", default="cpu")
    parser.add_argument("--batch-size", type=int, default=64)
    args = parser.parse_args()

    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] L1 品类分类准确率验证")
    print(f"  目标: {args.count} 张图片, 设备: {args.device}")
    print()

    # 1. 收集图片
    print("[1/3] 收集图片...")
    images = collect_images(args.count)
    print(f"  总计: {len(images)} 张\n")

    # 2. 加载模型
    print("[2/3] 加载模型...")
    model, preprocess, tokenizer = load_vitl14(args.device)
    l1_names, text_features = compute_text_features(model, tokenizer, args.device)
    text_features = text_features.to(args.device)
    print(f"  L1 品类: {len(l1_names)} 类\n")

    # 3. 批量分类
    print(f"[3/3] 分类 {len(images)} 张图片...")
    t0 = time.time()
    paths = [img["path"] for img in images]
    results = classify_batch(model, preprocess, text_features, l1_names, paths,
                             args.device, args.batch_size)
    elapsed = time.time() - t0
    print(f"  分类完成: {len(results)} 张, 耗时 {elapsed:.1f}s ({len(results)/elapsed:.0f} img/s)\n")

    # 4. 统计
    print("=" * 70)
    print("分类结果分布")
    print("=" * 70)

    # 按预测品类统计
    cat_counts = {}
    for r in results:
        cat = r["predicted_l1"]
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    # 按数量排序
    sorted_cats = sorted(cat_counts.items(), key=lambda x: -x[1])
    total = len(results)

    print(f"\n{'品类':<12} {'数量':>6} {'占比':>8} {'平均置信度':>10}")
    print("-" * 40)

    # 计算每个品类的平均置信度
    cat_confs = {}
    for r in results:
        cat = r["predicted_l1"]
        if cat not in cat_confs:
            cat_confs[cat] = []
        cat_confs[cat].append(r["confidence"])

    for cat, count in sorted_cats:
        pct = count / total * 100
        avg_conf = sum(cat_confs[cat]) / len(cat_confs[cat])
        print(f"{cat:<12} {count:>6} {pct:>7.1f}% {avg_conf:>10.4f}")

    print(f"\n{'总计':<12} {total:>6}")

    # 按来源分组统计
    print("\n" + "=" * 70)
    print("按来源分组统计")
    print("=" * 70)

    source_cats = {}
    for img, res in zip(images[:len(results)], results):
        src = img["source"]
        if src not in source_cats:
            source_cats[src] = {}
        cat = res["predicted_l1"]
        source_cats[src][cat] = source_cats[src].get(cat, 0) + 1

    for src, cats in source_cats.items():
        print(f"\n  [{src}] ({sum(cats.values())} 张)")
        for cat, count in sorted(cats.items(), key=lambda x: -x[1])[:10]:
            print(f"    {cat}: {count} ({count/sum(cats.values())*100:.1f}%)")

    # 置信度分布
    print("\n" + "=" * 70)
    print("置信度分布")
    print("=" * 70)
    confs = [r["confidence"] for r in results]
    print(f"  最低: {min(confs):.4f}")
    print(f"  最高: {max(confs):.4f}")
    print(f"  平均: {sum(confs)/len(confs):.4f}")
    print(f"  中位: {sorted(confs)[len(confs)//2]:.4f}")

    bins = [(0.0, 0.15), (0.15, 0.20), (0.20, 0.25), (0.25, 0.30), (0.30, 1.0)]
    print(f"\n  {'区间':<15} {'数量':>6} {'占比':>8}")
    for lo, hi in bins:
        n = sum(1 for c in confs if lo <= c < hi)
        print(f"  [{lo:.2f}, {hi:.2f}) {n:>6} {n/len(confs)*100:>7.1f}%")

    # 保存详细结果
    output_dir = "/data/imgsrch/task_logs"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"l1_validation_{datetime.now():%Y%m%d_%H%M%S}.json")
    with open(output_file, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "total_images": len(results),
            "elapsed_seconds": round(elapsed, 1),
            "category_distribution": dict(sorted_cats),
            "confidence_stats": {
                "min": min(confs), "max": max(confs),
                "mean": sum(confs)/len(confs),
                "median": sorted(confs)[len(confs)//2],
            },
            "results": results[:100],  # 前100条详细结果
        }, f, ensure_ascii=False, indent=2)
    print(f"\n详细结果保存: {output_file}")


if __name__ == "__main__":
    main()
