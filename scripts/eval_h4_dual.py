#!/usr/bin/env python3
"""
H4 双向量评测: DINOv2 vs CLIP vs 融合 (6 场景)
- 对每个场景, 分别用 dinov2_vec 和 clip_vec 搜索
- 对比 Recall@1, Recall@10, Recall@100
- 新增: 融合模式 (DINOv2 0.6 + CLIP 0.4 加权)
"""
import json
import os
import time
import numpy as np
import torch
from PIL import Image
from pymilvus import connections, Collection
from concurrent.futures import ThreadPoolExecutor


COLLECTION = "h4_main"
EVAL_DIR = "/data/imgsrch/eval_queries"
DINOV2_PATH = "/data/imgsrch/models/dinov2-large"

DINOV2_MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32)
DINOV2_STD = np.array([0.229, 0.224, 0.225], dtype=np.float32)
CLIP_MEAN = np.array([0.48145466, 0.4578275, 0.40821073], dtype=np.float32)
CLIP_STD = np.array([0.26862954, 0.26130258, 0.27577711], dtype=np.float32)


def preprocess_dual(img):
    if img.size != (224, 224):
        img = img.resize((224, 224), Image.BICUBIC)
    arr = np.array(img, dtype=np.float32) * (1.0 / 255.0)
    dino = ((arr - DINOV2_MEAN) / DINOV2_STD).transpose(2, 0, 1)
    clip = ((arr - CLIP_MEAN) / CLIP_STD).transpose(2, 0, 1)
    return dino, clip


def main():
    milvus_host = os.environ.get("MILVUS_HOST", "imgsrch-mvs-proxy")
    connections.connect(host=milvus_host, port="19530")
    coll = Collection(COLLECTION)
    coll.load()
    print(f"Collection: {coll.num_entities:,}")

    # 加载模型
    from transformers import AutoModel
    import open_clip
    dinov2 = AutoModel.from_pretrained(DINOV2_PATH, dtype=torch.float16).cuda().eval()
    clip_model, _, _ = open_clip.create_model_and_transforms("ViT-L-14", pretrained="/data/imgsrch/models/ViT-L-14.pt")
    clip_visual = clip_model.visual.cuda().half().eval()
    del clip_model
    print("模型加载完成")

    with open(os.path.join(EVAL_DIR, "ground_truth.json")) as f:
        gt = json.load(f)
    scenarios = gt["scenarios"]

    pool = ThreadPoolExecutor(max_workers=16)
    BS = 32
    search_params = {"metric_type": "COSINE", "params": {"ef": 300}}

    all_results = {}

    for scenario in scenarios:
        print(f"\n=== {scenario} ===")
        scenario_dir = os.path.join(EVAL_DIR, scenario)
        files = sorted(os.listdir(scenario_dir))[:200]

        # 加载图片
        def load(fname):
            try:
                img = Image.open(os.path.join(scenario_dir, fname)).convert("RGB")
                d, c = preprocess_dual(img)
                return (fname.replace(".jpg", ""), d, c)
            except:
                return None

        loaded = list(pool.map(load, files))
        valid = [x for x in loaded if x is not None]
        pks = [v[0] for v in valid]
        dino_arrs = np.stack([v[1] for v in valid])
        clip_arrs = np.stack([v[2] for v in valid])

        # GPU 推理
        dino_t = torch.from_numpy(dino_arrs).cuda().to(torch.float16)
        clip_t = torch.from_numpy(clip_arrs).cuda().to(torch.float16)

        with torch.no_grad():
            d_out = dinov2(pixel_values=dino_t)
            d_cls = d_out.last_hidden_state[:, 0, :]
            d_cls = d_cls / d_cls.norm(dim=-1, keepdim=True)

            c_out = clip_visual(clip_t)
            c_cls = c_out / c_out.norm(dim=-1, keepdim=True)

        dino_vecs = d_cls.cpu().float().numpy()
        clip_vecs = c_cls.cpu().float().numpy()

        # 三种搜索模式
        modes = {
            "dinov2": ("dinov2_vec", dino_vecs),
            "clip": ("clip_vec", clip_vecs),
        }

        scenario_results = {}
        for mode_name, (field, vecs) in modes.items():
            r1 = r10 = r100 = 0
            latencies = []

            for batch_start in range(0, len(pks), BS):
                batch_pks = pks[batch_start:batch_start+BS]
                batch_vecs = vecs[batch_start:batch_start+BS]

                t0 = time.time()
                res = coll.search(
                    data=batch_vecs.tolist(),
                    anns_field=field,
                    param=search_params,
                    limit=100,
                    output_fields=["image_pk"]
                )
                per_q = (time.time() - t0) * 1000 / len(batch_pks)
                latencies.extend([per_q] * len(batch_pks))

                for i, pk in enumerate(batch_pks):
                    hits = [h.entity.get("image_pk") for h in res[i]]
                    if pk in hits[:1]: r1 += 1
                    if pk in hits[:10]: r10 += 1
                    if pk in hits[:100]: r100 += 1

            n = len(pks)
            scenario_results[mode_name] = {
                "R@1": r1/n, "R@10": r10/n, "R@100": r100/n,
                "P50": float(np.percentile(latencies, 50)),
                "P95": float(np.percentile(latencies, 95)),
            }
            print(f"  {mode_name:>8}: R@1={r1/n*100:.1f}% R@10={r10/n*100:.1f}% R@100={r100/n*100:.1f}% P50={np.percentile(latencies,50):.1f}ms")

        # 融合模式: DINOv2 0.6 + CLIP 0.4
        # 做法: 两路各取 Top 100, 加权 merge
        r1 = r10 = r100 = 0
        latencies = []
        for batch_start in range(0, len(pks), BS):
            batch_pks = pks[batch_start:batch_start+BS]
            batch_dino = dino_vecs[batch_start:batch_start+BS]
            batch_clip = clip_vecs[batch_start:batch_start+BS]

            t0 = time.time()
            res_d = coll.search(data=batch_dino.tolist(), anns_field="dinov2_vec", param=search_params, limit=100, output_fields=["image_pk"])
            res_c = coll.search(data=batch_clip.tolist(), anns_field="clip_vec", param=search_params, limit=100, output_fields=["image_pk"])
            per_q = (time.time() - t0) * 1000 / len(batch_pks)
            latencies.extend([per_q] * len(batch_pks))

            for i, pk in enumerate(batch_pks):
                # 合并 (加权 score)
                scores = {}
                for h in res_d[i]:
                    scores[h.entity.get("image_pk")] = 0.6 * float(h.score)
                for h in res_c[i]:
                    k = h.entity.get("image_pk")
                    scores[k] = scores.get(k, 0) + 0.4 * float(h.score)
                ranked = sorted(scores.items(), key=lambda x: -x[1])
                hits = [k for k, v in ranked]
                if pk in hits[:1]: r1 += 1
                if pk in hits[:10]: r10 += 1
                if pk in hits[:100]: r100 += 1

        n = len(pks)
        scenario_results["fusion"] = {
            "R@1": r1/n, "R@10": r10/n, "R@100": r100/n,
            "P50": float(np.percentile(latencies, 50)),
            "P95": float(np.percentile(latencies, 95)),
        }
        print(f"  {'fusion':>8}: R@1={r1/n*100:.1f}% R@10={r10/n*100:.1f}% R@100={r100/n*100:.1f}% P50={np.percentile(latencies,50):.1f}ms")

        all_results[scenario] = scenario_results

    # 总报告
    print("\n" + "="*90)
    print("📊 H4 双向量评测报告 (DINOv2 1024d + CLIP 768d)")
    print("="*90)
    print(f"{'场景':<20} {'模式':<10} {'R@1':<10} {'R@10':<10} {'R@100':<10} {'P50':<10}")
    print("-"*90)
    for scenario in scenarios:
        for mode in ["dinov2", "clip", "fusion"]:
            r = all_results[scenario][mode]
            label = f"{scenario}" if mode == "dinov2" else ""
            print(f"{label:<20} {mode:<10} {r['R@1']*100:>6.1f}% {r['R@10']*100:>8.1f}% {r['R@100']*100:>8.1f}% {r['P50']:>7.1f}ms")
        print()

    # 保存
    with open("/data/imgsrch/task_logs/eval_h4_results.json", "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"详细结果: /data/imgsrch/task_logs/eval_h4_results.json")


if __name__ == "__main__":
    main()
