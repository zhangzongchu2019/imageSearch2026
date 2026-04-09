#!/usr/bin/env python3
"""
评测 H3 方案 (DINOv2-L 1024d HNSW_SQ8) 的 Recall@K 和延迟
- 用 6 场景 × 1000 query (来自 /data/imgsrch/eval_queries/)
- 每个 query 跑 DINOv2 推理 + Milvus 搜索
- 计算 Recall@1 / Recall@10 / Recall@100
- 按场景分组报告
- 延迟 P50/P95/P99
"""
import argparse
import json
import os
import time
import numpy as np
import torch
from PIL import Image
from pymilvus import connections, Collection
from concurrent.futures import ThreadPoolExecutor


COLLECTION = "test_dinov2_h3"
EVAL_DIR = "/data/imgsrch/eval_queries"
MODEL_PATH = "/data/imgsrch/models/dinov2-large"

DINOV2_MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32)
DINOV2_STD = np.array([0.229, 0.224, 0.225], dtype=np.float32)


def fast_preprocess(img):
    if img.size != (224, 224):
        img = img.resize((224, 224), Image.BICUBIC)
    arr = np.array(img, dtype=np.float32) * (1.0 / 255.0)
    arr = (arr - DINOV2_MEAN) / DINOV2_STD
    return arr.transpose(2, 0, 1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ef", type=int, default=300)
    parser.add_argument("--top-k", type=int, default=100)
    parser.add_argument("--max-per-scenario", type=int, default=1000)
    args = parser.parse_args()

    # 加载模型
    print("加载 DINOv2-L...")
    from transformers import AutoModel
    model = AutoModel.from_pretrained(MODEL_PATH, dtype=torch.float16).cuda().eval()
    print("  ✅")

    # 加载 Collection (gpu_worker 内用 milvus proxy hostname)
    print("加载 Milvus Collection...")
    import os
    milvus_host = os.environ.get("MILVUS_HOST", "imgsrch-mvs-proxy")
    connections.connect(host=milvus_host, port="19530")
    coll = Collection(COLLECTION)
    coll.load()
    print(f"  ✅ {coll.num_entities:,} entities")

    # 加载 ground truth
    print("加载 ground truth...")
    with open(os.path.join(EVAL_DIR, "ground_truth.json")) as f:
        gt = json.load(f)
    scenarios = gt["scenarios"]
    print(f"  场景: {scenarios}")

    # 评测每个场景
    results = {}
    BS = 32

    for scenario in scenarios:
        print(f"\n=== 场景: {scenario} ===")
        scenario_dir = os.path.join(EVAL_DIR, scenario)
        files = sorted(os.listdir(scenario_dir))[:args.max_per_scenario]

        # 并行加载图片
        def load(fname):
            try:
                img = Image.open(os.path.join(scenario_dir, fname)).convert("RGB")
                return (fname.replace(".jpg", ""), fast_preprocess(img))
            except:
                return None

        pool = ThreadPoolExecutor(max_workers=16)

        # 推理 + 搜索
        recall_at_1 = 0
        recall_at_10 = 0
        recall_at_100 = 0
        latencies = []
        n_queries = 0

        t_start = time.time()
        for batch_start in range(0, len(files), BS):
            batch_files = files[batch_start:batch_start+BS]

            # 加载 + 预处理
            loaded = list(pool.map(load, batch_files))
            valid = [x for x in loaded if x is not None]
            if not valid:
                continue

            pks = [v[0] for v in valid]
            arrs = np.stack([v[1] for v in valid])

            # GPU 推理
            tensor = torch.from_numpy(arrs).cuda().to(torch.float16)
            with torch.no_grad():
                out = model(pixel_values=tensor)
                cls = out.last_hidden_state[:, 0, :]
                cls = cls / cls.norm(dim=-1, keepdim=True)
                vecs = cls.cpu().float().numpy()

            # 批量搜索
            t0 = time.time()
            search_results = coll.search(
                data=vecs.tolist(),
                anns_field="dinov2_vec",
                param={"metric_type": "COSINE", "params": {"ef": args.ef}},
                limit=args.top_k,
                output_fields=["image_pk"]
            )
            search_elapsed = (time.time() - t0) * 1000
            per_query_ms = search_elapsed / len(pks)
            latencies.extend([per_query_ms] * len(pks))

            # 计算 Recall
            for i, pk in enumerate(pks):
                # 真实答案是原图 pk (变体的 image_pk 和原图相同)
                target_pk = pk
                hits = [h.entity.get("image_pk") for h in search_results[i]]
                if target_pk in hits[:1]:
                    recall_at_1 += 1
                if target_pk in hits[:10]:
                    recall_at_10 += 1
                if target_pk in hits[:100]:
                    recall_at_100 += 1
                n_queries += 1

        pool.shutdown()
        elapsed = time.time() - t_start

        latencies_np = np.array(latencies) if latencies else np.array([0])

        results[scenario] = {
            "n_queries": n_queries,
            "recall_at_1": recall_at_1 / n_queries if n_queries else 0,
            "recall_at_10": recall_at_10 / n_queries if n_queries else 0,
            "recall_at_100": recall_at_100 / n_queries if n_queries else 0,
            "latency_p50_ms": float(np.percentile(latencies_np, 50)),
            "latency_p95_ms": float(np.percentile(latencies_np, 95)),
            "latency_p99_ms": float(np.percentile(latencies_np, 99)),
            "total_time_s": elapsed,
        }

        r = results[scenario]
        print(f"  N={n_queries}")
        print(f"  Recall@1:   {r['recall_at_1']*100:.1f}%")
        print(f"  Recall@10:  {r['recall_at_10']*100:.1f}%")
        print(f"  Recall@100: {r['recall_at_100']*100:.1f}%")
        print(f"  Latency P50: {r['latency_p50_ms']:.1f}ms, P95: {r['latency_p95_ms']:.1f}ms, P99: {r['latency_p99_ms']:.1f}ms")
        print(f"  耗时: {elapsed:.0f}s")

    # 总报告
    print("\n" + "="*70)
    print("📊 H3 方案评测报告 (DINOv2-L 1024d HNSW_SQ8)")
    print("="*70)
    print(f"{'场景':<20} {'R@1':<10} {'R@10':<10} {'R@100':<10} {'P50':<10} {'P95':<10}")
    print("-"*70)
    for scenario in scenarios:
        r = results[scenario]
        print(f"{scenario:<20} {r['recall_at_1']*100:>6.1f}% {r['recall_at_10']*100:>8.1f}% {r['recall_at_100']*100:>8.1f}% {r['latency_p50_ms']:>7.1f}ms {r['latency_p95_ms']:>7.1f}ms")

    # 平均
    avg_r1 = np.mean([r["recall_at_1"] for r in results.values()])
    avg_r10 = np.mean([r["recall_at_10"] for r in results.values()])
    avg_r100 = np.mean([r["recall_at_100"] for r in results.values()])
    avg_p50 = np.mean([r["latency_p50_ms"] for r in results.values()])
    avg_p95 = np.mean([r["latency_p95_ms"] for r in results.values()])
    print("-"*70)
    print(f"{'AVG':<20} {avg_r1*100:>6.1f}% {avg_r10*100:>8.1f}% {avg_r100*100:>8.1f}% {avg_p50:>7.1f}ms {avg_p95:>7.1f}ms")

    # 保存
    with open("/data/imgsrch/task_logs/eval_h3_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n详细结果: /data/imgsrch/task_logs/eval_h3_results.json")


if __name__ == "__main__":
    main()
