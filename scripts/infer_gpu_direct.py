#!/usr/bin/env python3
"""
GPU 直接推理 — 不走 HTTP, batch=32, 4 卡并行
在 gpu_worker 容器内运行

用法:
  python3 infer_gpu_direct.py --image-dir /tmp/import_batch1 --output /data/imgsrch/infer_batch1.jsonl --gpu 0

4 卡并行:
  for gpu in 0 1 2 3; do
    python3 infer_gpu_direct.py --image-dir /tmp/import_batch1 --output /data/imgsrch/infer_batch1_gpu${gpu}.jsonl --gpu $gpu --shard $gpu --total-shards 4 &
  done
"""
import argparse, os, json, time, glob, sys
import numpy as np
import torch
from PIL import Image
from io import BytesIO

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--gpu", type=int, default=0)
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--total-shards", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=32)
    args = parser.parse_args()

    os.environ["CUDA_VISIBLE_DEVICES"] = str(args.gpu)
    device = torch.device("cuda")

    print(f"[GPU {args.gpu}] shard {args.shard}/{args.total_shards} batch={args.batch_size}", flush=True)

    # === 加载模型 ===
    import open_clip
    t0 = time.time()
    model, _, preprocess = open_clip.create_model_and_transforms(
        "ViT-L-14", pretrained="/data/imgsrch/models/ViT-L-14.pt")
    model = model.to(device).half().eval()
    print(f"[GPU {args.gpu}] ViT-L-14 loaded ({time.time()-t0:.1f}s)", flush=True)

    # 投影矩阵 768→256 (seed=42, SVD)
    rng = np.random.RandomState(42)
    proj = rng.randn(768, 256).astype(np.float32)
    u, _, _ = np.linalg.svd(proj, full_matrices=False)
    projection = torch.from_numpy(u).to(device).half()  # (768, 256)

    # L1 分类文本特征
    sys.path.insert(0, "/workspace/services/inference-service")
    from app.taxonomy import L1_CATEGORIES, L1_CODES, L1_CODE_TO_NAME
    from app.taxonomy import FASHION_L2, NONFASHION_L2, FASHION_L3

    tokenizer = open_clip.get_tokenizer("ViT-L-14")
    l1_names = [L1_CODE_TO_NAME[c] for c in L1_CODES]
    l1_prompts = [f"a photo of {name}" for name in l1_names]
    l1_tokens = tokenizer(l1_prompts).to(device)
    with torch.no_grad():
        l1_text_feat = model.encode_text(l1_tokens).half()
        l1_text_feat = l1_text_feat / l1_text_feat.norm(dim=-1, keepdim=True)
    # L1 投影到 256 维 (用于 tags 等)
    l1_text_feat_768 = l1_text_feat  # (N, 768)

    # L2 分类: 构建 l1_code → (l2_codes, l2_text_features)
    l2_info = {}  # l1_code → {'codes': [], 'features': tensor}
    for l1_code, l2_dict in {**{k: v for k, v in FASHION_L2.items()}, **{k: v for k, v in NONFASHION_L2.items()}}.items():
        codes = sorted(l2_dict.keys())
        prompts = [f"a photo of {l2_dict[c]['name']}" for c in codes]
        tokens = tokenizer(prompts).to(device)
        with torch.no_grad():
            feat = model.encode_text(tokens).half()
            feat = feat / feat.norm(dim=-1, keepdim=True)
        l2_info[l1_code] = {"codes": codes, "features": feat}

    print(f"[GPU {args.gpu}] L1: {len(L1_CODES)} classes, L2: {len(l2_info)} groups", flush=True)

    # === 获取文件列表 ===
    files = sorted(glob.glob(os.path.join(args.image_dir, "*.jpg")))
    # 分片
    shard_files = [f for i, f in enumerate(files) if i % args.total_shards == args.shard]
    print(f"[GPU {args.gpu}] 总文件: {len(files):,}, 本分片: {len(shard_files):,}", flush=True)

    # === 批量推理 ===
    out = open(args.output, "w")
    processed = ok = fail = 0
    t_start = time.time()
    BS = args.batch_size

    for batch_start in range(0, len(shard_files), BS):
        batch_files = shard_files[batch_start:batch_start + BS]

        # 加载图片
        tensors = []
        valid_idx = []
        pks = []
        for i, fpath in enumerate(batch_files):
            try:
                img = Image.open(fpath).convert("RGB")
                tensors.append(preprocess(img))
                valid_idx.append(i)
                pks.append(os.path.basename(fpath).replace(".jpg", ""))
            except:
                fail += 1

        if not tensors:
            continue

        batch_tensor = torch.stack(tensors).to(device).half()

        # 推理
        with torch.no_grad():
            image_features = model.encode_image(batch_tensor)  # (N, 768)
            image_features = image_features / image_features.norm(dim=-1, keepdim=True)

            # 投影到 256 维
            vec_256 = (image_features @ projection).float()  # (N, 256)
            vec_256 = vec_256 / vec_256.norm(dim=-1, keepdim=True)

            # L1 分类
            sim_l1 = (image_features @ l1_text_feat_768.T).float()  # (N, num_l1)
            l1_idx = sim_l1.argmax(dim=-1)  # (N,)

        # 逐条处理 L2 + 输出
        for j in range(len(pks)):
            vec = vec_256[j].cpu().numpy().tolist()
            l1_code = L1_CODES[l1_idx[j].item()]

            # L2 分类
            l2_code = 0
            if l1_code in l2_info:
                with torch.no_grad():
                    sim_l2 = (image_features[j:j+1] @ l2_info[l1_code]["features"].T).float()
                    l2_idx = sim_l2.argmax(dim=-1).item()
                    l2_code = l2_info[l1_code]["codes"][l2_idx]

            record = {
                "image_pk": pks[j],
                "global_vec": vec,
                "category_l1": l1_code,
                "category_l2": l2_code,
                "category_l3": 0,
                "tags": [],
                "color_code": 0,
                "material_code": 0,
                "style_code": 0,
                "season_code": 0,
                "is_evergreen": False,
            }
            out.write(json.dumps(record) + "\n")
            ok += 1

        processed += len(batch_files)
        if processed % 10000 == 0:
            elapsed = time.time() - t_start
            rate = ok / elapsed
            eta = (len(shard_files) - processed) / rate / 60 if rate > 0 else 0
            print(f"[GPU {args.gpu}] {processed:,}/{len(shard_files):,} ({rate:.0f}/s) ok={ok:,} fail={fail:,} ETA={eta:.0f}min", flush=True)

    out.close()
    elapsed = time.time() - t_start
    print(f"[GPU {args.gpu}] 完成: {ok:,} ok, {fail:,} fail, {elapsed:.0f}s ({ok/elapsed:.0f}/s)", flush=True)


if __name__ == "__main__":
    main()
