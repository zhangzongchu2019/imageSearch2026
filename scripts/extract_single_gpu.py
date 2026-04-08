#!/usr/bin/env python3
"""
单 GPU 特征提取 worker。配合 CUDA_VISIBLE_DEVICES 使用。

Usage:
    CUDA_VISIBLE_DEVICES=0 python3 scripts/extract_single_gpu.py \
        --file-list /tmp/gpu_chunk_aa --output /data/imgsrch/task_logs/extract_output/gpu_0.jsonl \
        --model-path /data/imgsrch/models/ViT-L-14.pt --gpu-id 0
"""
import argparse
import hashlib
import json
import logging
import os
import sys
import time

import numpy as np
import torch
import open_clip
from PIL import Image

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("extract_worker")

EMBEDDING_DIM = 256
CLIP_DIM = 768
GPU_BATCH_SIZE = 128
PROGRESS_DIR = "/tmp/gpu_progress"


def build_projection_matrix():
    rng = np.random.RandomState(42)
    proj = rng.randn(CLIP_DIM, EMBEDDING_DIM).astype(np.float32)
    u, _, vt = np.linalg.svd(proj, full_matrices=False)
    return u


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file-list", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--model-path", default="/data/imgsrch/models/ViT-L-14.pt")
    parser.add_argument("--gpu-id", type=int, default=0)
    args = parser.parse_args()

    gpu_id = args.gpu_id
    device = torch.device("cuda:0")  # CUDA_VISIBLE_DEVICES 控制实际 GPU

    # 读取文件列表
    with open(args.file_list) as f:
        paths = [l.strip() for l in f if l.strip()]
    log.info(f"[GPU{gpu_id}] {len(paths)} images, device={device}")

    # 加载模型
    log.info(f"[GPU{gpu_id}] Loading ViT-L-14...")
    _orig = torch.load
    torch.load = lambda *a, **kw: _orig(*a, **{**kw, "weights_only": False})
    model, _, preprocess = open_clip.create_model_and_transforms(
        "ViT-L-14", pretrained=args.model_path, device=device)
    torch.load = _orig
    model.eval()
    tokenizer = open_clip.get_tokenizer("ViT-L-14")
    projection = build_projection_matrix()

    # 加载品类体系
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     "..", "services", "inference-service"))
    from app.taxonomy import L1_CATEGORIES, L1_CODES, L1_CODE_TO_NAME, TAGS, TAG_NAMES
    taxonomy = {"L1_CATEGORIES": L1_CATEGORIES, "L1_CODES": L1_CODES,
                "L1_CODE_TO_NAME": L1_CODE_TO_NAME, "TAGS": TAGS, "TAG_NAMES": TAG_NAMES}

    # 预计算 L1 文本特征
    l1_codes = taxonomy["L1_CODES"]
    l1_names = [taxonomy["L1_CODE_TO_NAME"][c] for c in l1_codes]
    l1_prompts_all = []
    l1_counts = []
    for c in l1_codes:
        ps = taxonomy["L1_CATEGORIES"][c]["prompts"]
        l1_prompts_all.extend(ps)
        l1_counts.append(len(ps))
    l1_tokens = tokenizer(l1_prompts_all).to(device)
    with torch.no_grad():
        l1_feats = model.encode_text(l1_tokens)
    l1_feats = l1_feats / l1_feats.norm(dim=-1, keepdim=True)
    # 多 prompt 平均
    averaged = []
    idx = 0
    for c in l1_counts:
        avg = l1_feats[idx:idx+c].mean(dim=0)
        avg = avg / avg.norm()
        averaged.append(avg)
        idx += c
    l1_text_features = torch.stack(averaged).float()

    # 标签文本特征
    tag_names = taxonomy["TAG_NAMES"]
    tag_prompts_all = []
    tag_counts = []
    for n in tag_names:
        ps = taxonomy["TAGS"][n]
        if isinstance(ps, str):
            ps = [ps]
        tag_prompts_all.extend(ps)
        tag_counts.append(len(ps))
    tag_tokens = tokenizer(tag_prompts_all).to(device)
    with torch.no_grad():
        tag_feats = model.encode_text(tag_tokens)
    tag_feats = tag_feats / tag_feats.norm(dim=-1, keepdim=True)
    tag_averaged = []
    idx = 0
    for c in tag_counts:
        avg = tag_feats[idx:idx+c].mean(dim=0)
        avg = avg / avg.norm()
        tag_averaged.append(avg)
        idx += c
    tag_text_features = torch.stack(tag_averaged).float()

    log.info(f"[GPU{gpu_id}] Models ready: {len(l1_codes)} L1, {len(tag_names)} tags")

    # 推理
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    os.makedirs(PROGRESS_DIR, exist_ok=True)
    progress_file = f"{PROGRESS_DIR}/gpu_{gpu_id}.json"
    total = len(paths)
    total_batches = (total + GPU_BATCH_SIZE - 1) // GPU_BATCH_SIZE
    done = 0
    t0 = time.time()

    with open(args.output, "w") as out_f:
        for bi in range(0, total, GPU_BATCH_SIZE):
            batch_paths = paths[bi:bi + GPU_BATCH_SIZE]

            # 加载图片
            tensors = []
            valid_paths = []
            for p in batch_paths:
                try:
                    img = Image.open(p).convert("RGB")
                    tensors.append(preprocess(img))
                    valid_paths.append(p)
                except Exception:
                    pass

            if not tensors:
                continue

            batch = torch.stack(tensors).to(device)
            with torch.no_grad():
                features = model.encode_image(batch)  # (B, 768)

            # L1 分类
            feat_norm = features.float()
            feat_norm = feat_norm / feat_norm.norm(dim=-1, keepdim=True)
            l1_sims = feat_norm @ l1_text_features.T
            tag_sims = feat_norm @ tag_text_features.T

            # 投影 768→256
            feat_np = features.cpu().numpy().astype(np.float32)
            reduced = feat_np @ projection
            norms = np.linalg.norm(reduced, axis=1, keepdims=True)
            norms = np.maximum(norms, 1e-8)
            feat_256 = (reduced / norms).astype(np.float32)

            # 写结果
            for i, (path, vec) in enumerate(zip(valid_paths, feat_256)):
                l1_idx = l1_sims[i].argmax().item()
                l1_conf = l1_sims[i][l1_idx].item()

                topk = tag_sims[i].topk(min(8, len(tag_names)))
                tags = [tag_names[j] for j, s in zip(topk.indices.tolist(), topk.values.tolist()) if s > 0.2]
                tag_ids = [hash(t) % 4096 for t in tags]

                # image_pk from path
                fname = os.path.basename(path).split(".")[0]
                record = {
                    "image_pk": fname.ljust(32, "0")[:32],
                    "local_path": path,
                    "global_vec": vec.tolist(),
                    "category_l1_id": l1_codes[l1_idx],
                    "category_l1": l1_names[l1_idx],
                    "category_l1_conf": round(l1_conf, 4),
                    "category_l2_id": 0,
                    "category_l3_id": 0,
                    "tags": tag_ids,
                    "tags_name": tags,
                }
                out_f.write(json.dumps(record, ensure_ascii=False) + "\n")

            done += len(valid_paths)
            batch_num = bi // GPU_BATCH_SIZE + 1
            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0

            if batch_num % 10 == 0 or batch_num == total_batches:
                log.info(f"[GPU{gpu_id}] Batch {batch_num}/{total_batches}, {done} done, {rate:.0f} img/s")

            # 进度文件
            try:
                with open(progress_file, "w") as pf:
                    json.dump({"gpu": gpu_id, "done": done, "total": total, "rate": round(rate, 1)}, pf)
            except Exception:
                pass

    elapsed = time.time() - t0
    log.info(f"[GPU{gpu_id}] DONE: {done}/{total} in {elapsed:.1f}s ({done/elapsed:.0f} img/s)")


if __name__ == "__main__":
    main()
