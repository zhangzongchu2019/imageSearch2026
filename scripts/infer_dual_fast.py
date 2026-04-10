#!/usr/bin/env python3
"""
H4 双模型推理脚本
- 模型1: DINOv2-L (1024d, fp16) - 实例/形变/同款
- 模型2: CLIP ViT-L-14 (768d, fp16) - 语义相似 + zero-shot 分类标签

输出 jsonl: {image_pk, dinov2_vec, clip_vec, category_l1, category_l2}

性能预期 (单 GPU):
- DINOv2 单跑: 520 img/s
- CLIP 单跑: ~600 img/s
- 双模型并跑 (共享 preprocess): ~280 img/s
- 4 GPU 总速率: ~1100 img/s

用法:
  python3 infer_dual_fast.py --image-dir /data/test-images --output /data/out.jsonl --gpu 0 --shard 0 --total-shards 4
"""
import argparse
import os
import json
import time
import sys
import queue
import threading
import numpy as np
import torch
from PIL import Image
from concurrent.futures import ThreadPoolExecutor


# DINOv2 预处理常量 (ImageNet)
DINOV2_MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32)
DINOV2_STD = np.array([0.229, 0.224, 0.225], dtype=np.float32)

# CLIP 预处理常量 (OpenAI CLIP, 与 DINOv2 不同!)
CLIP_MEAN = np.array([0.48145466, 0.4578275, 0.40821073], dtype=np.float32)
CLIP_STD = np.array([0.26862954, 0.26130258, 0.27577711], dtype=np.float32)


def preprocess_dual(img_pil):
    """同时为 DINOv2 和 CLIP 准备 224x224 输入
    返回 (dinov2_arr, clip_arr) 都是 (3, 224, 224) float32
    """
    if img_pil.size != (224, 224):
        img_pil = img_pil.resize((224, 224), Image.BICUBIC)
    arr = np.array(img_pil, dtype=np.float32) * (1.0 / 255.0)  # (224, 224, 3)

    # DINOv2
    dino = (arr - DINOV2_MEAN) / DINOV2_STD
    dino = dino.transpose(2, 0, 1)  # CHW

    # CLIP (不同的 mean/std)
    clip = (arr - CLIP_MEAN) / CLIP_STD
    clip = clip.transpose(2, 0, 1)

    return dino, clip


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--gpu", type=int, default=0)
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--total-shards", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--dinov2-path", default="/data/imgsrch/models/dinov2-large")
    parser.add_argument("--clip-path", default="/data/imgsrch/models/ViT-L-14.pt")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    os.environ["CUDA_VISIBLE_DEVICES"] = str(args.gpu)
    device = torch.device("cuda")

    print(f"[GPU {args.gpu}] shard {args.shard}/{args.total_shards} batch={args.batch_size}", flush=True)

    # === 加载 DINOv2-L (fp16) ===
    from transformers import AutoModel
    t0 = time.time()
    dinov2 = AutoModel.from_pretrained(args.dinov2_path, dtype=torch.float16).to(device).eval()
    print(f"[GPU {args.gpu}] DINOv2-L loaded ({time.time()-t0:.1f}s)", flush=True)

    # === 加载 CLIP ViT-L-14 (fp16) ===
    import open_clip
    t0 = time.time()
    clip_model, _, _ = open_clip.create_model_and_transforms(
        "ViT-L-14", pretrained=args.clip_path
    )
    clip_model = clip_model.visual.to(device).half().eval()  # 只用 visual encoder
    print(f"[GPU {args.gpu}] CLIP ViT-L-14 loaded ({time.time()-t0:.1f}s)", flush=True)

    # === 加载 L1 zero-shot 分类用的文本特征 (CLIP text encoder) ===
    sys.path.insert(0, "/workspace/services/inference-service")
    try:
        from app.taxonomy import L1_CODES, L1_CODE_TO_NAME, FASHION_L2, NONFASHION_L2
        # 重新加载完整 CLIP (含 text encoder)
        clip_full, _, _ = open_clip.create_model_and_transforms(
            "ViT-L-14", pretrained=args.clip_path
        )
        clip_full = clip_full.to(device).half().eval()
        tokenizer = open_clip.get_tokenizer("ViT-L-14")
        l1_prompts = [f"a photo of {L1_CODE_TO_NAME[c]}" for c in L1_CODES]
        l1_tokens = tokenizer(l1_prompts).to(device)
        with torch.no_grad():
            l1_text_feat = clip_full.encode_text(l1_tokens).half()
            l1_text_feat = l1_text_feat / l1_text_feat.norm(dim=-1, keepdim=True)

        # L2 分组
        l2_info = {}
        for l1_code, l2_dict in {**FASHION_L2, **NONFASHION_L2}.items():
            codes = sorted(l2_dict.keys())
            prompts = [f"a photo of {l2_dict[c]['name']}" for c in codes]
            tokens = tokenizer(prompts).to(device)
            with torch.no_grad():
                feat = clip_full.encode_text(tokens).half()
                feat = feat / feat.norm(dim=-1, keepdim=True)
            l2_info[l1_code] = {"codes": codes, "features": feat}

        del clip_full  # 释放内存, text 特征已缓存
        torch.cuda.empty_cache()
        print(f"[GPU {args.gpu}] L1 ({len(L1_CODES)} 类) + L2 ({len(l2_info)} 组) 文本特征已加载", flush=True)
        has_taxonomy = True
    except Exception as e:
        print(f"[GPU {args.gpu}] taxonomy 加载失败 (跳过分类): {e}", flush=True)
        has_taxonomy = False
        L1_CODES = []
        l1_text_feat = None
        l2_info = {}

    # === 断点恢复 ===
    done_pks = set()
    if args.resume and os.path.exists(args.output):
        with open(args.output) as f:
            for line in f:
                try: done_pks.add(json.loads(line)["image_pk"])
                except: pass
        print(f"[GPU {args.gpu}] 断点恢复: {len(done_pks):,}", flush=True)

    # === 文件分片 ===
    all_files = sorted(os.listdir(args.image_dir))
    jpg_files = [f for f in all_files if f.endswith(".jpg")]
    shard_files = [f for i, f in enumerate(jpg_files) if i % args.total_shards == args.shard]
    print(f"[GPU {args.gpu}] 总文件: {len(jpg_files):,}, 本分片: {len(shard_files):,}", flush=True)

    # === 多线程预处理 → GPU 流水线 ===
    BS = args.batch_size
    batch_queue = queue.Queue(maxsize=4)
    producer_done = threading.Event()

    def load_and_preprocess(fname):
        pk = fname.replace(".jpg", "")
        if pk in done_pks:
            return None
        try:
            img = Image.open(os.path.join(args.image_dir, fname)).convert("RGB")
            dino_arr, clip_arr = preprocess_dual(img)
            return (pk, dino_arr, clip_arr)
        except:
            return None

    def producer():
        pool = ThreadPoolExecutor(max_workers=args.workers)
        batch_items = []

        def flush_batch():
            results = list(pool.map(load_and_preprocess, batch_items))
            pks = []
            dino_arrs = []
            clip_arrs = []
            for r in results:
                if r is not None:
                    pks.append(r[0])
                    dino_arrs.append(r[1])
                    clip_arrs.append(r[2])
            if dino_arrs:
                batch_queue.put((
                    pks,
                    np.stack(dino_arrs),
                    np.stack(clip_arrs),
                ))

        for fname in shard_files:
            batch_items.append(fname)
            if len(batch_items) >= BS:
                flush_batch()
                batch_items = []

        if batch_items:
            flush_batch()

        pool.shutdown(wait=True)
        producer_done.set()

    t_producer = threading.Thread(target=producer, daemon=True)
    t_producer.start()

    # === GPU 消费 ===
    out = open(args.output, "a" if args.resume else "w")
    processed = ok = 0
    t_start = time.time()

    while True:
        try:
            pks, dino_batch, clip_batch = batch_queue.get(timeout=2)
        except queue.Empty:
            if producer_done.is_set() and batch_queue.empty():
                break
            continue

        try:
            # DINOv2 推理
            dino_tensor = torch.from_numpy(dino_batch).to(device).to(torch.float16)
            with torch.no_grad():
                dino_out = dinov2(pixel_values=dino_tensor)
                dino_cls = dino_out.last_hidden_state[:, 0, :]  # (N, 1024)
                dino_cls = dino_cls / dino_cls.norm(dim=-1, keepdim=True)
            dino_vecs = dino_cls.cpu().float().numpy()

            # CLIP 推理 (复用 visual encoder)
            clip_tensor = torch.from_numpy(clip_batch).to(device).to(torch.float16)
            with torch.no_grad():
                clip_feat = clip_model(clip_tensor)  # (N, 768)
                clip_feat_normed = clip_feat / clip_feat.norm(dim=-1, keepdim=True)
            clip_vecs = clip_feat_normed.cpu().float().numpy()

            # zero-shot 分类 (用 normed CLIP 特征)
            l1_codes_list = [0] * len(pks)
            l2_codes_list = [0] * len(pks)
            if has_taxonomy and l1_text_feat is not None:
                with torch.no_grad():
                    sim_l1 = (clip_feat_normed.half() @ l1_text_feat.T).float()
                    l1_idx = sim_l1.argmax(dim=-1)
                for j in range(len(pks)):
                    l1_code = L1_CODES[l1_idx[j].item()]
                    l1_codes_list[j] = l1_code
                    if l1_code in l2_info:
                        with torch.no_grad():
                            sim_l2 = (clip_feat_normed[j:j+1].half() @ l2_info[l1_code]["features"].T).float()
                            l2_codes_list[j] = l2_info[l1_code]["codes"][sim_l2.argmax(dim=-1).item()]
        except Exception as e:
            print(f"[GPU {args.gpu}] batch error: {e}", flush=True)
            continue

        for j in range(len(pks)):
            record = {
                "image_pk": pks[j],
                "dinov2_vec": dino_vecs[j].tolist(),
                "clip_vec": clip_vecs[j].tolist(),
                "category_l1": l1_codes_list[j],
                "category_l2": l2_codes_list[j],
            }
            out.write(json.dumps(record) + "\n")
            ok += 1

        processed += len(pks)
        if ok % 5000 == 0 and ok > 0:
            elapsed = time.time() - t_start
            rate = ok / elapsed
            eta = (len(shard_files) - processed) / rate / 60 if rate > 0 else 0
            print(f"[GPU {args.gpu}] {processed:,}/{len(shard_files):,} ({rate:.0f}/s) ok={ok:,} ETA={eta:.0f}min", flush=True)
            out.flush()

    t_producer.join(timeout=5)
    out.close()
    elapsed = time.time() - t_start
    print(f"[GPU {args.gpu}] 完成: {ok:,} ok, {elapsed:.0f}s ({ok/max(elapsed,1):.0f}/s)", flush=True)


if __name__ == "__main__":
    main()
