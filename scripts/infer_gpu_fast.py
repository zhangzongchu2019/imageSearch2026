#!/usr/bin/env python3
"""
GPU 推理 v3 — 多线程 preprocess + GPU 流水线
  v1: 单线程 preprocess → 4/s per GPU (preprocess瓶颈)
  v3: 8线程 preprocess → 400/s per GPU

用法:
  python3 infer_gpu_fast.py --image-dir /data/imgsrch/dl_batch1 --output /data/infer.jsonl --gpu 0 --shard 0 --total-shards 4
  # 或 SQLite 模式:
  python3 infer_gpu_fast.py --db /data/imgsrch/batch1.db --output /data/infer.jsonl --gpu 0 --shard 0 --total-shards 4
"""
import argparse, os, json, time, glob, sys, sqlite3, queue, threading
import numpy as np
import torch
from PIL import Image
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

# 自定义快速预处理 (numpy, 跳过 torchvision 开销)
# 图片已经是 224x224 (CDN 缩放), 直接 normalize
CLIP_MEAN = np.array([0.48145466, 0.4578275, 0.40821073], dtype=np.float32)
CLIP_STD = np.array([0.26862954, 0.26130258, 0.27577711], dtype=np.float32)

def fast_preprocess(img):
    """PIL Image → normalized CHW float32 tensor (2x faster than torchvision)"""
    # 确保 224x224 (大部分已经是, 少数需要 resize)
    if img.size != (224, 224):
        img = img.resize((224, 224), Image.BICUBIC)
    arr = np.array(img, dtype=np.float32) * (1.0 / 255.0)
    arr = (arr - CLIP_MEAN) / CLIP_STD
    return torch.from_numpy(arr.transpose(2, 0, 1))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-dir", default=None)
    parser.add_argument("--db", default=None, help="SQLite 数据库路径 (可选, 优先)")
    parser.add_argument("--output", required=True)
    parser.add_argument("--gpu", type=int, default=0)
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--total-shards", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    if not args.image_dir and not args.db:
        print("ERROR: 必须指定 --image-dir 或 --db"); sys.exit(1)

    os.environ["CUDA_VISIBLE_DEVICES"] = str(args.gpu)
    device = torch.device("cuda")

    print(f"[GPU {args.gpu}] shard {args.shard}/{args.total_shards} batch={args.batch_size} workers={args.workers}", flush=True)

    # === 加载模型 ===
    import open_clip
    t0 = time.time()
    model, _, _default_preprocess = open_clip.create_model_and_transforms(
        "ViT-L-14", pretrained="/data/imgsrch/models/ViT-L-14.pt")
    model = model.to(device).half().eval()
    print(f"[GPU {args.gpu}] ViT-L-14 loaded ({time.time()-t0:.1f}s)", flush=True)

    # 投影矩阵 768→256
    rng = np.random.RandomState(42)
    proj = rng.randn(768, 256).astype(np.float32)
    u, _, _ = np.linalg.svd(proj, full_matrices=False)
    projection = torch.from_numpy(u).to(device).half()

    # L1/L2 分类
    sys.path.insert(0, "/workspace/services/inference-service")
    from app.taxonomy import L1_CODES, L1_CODE_TO_NAME, FASHION_L2, NONFASHION_L2

    tokenizer = open_clip.get_tokenizer("ViT-L-14")
    l1_prompts = [f"a photo of {L1_CODE_TO_NAME[c]}" for c in L1_CODES]
    l1_tokens = tokenizer(l1_prompts).to(device)
    with torch.no_grad():
        l1_text_feat = model.encode_text(l1_tokens).half()
        l1_text_feat = l1_text_feat / l1_text_feat.norm(dim=-1, keepdim=True)

    l2_info = {}
    for l1_code, l2_dict in {**FASHION_L2, **NONFASHION_L2}.items():
        codes = sorted(l2_dict.keys())
        prompts = [f"a photo of {l2_dict[c]['name']}" for c in codes]
        tokens = tokenizer(prompts).to(device)
        with torch.no_grad():
            feat = model.encode_text(tokens).half()
            feat = feat / feat.norm(dim=-1, keepdim=True)
        l2_info[l1_code] = {"codes": codes, "features": feat}

    # === 断点恢复 ===
    done_pks = set()
    if args.resume and os.path.exists(args.output):
        with open(args.output) as f:
            for line in f:
                try: done_pks.add(json.loads(line)["image_pk"])
                except: pass
        print(f"[GPU {args.gpu}] 断点恢复: {len(done_pks):,}", flush=True)

    # === 构建数据源 ===
    if args.db:
        # SQLite 模式
        conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
        total = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
        my_count = (total + args.total_shards - 1) // args.total_shards
        offset = args.shard * my_count
        print(f"[GPU {args.gpu}] SQLite: {total:,} 总图, 本分片: {my_count:,}", flush=True)

        def iter_images():
            cursor = conn.execute("SELECT pk, data FROM images LIMIT ? OFFSET ?", (my_count, offset))
            for pk, data in cursor:
                if pk in done_pks: continue
                yield pk, data
    else:
        # 文件模式
        files = sorted(os.listdir(args.image_dir))
        jpg_files = [f for f in files if f.endswith(".jpg")]
        shard_files = [f for i, f in enumerate(jpg_files) if i % args.total_shards == args.shard]
        print(f"[GPU {args.gpu}] 文件: {len(jpg_files):,} 总, 本分片: {len(shard_files):,}", flush=True)
        my_count = len(shard_files)

        def iter_images():
            for fname in shard_files:
                pk = fname.replace(".jpg", "")
                if pk in done_pks: continue
                with open(os.path.join(args.image_dir, fname), "rb") as f:
                    yield pk, f.read()

    # === 多线程预处理 → GPU 推理流水线 ===
    BS = args.batch_size
    # 预处理好的 batch 放入队列, GPU 消费
    batch_queue = queue.Queue(maxsize=4)  # 缓冲 4 个 batch
    producer_done = threading.Event()

    def preprocess_one(item):
        pk, data = item
        try:
            img = Image.open(BytesIO(data)).convert("RGB")
            tensor = _default_preprocess(img)
            return pk, tensor
        except:
            return pk, None

    def producer():
        """多线程预处理, 攒够一个batch放入队列"""
        pool = ThreadPoolExecutor(max_workers=args.workers)
        batch_items = []

        for pk, data in iter_images():
            batch_items.append((pk, data))
            if len(batch_items) >= BS:
                # 并行预处理
                results = list(pool.map(preprocess_one, batch_items))
                pks = []
                tensors = []
                for r_pk, r_tensor in results:
                    if r_tensor is not None:
                        pks.append(r_pk)
                        tensors.append(r_tensor)
                if tensors:
                    batch_queue.put((pks, torch.stack(tensors)))
                batch_items = []

        # 最后不足一个 batch 的
        if batch_items:
            results = list(pool.map(preprocess_one, batch_items))
            pks = []
            tensors = []
            for r_pk, r_tensor in results:
                if r_tensor is not None:
                    pks.append(r_pk)
                    tensors.append(r_tensor)
            if tensors:
                batch_queue.put((pks, torch.stack(tensors)))

        pool.shutdown(wait=True)
        producer_done.set()

    # 启动生产者线程
    t_producer = threading.Thread(target=producer, daemon=True)
    t_producer.start()

    # === GPU 消费者: 从队列取batch, 推理, 写出 ===
    out = open(args.output, "a" if args.resume else "w")
    processed = ok = fail = 0
    t_start = time.time()

    while True:
        try:
            pks, batch_tensor = batch_queue.get(timeout=2)
        except queue.Empty:
            if producer_done.is_set() and batch_queue.empty():
                break
            continue

        batch_tensor = batch_tensor.to(device).half()

        with torch.no_grad():
            image_features = model.encode_image(batch_tensor)
            image_features = image_features / image_features.norm(dim=-1, keepdim=True)
            vec_256 = (image_features @ projection).float()
            vec_256 = vec_256 / vec_256.norm(dim=-1, keepdim=True)
            sim_l1 = (image_features @ l1_text_feat.T).float()
            l1_idx = sim_l1.argmax(dim=-1)

            # 批量 L2 分类: 按 L1 分组
            l2_codes = [0] * len(pks)
            for l1_code_val in l2_info:
                mask = (l1_idx == L1_CODES.index(l1_code_val)) if l1_code_val in L1_CODES else None
                if mask is None: continue
                indices = mask.nonzero(as_tuple=True)[0]
                if len(indices) == 0: continue
                sub_feat = image_features[indices]
                sim_l2 = (sub_feat @ l2_info[l1_code_val]["features"].T).float()
                l2_idx = sim_l2.argmax(dim=-1)
                for k, idx in enumerate(indices):
                    l2_codes[idx.item()] = l2_info[l1_code_val]["codes"][l2_idx[k].item()]

        for j in range(len(pks)):
            record = {
                "image_pk": pks[j],
                "global_vec": vec_256[j].cpu().numpy().tolist(),
                "category_l1": L1_CODES[l1_idx[j].item()],
                "category_l2": l2_codes[j],
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

        processed += len(pks)
        if ok % 5000 == 0 and ok > 0:
            elapsed = time.time() - t_start
            rate = ok / elapsed
            eta = (my_count - processed) / rate / 60 if rate > 0 else 0
            print(f"[GPU {args.gpu}] {processed:,}/{my_count:,} ({rate:.0f}/s) ok={ok:,} ETA={eta:.0f}min", flush=True)
            out.flush()

    t_producer.join(timeout=5)
    out.close()
    if args.db:
        conn.close()

    elapsed = time.time() - t_start
    print(f"[GPU {args.gpu}] 完成: {ok:,} ok, {elapsed:.0f}s ({ok/max(elapsed,1):.0f}/s)", flush=True)


if __name__ == "__main__":
    main()
