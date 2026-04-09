#!/usr/bin/env python3
"""
DINOv2-L 快速推理 (基于 infer_gpu_fast.py 改造)
- 模型: /data/imgsrch/models/dinov2-large
- 输出: 1024d float32 (CLS token, L2 normalized)
- 速度: ~300 img/s per GPU (fp16, 16 worker preprocess)
- 输出格式: jsonl, 每行 {image_pk, dinov2_vec}

用法:
  python3 infer_dinov2_fast.py --image-dir /data/test-images --output /data/out.jsonl --gpu 0 --shard 0 --total-shards 4
"""
import argparse, os, json, time, sys, queue, threading
import numpy as np
import torch
from PIL import Image
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

# DINOv2 自定义快速预处理 (numpy, 比 transformers AutoImageProcessor 快 10 倍)
DINOV2_MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32)
DINOV2_STD = np.array([0.229, 0.224, 0.225], dtype=np.float32)

def fast_preprocess(img):
    """PIL Image → CHW float32 numpy array (跳过 transformers 慢路径)"""
    if img.size != (224, 224):
        img = img.resize((224, 224), Image.BICUBIC)
    arr = np.array(img, dtype=np.float32) * (1.0 / 255.0)
    arr = (arr - DINOV2_MEAN) / DINOV2_STD
    return arr.transpose(2, 0, 1)  # CHW


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--gpu", type=int, default=0)
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--total-shards", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--model-path", default="/data/imgsrch/models/dinov2-large")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    os.environ["CUDA_VISIBLE_DEVICES"] = str(args.gpu)
    device = torch.device("cuda")

    print(f"[GPU {args.gpu}] shard {args.shard}/{args.total_shards} batch={args.batch_size} workers={args.workers}", flush=True)

    # === 加载 DINOv2-L (fp16, 跳过 processor 用 numpy 自定义) ===
    from transformers import AutoModel
    t0 = time.time()
    model = AutoModel.from_pretrained(args.model_path, dtype=torch.float16)
    model = model.to(device).eval()
    print(f"[GPU {args.gpu}] DINOv2-L loaded ({time.time()-t0:.1f}s)", flush=True)

    # === 断点恢复 ===
    done_pks = set()
    if args.resume and os.path.exists(args.output):
        with open(args.output) as f:
            for line in f:
                try:
                    done_pks.add(json.loads(line)["image_pk"])
                except:
                    pass
        print(f"[GPU {args.gpu}] 断点恢复: {len(done_pks):,}", flush=True)

    # === 文件列表 + 分片 ===
    all_files = sorted(os.listdir(args.image_dir))
    jpg_files = [f for f in all_files if f.endswith(".jpg")]
    shard_files = [f for i, f in enumerate(jpg_files) if i % args.total_shards == args.shard]
    print(f"[GPU {args.gpu}] 总文件: {len(jpg_files):,}, 本分片: {len(shard_files):,}", flush=True)

    # === 多线程图片加载 + GPU 流水线 ===
    BS = args.batch_size
    batch_queue = queue.Queue(maxsize=4)
    producer_done = threading.Event()

    def load_and_preprocess(fname):
        """加载图片并立即 preprocess (返回 numpy array)"""
        pk = fname.replace(".jpg", "")
        if pk in done_pks:
            return None
        try:
            img = Image.open(os.path.join(args.image_dir, fname)).convert("RGB")
            arr = fast_preprocess(img)
            return (pk, arr)
        except:
            return None

    def producer():
        pool = ThreadPoolExecutor(max_workers=args.workers)
        batch_items = []

        def flush_batch():
            results = list(pool.map(load_and_preprocess, batch_items))
            pks = []
            arrs = []
            for r in results:
                if r is not None:
                    pks.append(r[0])
                    arrs.append(r[1])
            if arrs:
                batch_tensor = np.stack(arrs)  # (N, 3, 224, 224)
                batch_queue.put((pks, batch_tensor))

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
            pks, batch_arr = batch_queue.get(timeout=2)
        except queue.Empty:
            if producer_done.is_set() and batch_queue.empty():
                break
            continue

        # 推理 (preprocess 已在 producer 完成)
        try:
            batch_tensor = torch.from_numpy(batch_arr).to(device).to(torch.float16)
            with torch.no_grad():
                outputs = model(pixel_values=batch_tensor)
                cls = outputs.last_hidden_state[:, 0, :]  # (N, 1024)
                cls = cls / cls.norm(dim=-1, keepdim=True)  # L2 normalize
                cls_np = cls.cpu().float().numpy()
        except Exception as e:
            print(f"[GPU {args.gpu}] batch error: {e}", flush=True)
            continue

        for j in range(len(pks)):
            record = {
                "image_pk": pks[j],
                "dinov2_vec": cls_np[j].tolist(),
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
