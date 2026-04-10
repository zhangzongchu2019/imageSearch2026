#!/usr/bin/env python3
"""
SSCD ResNet50 推理 — 副本检测向量 (512d)
TorchScript 模型, 极速 (~800 img/s per GPU)
"""
import argparse, os, json, time, queue, threading
import numpy as np
import torch
from PIL import Image
from concurrent.futures import ThreadPoolExecutor

# SSCD 用 ImageNet 均值/标准差
SSCD_MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32)
SSCD_STD = np.array([0.229, 0.224, 0.225], dtype=np.float32)

def fast_preprocess(img):
    if img.size != (224, 224):
        img = img.resize((224, 224), Image.BICUBIC)
    arr = np.array(img, dtype=np.float32) * (1.0 / 255.0)
    return ((arr - SSCD_MEAN) / SSCD_STD).transpose(2, 0, 1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--gpu", type=int, default=0)
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--total-shards", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--model-path", default="/data/imgsrch/models/sscd_resnet50.pt")
    args = parser.parse_args()

    os.environ["CUDA_VISIBLE_DEVICES"] = str(args.gpu)
    device = torch.device("cuda")
    print(f"[GPU {args.gpu}] shard {args.shard}/{args.total_shards}", flush=True)

    t0 = time.time()
    model = torch.jit.load(args.model_path).to(device).eval()
    print(f"[GPU {args.gpu}] SSCD loaded ({time.time()-t0:.1f}s)", flush=True)

    all_files = sorted(os.listdir(args.image_dir))
    jpg_files = [f for f in all_files if f.endswith(".jpg")]
    shard_files = [f for i, f in enumerate(jpg_files) if i % args.total_shards == args.shard]
    print(f"[GPU {args.gpu}] 总: {len(jpg_files):,}, 分片: {len(shard_files):,}", flush=True)

    BS = args.batch_size
    batch_queue = queue.Queue(maxsize=4)
    producer_done = threading.Event()

    def load_one(fname):
        try:
            img = Image.open(os.path.join(args.image_dir, fname)).convert("RGB")
            return (fname.replace(".jpg", ""), fast_preprocess(img))
        except: return None

    def producer():
        pool = ThreadPoolExecutor(max_workers=args.workers)
        batch = []
        for fname in shard_files:
            batch.append(fname)
            if len(batch) >= BS:
                results = list(pool.map(load_one, batch))
                pks = [r[0] for r in results if r]
                arrs = [r[1] for r in results if r]
                if arrs: batch_queue.put((pks, np.stack(arrs)))
                batch = []
        if batch:
            results = list(pool.map(load_one, batch))
            pks = [r[0] for r in results if r]
            arrs = [r[1] for r in results if r]
            if arrs: batch_queue.put((pks, np.stack(arrs)))
        pool.shutdown(wait=True)
        producer_done.set()

    threading.Thread(target=producer, daemon=True).start()

    out = open(args.output, "w")
    ok = processed = 0
    t_start = time.time()

    while True:
        try:
            pks, batch_arr = batch_queue.get(timeout=2)
        except queue.Empty:
            if producer_done.is_set() and batch_queue.empty(): break
            continue

        tensor = torch.from_numpy(batch_arr).to(device)
        with torch.no_grad():
            feat = model(tensor)
            feat = feat / feat.norm(dim=-1, keepdim=True)
        vecs = feat.cpu().numpy()

        for j in range(len(pks)):
            out.write(json.dumps({"image_pk": pks[j], "sscd_vec": vecs[j].tolist()}) + "\n")
            ok += 1

        processed += len(pks)
        if ok % 10000 == 0 and ok > 0:
            elapsed = time.time() - t_start
            rate = ok / elapsed
            eta = (len(shard_files) - processed) / rate / 60 if rate > 0 else 0
            print(f"[GPU {args.gpu}] {processed:,}/{len(shard_files):,} ({rate:.0f}/s) ETA={eta:.0f}min", flush=True)
            out.flush()

    out.close()
    elapsed = time.time() - t_start
    print(f"[GPU {args.gpu}] 完成: {ok:,} ok, {elapsed:.0f}s ({ok/max(elapsed,1):.0f}/s)", flush=True)

if __name__ == "__main__":
    main()
