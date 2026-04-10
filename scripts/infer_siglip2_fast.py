#!/usr/bin/env python3
"""SigLIP 2 Base vision 推理 (768d)"""
import argparse, os, json, time, queue, threading
import numpy as np
import torch
from PIL import Image
from concurrent.futures import ThreadPoolExecutor

SIGLIP_MEAN = np.array([0.5, 0.5, 0.5], dtype=np.float32)
SIGLIP_STD = np.array([0.5, 0.5, 0.5], dtype=np.float32)

def fast_preprocess(img):
    if img.size != (224, 224):
        img = img.resize((224, 224), Image.BICUBIC)
    arr = np.array(img, dtype=np.float32) * (1.0 / 255.0)
    return ((arr - SIGLIP_MEAN) / SIGLIP_STD).transpose(2, 0, 1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--gpu", type=int, default=0)
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--total-shards", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()

    os.environ["CUDA_VISIBLE_DEVICES"] = str(args.gpu)
    device = torch.device("cuda")
    print(f"[GPU {args.gpu}] shard {args.shard}/{args.total_shards}", flush=True)

    from transformers import AutoModel
    t0 = time.time()
    full = AutoModel.from_pretrained("/data/imgsrch/models/siglip2-base", dtype=torch.float16)
    model = full.vision_model.to(device).eval()
    del full
    print(f"[GPU {args.gpu}] SigLIP 2 loaded ({time.time()-t0:.1f}s)", flush=True)

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

        tensor = torch.from_numpy(batch_arr).to(device).to(torch.float16)
        with torch.no_grad():
            o = model(pixel_values=tensor)
            feat = o.pooler_output
            feat = feat / feat.norm(dim=-1, keepdim=True)
        vecs = feat.cpu().float().numpy()

        for j in range(len(pks)):
            out.write(json.dumps({"image_pk": pks[j], "siglip2_vec": vecs[j].tolist()}) + "\n")
            ok += 1

        processed += len(pks)
        if ok % 5000 == 0 and ok > 0:
            elapsed = time.time() - t_start
            rate = ok / elapsed
            print(f"[GPU {args.gpu}] {processed:,}/{len(shard_files):,} ({rate:.0f}/s)", flush=True)
            out.flush()

    out.close()
    elapsed = time.time() - t_start
    print(f"[GPU {args.gpu}] 完成: {ok:,}, {elapsed:.0f}s ({ok/max(elapsed,1):.0f}/s)", flush=True)

if __name__ == "__main__":
    main()
