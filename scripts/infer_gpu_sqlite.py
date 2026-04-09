#!/usr/bin/env python3
"""
GPU 直接推理 v2 — 从 SQLite 读取图片 (解决文件IO瓶颈)

对比:
  v1 (文件): glob 998K → open/read/close → ~30/s (4GPU)
  v2 (SQLite): SELECT 顺序读 → 预计 500+/s (4GPU)

用法:
  python3 infer_gpu_sqlite.py --db /data/imgsrch/dl_batch2.db --output /data/infer.jsonl --gpu 0 --shard 0 --total-shards 4
"""
import argparse, os, json, time, sys, sqlite3
import numpy as np
import torch
from PIL import Image
from io import BytesIO


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, help="SQLite 数据库路径")
    parser.add_argument("--output", required=True)
    parser.add_argument("--gpu", type=int, default=0)
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--total-shards", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--resume", action="store_true", help="从上次断点继续")
    args = parser.parse_args()

    os.environ["CUDA_VISIBLE_DEVICES"] = str(args.gpu)
    device = torch.device("cuda")

    print(f"[GPU {args.gpu}] shard {args.shard}/{args.total_shards} batch={args.batch_size} db={args.db}", flush=True)

    # === 加载模型 ===
    import open_clip
    t0 = time.time()
    model, _, preprocess = open_clip.create_model_and_transforms(
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
    from app.taxonomy import L1_CATEGORIES, L1_CODES, L1_CODE_TO_NAME
    from app.taxonomy import FASHION_L2, NONFASHION_L2

    tokenizer = open_clip.get_tokenizer("ViT-L-14")
    l1_names = [L1_CODE_TO_NAME[c] for c in L1_CODES]
    l1_prompts = [f"a photo of {name}" for name in l1_names]
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

    # === 断点恢复: 读取已完成的 pk ===
    done_pks = set()
    if args.resume and os.path.exists(args.output):
        with open(args.output) as f:
            for line in f:
                try:
                    done_pks.add(json.loads(line)["image_pk"])
                except:
                    pass
        print(f"[GPU {args.gpu}] 断点恢复: 已完成 {len(done_pks):,}", flush=True)

    # === 从 SQLite 读取图片 (分片) ===
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    conn.execute("PRAGMA cache_size=-262144")  # 256MB cache
    total = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
    my_count = (total + args.total_shards - 1) // args.total_shards
    offset = args.shard * my_count
    print(f"[GPU {args.gpu}] 总图: {total:,}, 本分片: {my_count:,} (offset={offset:,})", flush=True)

    # === 批量推理 ===
    out = open(args.output, "a" if args.resume else "w")
    processed = ok = fail = skipped = 0
    t_start = time.time()
    BS = args.batch_size

    # 用 cursor 流式读取, 避免全量加载到内存
    cursor = conn.execute(
        "SELECT pk, data FROM images LIMIT ? OFFSET ?",
        (my_count, offset))

    batch_pks = []
    batch_tensors = []

    for pk, data in cursor:
        processed += 1

        if pk in done_pks:
            skipped += 1
            continue

        try:
            img = Image.open(BytesIO(data)).convert("RGB")
            batch_tensors.append(preprocess(img))
            batch_pks.append(pk)
        except:
            fail += 1
            continue

        # 攒够一个 batch
        if len(batch_pks) < BS:
            continue

        # --- 推理一个 batch ---
        tensor = torch.stack(batch_tensors).to(device).half()
        with torch.no_grad():
            image_features = model.encode_image(tensor)
            image_features = image_features / image_features.norm(dim=-1, keepdim=True)
            vec_256 = (image_features @ projection).float()
            vec_256 = vec_256 / vec_256.norm(dim=-1, keepdim=True)
            sim_l1 = (image_features @ l1_text_feat.T).float()
            l1_idx = sim_l1.argmax(dim=-1)

        for j in range(len(batch_pks)):
            l1_code = L1_CODES[l1_idx[j].item()]
            l2_code = 0
            if l1_code in l2_info:
                with torch.no_grad():
                    sim_l2 = (image_features[j:j+1] @ l2_info[l1_code]["features"].T).float()
                    l2_code = l2_info[l1_code]["codes"][sim_l2.argmax(dim=-1).item()]

            record = {
                "image_pk": batch_pks[j],
                "global_vec": vec_256[j].cpu().numpy().tolist(),
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

        batch_pks = []
        batch_tensors = []

        if ok % 5000 == 0:
            elapsed = time.time() - t_start
            rate = ok / elapsed
            eta = (my_count - processed) / rate / 60 if rate > 0 else 0
            print(f"[GPU {args.gpu}] {processed:,}/{my_count:,} ({rate:.0f}/s) ok={ok:,} fail={fail:,} skip={skipped:,} ETA={eta:.0f}min", flush=True)
            out.flush()

    # 处理最后不满一个 batch 的
    if batch_pks:
        tensor = torch.stack(batch_tensors).to(device).half()
        with torch.no_grad():
            image_features = model.encode_image(tensor)
            image_features = image_features / image_features.norm(dim=-1, keepdim=True)
            vec_256 = (image_features @ projection).float()
            vec_256 = vec_256 / vec_256.norm(dim=-1, keepdim=True)
            sim_l1 = (image_features @ l1_text_feat.T).float()
            l1_idx = sim_l1.argmax(dim=-1)

        for j in range(len(batch_pks)):
            l1_code = L1_CODES[l1_idx[j].item()]
            l2_code = 0
            if l1_code in l2_info:
                with torch.no_grad():
                    sim_l2 = (image_features[j:j+1] @ l2_info[l1_code]["features"].T).float()
                    l2_code = l2_info[l1_code]["codes"][sim_l2.argmax(dim=-1).item()]

            record = {
                "image_pk": batch_pks[j],
                "global_vec": vec_256[j].cpu().numpy().tolist(),
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

    out.close()
    conn.close()
    elapsed = time.time() - t_start
    print(f"[GPU {args.gpu}] 完成: {ok:,} ok, {fail:,} fail, {skipped:,} skip, {elapsed:.0f}s ({ok/max(elapsed,1):.0f}/s)", flush=True)


if __name__ == "__main__":
    main()
