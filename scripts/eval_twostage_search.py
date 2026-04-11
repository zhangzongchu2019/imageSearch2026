#!/usr/bin/env python3
"""
两阶段搜索评测: Stage 1 (全图) + Stage 2 (多尺度 fallback)
验证 score < 0.8 时触发多尺度裁剪对图中图/反图中图的改善

在 gpu_worker 中运行
"""
import os, json, time
import numpy as np, torch
from PIL import Image
from pymilvus import connections, Collection

EVAL = "/data/imgsrch/eval_10scenes"
COLLECTION = "h4v03_main"
MILVUS = "imgsrch-mvs-proxy"

MEAN = np.array([0.485,0.456,0.406], dtype=np.float32)
STD = np.array([0.229,0.224,0.225], dtype=np.float32)

def pp(img):
    if img.size != (224,224): img = img.resize((224,224), Image.BICUBIC)
    return ((np.array(img, dtype=np.float32)/255.0 - MEAN) / STD).transpose(2,0,1)

def make_crops(img):
    """生成多尺度裁剪"""
    w, h = img.size
    crops = [
        img.crop((int(w*0.2), int(h*0.2), int(w*0.8), int(h*0.8))),  # 中心 60%
        img.crop((int(w*0.3), int(h*0.3), int(w*0.7), int(h*0.7))),  # 中心 40%
        img.crop((0, 0, w, int(h*0.5))),                               # 上半
        img.crop((0, int(h*0.5), w, h)),                               # 下半
        img.crop((int(w*0.1), int(h*0.1), int(w*0.9), int(h*0.5))),    # 上中
        img.crop((int(w*0.1), int(h*0.5), int(w*0.9), int(h*0.9))),    # 下中
    ]
    return crops

def main():
    connections.connect(host=MILVUS, port="19530")
    coll = Collection(COLLECTION)
    coll.load()
    print(f"Collection: {coll.num_entities:,}")

    sscd = torch.jit.load("/data/imgsrch/models/sscd_resnet50.pt").cuda().eval()
    params = {"metric_type":"COSINE", "params":{"ef":200}}

    with open(os.path.join(EVAL, "ground_truth.json")) as f:
        scenarios = json.load(f)["scenarios"]

    # 对每个场景跑三种模式
    print(f"\n{'场景':<22} {'Stage1':>8} {'+Stage2':>8} {'提升':>8}")
    print("-" * 52)

    for sc in scenarios:
        d = os.path.join(EVAL, sc)
        files = sorted(os.listdir(d))[:200]

        s1_r10 = 0  # Stage 1 only
        s2_r10 = 0  # Stage 1 + Stage 2 fallback
        fallback_count = 0

        for f in files:
            try:
                pk = f.replace(".jpg", "")
                img = Image.open(os.path.join(d, f)).convert("RGB")
            except:
                continue

            # Stage 1: 全图搜索
            arr = pp(img)
            tensor = torch.from_numpy(arr[np.newaxis]).cuda()
            with torch.no_grad():
                feat = sscd(tensor)
                feat = feat / feat.norm(dim=-1, keepdim=True)
            vec = feat[0].cpu().numpy()

            res1 = coll.search(data=[vec.tolist()], anns_field="sscd_vec",
                              param=params, limit=15, output_fields=["image_pk"])
            hits1 = [h.entity.get("image_pk") for h in res1[0]]
            top1_score = float(res1[0][0].score) if res1[0] else 0

            # Stage 1 结果
            if pk in hits1[:10]:
                s1_r10 += 1

            # Stage 2: 如果 top1_score < 0.8, 触发多尺度
            if top1_score < 0.8:
                fallback_count += 1
                crops = make_crops(img)
                all_hits = {h.entity.get("image_pk"): float(h.score) for h in res1[0]}

                # 批量推理所有裁剪
                crop_arrs = [pp(c) for c in crops]
                crop_tensor = torch.from_numpy(np.stack(crop_arrs)).cuda()
                with torch.no_grad():
                    crop_feats = sscd(crop_tensor)
                    crop_feats = crop_feats / crop_feats.norm(dim=-1, keepdim=True)
                crop_vecs = crop_feats.cpu().numpy()

                for cv in crop_vecs:
                    res_c = coll.search(data=[cv.tolist()], anns_field="sscd_vec",
                                       param=params, limit=15, output_fields=["image_pk"])
                    for h in res_c[0]:
                        hpk = h.entity.get("image_pk")
                        all_hits[hpk] = max(all_hits.get(hpk, 0), float(h.score))

                # 也搜 center_vec
                res_center = coll.search(data=[vec.tolist()], anns_field="center_vec",
                                        param=params, limit=15, output_fields=["image_pk"])
                for h in res_center[0]:
                    hpk = h.entity.get("image_pk")
                    all_hits[hpk] = max(all_hits.get(hpk, 0), float(h.score))

                merged = sorted(all_hits.items(), key=lambda x: -x[1])
                hits2 = [k for k, v in merged[:10]]
            else:
                hits2 = hits1[:10]

            if pk in hits2[:10]:
                s2_r10 += 1

        n = len(files)
        r1 = s1_r10 / n * 100
        r2 = s2_r10 / n * 100
        diff = r2 - r1
        print(f"{sc:<22} {r1:>6.1f}% {r2:>6.1f}% {diff:>+6.1f}%  (fallback {fallback_count}/{n})")

    print(f"\n阈值: score < 0.8 触发 Stage 2")

if __name__ == "__main__":
    main()
