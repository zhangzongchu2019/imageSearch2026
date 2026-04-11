#!/usr/bin/env python3
"""
两阶段搜索 + 标签辅助评测
Stage 1: SSCD 全图
Stage 2 (score < 0.8): 多尺度裁剪 + CLIP 标签缩小范围
对比: 无标签 vs 有标签
"""
import os, json, time, sys
import numpy as np, torch
from PIL import Image
from pymilvus import connections, Collection

EVAL = "/data/imgsrch/eval_10scenes"
COLLECTION = "h4v03_main"

SSCD_MEAN = np.array([0.485,0.456,0.406], dtype=np.float32)
SSCD_STD = np.array([0.229,0.224,0.225], dtype=np.float32)
CLIP_MEAN = np.array([0.48145466,0.4578275,0.40821073], dtype=np.float32)
CLIP_STD = np.array([0.26862954,0.26130258,0.27577711], dtype=np.float32)

def pp_sscd(img):
    if img.size != (224,224): img = img.resize((224,224), Image.BICUBIC)
    return ((np.array(img, dtype=np.float32)/255.0 - SSCD_MEAN) / SSCD_STD).transpose(2,0,1)

def pp_clip(img):
    if img.size != (224,224): img = img.resize((224,224), Image.BICUBIC)
    return ((np.array(img, dtype=np.float32)/255.0 - CLIP_MEAN) / CLIP_STD).transpose(2,0,1)

def make_crops(img):
    w, h = img.size
    return [
        img.crop((int(w*0.2), int(h*0.2), int(w*0.8), int(h*0.8))),
        img.crop((int(w*0.3), int(h*0.3), int(w*0.7), int(h*0.7))),
        img.crop((0, 0, w, int(h*0.5))),
        img.crop((0, int(h*0.5), w, h)),
        img.crop((int(w*0.1), int(h*0.1), int(w*0.9), int(h*0.5))),
        img.crop((int(w*0.1), int(h*0.5), int(w*0.9), int(h*0.9))),
    ]

def main():
    connections.connect(host="imgsrch-mvs-proxy", port="19530")
    coll = Collection(COLLECTION)
    coll.load()
    print(f"Collection: {coll.num_entities:,}")

    # 加载 SSCD
    sscd = torch.jit.load("/data/imgsrch/models/sscd_resnet50.pt").cuda().eval()

    # 加载 CLIP (用于标签提取)
    import open_clip
    sys.path.insert(0, "/workspace/services/inference-service")
    from app.taxonomy import L1_CODES, L1_CODE_TO_NAME

    clip_model, _, _ = open_clip.create_model_and_transforms("ViT-L-14",
        pretrained="/data/imgsrch/models/ViT-L-14.pt")
    clip_model = clip_model.cuda().half().eval()
    clip_visual = clip_model.visual
    tokenizer = open_clip.get_tokenizer("ViT-L-14")

    # L1 文本特征
    l1_prompts = [f"a photo of {L1_CODE_TO_NAME[c]}" for c in L1_CODES]
    l1_tokens = tokenizer(l1_prompts).cuda()
    with torch.no_grad():
        l1_text = clip_model.encode_text(l1_tokens).half()
        l1_text = l1_text / l1_text.norm(dim=-1, keepdim=True)
    del clip_model
    print("SSCD + CLIP 加载完成")

    def get_l1_tag(img):
        arr = pp_clip(img)
        tensor = torch.from_numpy(arr[np.newaxis]).cuda().half()
        with torch.no_grad():
            feat = clip_visual(tensor)
            feat = feat / feat.norm(dim=-1, keepdim=True)
            sim = (feat @ l1_text.T).float()
            idx = sim.argmax(dim=-1).item()
            confidence = sim[0, idx].item()
        return L1_CODES[idx], confidence

    params = {"metric_type":"COSINE", "params":{"ef":200}}

    with open(os.path.join(EVAL, "ground_truth.json")) as f:
        scenarios = json.load(f)["scenarios"]

    print(f"\n{'场景':<22} {'S1 only':>8} {'S2无标签':>8} {'S2+标签':>8} {'标签提升':>8}")
    print("-" * 62)

    for sc in scenarios:
        d = os.path.join(EVAL, sc)
        files = sorted(os.listdir(d))[:200]

        s1_r10 = 0
        s2_notag_r10 = 0
        s2_tag_r10 = 0

        for f in files:
            try:
                pk = f.replace(".jpg", "")
                img = Image.open(os.path.join(d, f)).convert("RGB")
            except:
                continue

            # Stage 1
            arr = pp_sscd(img)
            tensor = torch.from_numpy(arr[np.newaxis]).cuda()
            with torch.no_grad():
                feat = sscd(tensor)
                feat = feat / feat.norm(dim=-1, keepdim=True)
            vec = feat[0].cpu().numpy()

            res1 = coll.search(data=[vec.tolist()], anns_field="sscd_vec",
                              param=params, limit=15, output_fields=["image_pk"])
            hits1 = [h.entity.get("image_pk") for h in res1[0]]
            top1_score = float(res1[0][0].score) if res1[0] else 0

            if pk in hits1[:10]: s1_r10 += 1

            # Stage 2 (score < 0.8)
            if top1_score < 0.8:
                crops = make_crops(img)
                crop_arrs = [pp_sscd(c) for c in crops]
                crop_tensor = torch.from_numpy(np.stack(crop_arrs)).cuda()
                with torch.no_grad():
                    crop_feats = sscd(crop_tensor)
                    crop_feats = crop_feats / crop_feats.norm(dim=-1, keepdim=True)
                crop_vecs = crop_feats.cpu().numpy()

                # --- 无标签版 ---
                hits_notag = {h.entity.get("image_pk"): float(h.score) for h in res1[0]}
                # center_vec
                res_c = coll.search(data=[vec.tolist()], anns_field="center_vec",
                                   param=params, limit=15, output_fields=["image_pk"])
                for h in res_c[0]:
                    hpk = h.entity.get("image_pk")
                    hits_notag[hpk] = max(hits_notag.get(hpk, 0), float(h.score))
                for cv in crop_vecs:
                    res_crop = coll.search(data=[cv.tolist()], anns_field="sscd_vec",
                                          param=params, limit=15, output_fields=["image_pk"])
                    for h in res_crop[0]:
                        hpk = h.entity.get("image_pk")
                        hits_notag[hpk] = max(hits_notag.get(hpk, 0), float(h.score))
                merged_notag = [k for k, v in sorted(hits_notag.items(), key=lambda x: -x[1])[:10]]

                # --- 有标签版 ---
                l1_tag, conf = get_l1_tag(img)
                expr = f"category_l1 == {l1_tag}" if conf > 0.5 else None

                hits_tag = {h.entity.get("image_pk"): float(h.score) for h in res1[0]}
                # center_vec with tag filter
                try:
                    res_ct = coll.search(data=[vec.tolist()], anns_field="center_vec",
                                       param=params, limit=15, expr=expr, output_fields=["image_pk"])
                    for h in res_ct[0]:
                        hpk = h.entity.get("image_pk")
                        hits_tag[hpk] = max(hits_tag.get(hpk, 0), float(h.score))
                except: pass

                for cv in crop_vecs:
                    try:
                        res_ct2 = coll.search(data=[cv.tolist()], anns_field="sscd_vec",
                                             param=params, limit=15, expr=expr, output_fields=["image_pk"])
                        for h in res_ct2[0]:
                            hpk = h.entity.get("image_pk")
                            hits_tag[hpk] = max(hits_tag.get(hpk, 0), float(h.score))
                    except: pass

                # fallback: 标签过滤结果太少 → 补充无标签结果
                if len(hits_tag) < 10:
                    for k, v in hits_notag.items():
                        if k not in hits_tag:
                            hits_tag[k] = v

                merged_tag = [k for k, v in sorted(hits_tag.items(), key=lambda x: -x[1])[:10]]
            else:
                merged_notag = hits1[:10]
                merged_tag = hits1[:10]

            if pk in merged_notag[:10]: s2_notag_r10 += 1
            if pk in merged_tag[:10]: s2_tag_r10 += 1

        n = len(files)
        r1 = s1_r10/n*100
        r2n = s2_notag_r10/n*100
        r2t = s2_tag_r10/n*100
        diff = r2t - r2n
        print(f"{sc:<22} {r1:>6.1f}% {r2n:>6.1f}% {r2t:>6.1f}% {diff:>+6.1f}%")

if __name__ == "__main__":
    main()
