#!/usr/bin/env python3
"""
10 场景 × 4 模型 完整对比评测
在 h4_main (DINOv2+CLIP) 基础上, 额外建 SSCD 和 SigLIP2 临时 Collection 做对比

输出: 10×4 矩阵, 每个模型在每个场景的 R@1/R@3/R@5/R@8/R@10/R@15
"""
import os, json, time, glob
import numpy as np
import torch
from PIL import Image
from pymilvus import connections, utility, Collection, CollectionSchema, FieldSchema, DataType
from concurrent.futures import ThreadPoolExecutor

EVAL_DIR = "/data/imgsrch/eval_10scenes"
MILVUS_HOST = os.environ.get("MILVUS_HOST", "imgsrch-mvs-proxy")

# 模型预处理常量
DINOV2_MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32)
DINOV2_STD = np.array([0.229, 0.224, 0.225], dtype=np.float32)
CLIP_MEAN = np.array([0.48145466, 0.4578275, 0.40821073], dtype=np.float32)
CLIP_STD = np.array([0.26862954, 0.26130258, 0.27577711], dtype=np.float32)
SIGLIP_MEAN = np.array([0.5, 0.5, 0.5], dtype=np.float32)
SIGLIP_STD = np.array([0.5, 0.5, 0.5], dtype=np.float32)
SSCD_MEAN = DINOV2_MEAN  # ImageNet
SSCD_STD = DINOV2_STD


def preprocess(img, mean, std):
    if img.size != (224, 224):
        img = img.resize((224, 224), Image.BICUBIC)
    arr = np.array(img, dtype=np.float32) / 255.0
    return ((arr - mean) / std).transpose(2, 0, 1)


def create_temp_collection(name, dim):
    """创建临时单向量 Collection"""
    if utility.has_collection(name):
        utility.drop_collection(name)
    fields = [
        FieldSchema(name="image_pk", dtype=DataType.VARCHAR, max_length=64, is_primary=True),
        FieldSchema(name="vec", dtype=DataType.FLOAT_VECTOR, dim=dim),
    ]
    schema = CollectionSchema(fields)
    coll = Collection(name, schema=schema)
    coll.create_index("vec", {
        "index_type": "HNSW_SQ",
        "metric_type": "COSINE",
        "params": {"M": 48, "efConstruction": 500, "sq_type": "SQ8", "refine": True, "refine_type": "FP16"}
    })
    return coll


def import_vectors(coll, vector_dir, vec_key):
    """从 jsonl 导入向量 (含重试)"""
    files = sorted(glob.glob(os.path.join(vector_dir, "*.jsonl")))
    pks = []
    vecs = []
    imported = 0
    for f in files:
        with open(f) as fh:
            for line in fh:
                d = json.loads(line)
                pks.append(d["image_pk"])
                vecs.append(d[vec_key])
                if len(pks) >= 100:
                    for attempt in range(3):
                        try:
                            coll.insert([pks, vecs])
                            imported += len(pks)
                            break
                        except Exception as e:
                            if attempt < 2:
                                import time; time.sleep(2)
                            else:
                                print(f"  跳过 {len(pks)} 条: {str(e)[:60]}")
                    pks, vecs = [], []
                    if imported % 50000 == 0:
                        print(f"  导入 {imported:,}...")
    if pks:
        try:
            coll.insert([pks, vecs])
            imported += len(pks)
        except: pass
    coll.flush()
    coll.load()
    return coll.num_entities


def main():
    connections.connect(host=MILVUS_HOST, port="19530")

    # === 准备 4 个 Collection ===
    print("=== 准备 Collections ===")

    # 1. DINOv2 (从 h4_main)
    print("DINOv2: 使用 h4_main.dinov2_vec")
    h4 = Collection("h4_main")
    h4.load()
    print(f"  h4_main: {h4.num_entities:,}")

    # 2. CLIP (从 h4_main)
    print("CLIP: 使用 h4_main.clip_vec")

    # 3. SSCD (建临时 collection)
    print("SSCD: 创建临时 collection...")
    sscd_coll = create_temp_collection("eval_sscd_temp", 512)
    n = import_vectors(sscd_coll, "/data/imgsrch/vectors_sscd", "sscd_vec")
    print(f"  eval_sscd_temp: {n:,}")

    # 4. SigLIP2 (建临时 collection)
    print("SigLIP2: 创建临时 collection...")
    siglip_coll = create_temp_collection("eval_siglip2_temp", 768)
    n = import_vectors(siglip_coll, "/data/imgsrch/vectors_siglip2", "siglip2_vec")
    print(f"  eval_siglip2_temp: {n:,}")

    # === 加载模型 ===
    print("\n=== 加载推理模型 ===")
    from transformers import AutoModel
    import open_clip

    dinov2 = AutoModel.from_pretrained("/data/imgsrch/models/dinov2-large", dtype=torch.float16).cuda().eval()
    print("  DINOv2 ✅")

    clip_model, _, _ = open_clip.create_model_and_transforms("ViT-L-14", pretrained="/data/imgsrch/models/ViT-L-14.pt")
    clip_visual = clip_model.visual.cuda().half().eval()
    del clip_model
    print("  CLIP ✅")

    sscd_model = torch.jit.load("/data/imgsrch/models/sscd_resnet50.pt").cuda().eval()
    print("  SSCD ✅")

    full_sig = AutoModel.from_pretrained("/data/imgsrch/models/siglip2-base", dtype=torch.float16)
    siglip_vision = full_sig.vision_model.cuda().eval()
    del full_sig
    print("  SigLIP2 ✅")

    # === 加载 ground truth ===
    with open(os.path.join(EVAL_DIR, "ground_truth.json")) as f:
        gt = json.load(f)
    scenarios = gt["scenarios"]

    pool = ThreadPoolExecutor(max_workers=16)
    BS = 32
    search_params = {"metric_type": "COSINE", "params": {"ef": 300}}

    # 定义模型推理 + 搜索
    def encode_and_search(model_name, imgs_pil, target_pks):
        """对一批图推理 + 搜索, 返回 {pk: [hit_pks]}"""
        # 预处理
        if model_name == "dinov2":
            arrs = [preprocess(img, DINOV2_MEAN, DINOV2_STD) for img in imgs_pil]
            tensor = torch.from_numpy(np.stack(arrs)).cuda().to(torch.float16)
            with torch.no_grad():
                out = dinov2(pixel_values=tensor)
                feat = out.last_hidden_state[:, 0, :]
                feat = feat / feat.norm(dim=-1, keepdim=True)
            vecs = feat.cpu().float().numpy()
            results = h4.search(data=vecs.tolist(), anns_field="dinov2_vec",
                               param=search_params, limit=15, output_fields=["image_pk"])
        elif model_name == "clip":
            arrs = [preprocess(img, CLIP_MEAN, CLIP_STD) for img in imgs_pil]
            tensor = torch.from_numpy(np.stack(arrs)).cuda().to(torch.float16)
            with torch.no_grad():
                feat = clip_visual(tensor)
                feat = feat / feat.norm(dim=-1, keepdim=True)
            vecs = feat.cpu().float().numpy()
            results = h4.search(data=vecs.tolist(), anns_field="clip_vec",
                               param=search_params, limit=15, output_fields=["image_pk"])
        elif model_name == "sscd":
            arrs = [preprocess(img, SSCD_MEAN, SSCD_STD) for img in imgs_pil]
            tensor = torch.from_numpy(np.stack(arrs)).cuda()
            with torch.no_grad():
                feat = sscd_model(tensor)
                feat = feat / feat.norm(dim=-1, keepdim=True)
            vecs = feat.cpu().numpy()
            results = sscd_coll.search(data=vecs.tolist(), anns_field="vec",
                                       param=search_params, limit=15, output_fields=["image_pk"])
        elif model_name == "siglip2":
            arrs = [preprocess(img, SIGLIP_MEAN, SIGLIP_STD) for img in imgs_pil]
            tensor = torch.from_numpy(np.stack(arrs)).cuda().to(torch.float16)
            with torch.no_grad():
                out = siglip_vision(pixel_values=tensor)
                feat = out.pooler_output
                feat = feat / feat.norm(dim=-1, keepdim=True)
            vecs = feat.cpu().float().numpy()
            results = siglip_coll.search(data=vecs.tolist(), anns_field="vec",
                                          param=search_params, limit=15, output_fields=["image_pk"])

        # 收集 hits
        all_hits = {}
        for i, pk in enumerate(target_pks):
            all_hits[pk] = [h.entity.get("image_pk") for h in results[i]]
        return all_hits

    # === 跑 10 场景 × 4 模型 ===
    all_results = {}
    models = ["dinov2", "clip", "sscd", "siglip2"]

    for scenario in scenarios:
        print(f"\n=== {scenario} ===")
        scenario_dir = os.path.join(EVAL_DIR, scenario)
        files = sorted(os.listdir(scenario_dir))

        # 加载所有图片
        def load(fname):
            try:
                return (fname.replace(".jpg", ""), Image.open(os.path.join(scenario_dir, fname)).convert("RGB"))
            except:
                return None

        loaded = [x for x in pool.map(load, files) if x]
        pks = [x[0] for x in loaded]
        imgs = [x[1] for x in loaded]

        scenario_results = {}
        for model_name in models:
            # 分 batch
            r = {f"R@{k}": 0 for k in [1, 3, 5, 8, 10, 15]}
            for bs in range(0, len(pks), BS):
                batch_pks = pks[bs:bs+BS]
                batch_imgs = imgs[bs:bs+BS]
                hits = encode_and_search(model_name, batch_imgs, batch_pks)
                for pk in batch_pks:
                    target = pk  # ground truth = 原图 pk
                    h = hits.get(pk, [])
                    for k in [1, 3, 5, 8, 10, 15]:
                        if target in h[:k]:
                            r[f"R@{k}"] += 1

            n = len(pks)
            for k in r:
                r[k] = round(r[k] / n * 100, 1)
            scenario_results[model_name] = r
            print(f"  {model_name:>8}: R@1={r['R@1']:>5.1f}% R@3={r['R@3']:>5.1f}% R@5={r['R@5']:>5.1f}% R@8={r['R@8']:>5.1f}% R@10={r['R@10']:>5.1f}% R@15={r['R@15']:>5.1f}%")

        all_results[scenario] = scenario_results

    # === 总报告 ===
    print("\n" + "=" * 120)
    print("📊 10 场景 × 4 模型 完整对比报告")
    print("=" * 120)
    print(f"{'场景':<22} {'模型':<10} {'R@1':<8} {'R@3':<8} {'R@5':<8} {'R@8':<8} {'R@10':<8} {'R@15':<8}")
    print("-" * 120)
    for scenario in scenarios:
        for i, model in enumerate(models):
            r = all_results[scenario][model]
            label = scenario if i == 0 else ""
            print(f"{label:<22} {model:<10} {r['R@1']:>5.1f}% {r['R@3']:>5.1f}% {r['R@5']:>5.1f}% {r['R@8']:>5.1f}% {r['R@10']:>5.1f}% {r['R@15']:>5.1f}%")
        print()

    # 验收对照
    print("=" * 120)
    print("📋 验收标准对照")
    print("=" * 120)
    acceptance = [
        ("1_original",       "R@1",  "Top1=原图"),
        ("2_jpeg_compress",  "R@3",  "Top3找到原图"),
        ("3_crop_70",        "R@8",  "Top8找到原图"),
        ("4_aspect_change",  "R@10", "Top10找到原图"),
        ("5_ui_overlay",     "R@5",  "Top5找到原图"),
        ("6_crop_30",        "R@10", "Top10找到原图"),
        ("7_embed_in_large", "R@10", "Top3~10找到原图"),
        ("8_reverse_embed",  "R@10", "Top3~10找到原图"),
        ("9_graffiti_heavy", "R@5",  "Top5找到原图"),
        ("10_resize_scale",  "R@10", "Top10找到原图"),
    ]
    print(f"{'需求':<22} {'标准':<18} {'DINOv2':<10} {'CLIP':<10} {'SSCD':<10} {'SigLIP2':<10} {'最优':<10}")
    print("-" * 120)
    for scenario, metric, desc in acceptance:
        if scenario in all_results:
            vals = {m: all_results[scenario][m][metric] for m in models}
            best_model = max(vals, key=vals.get)
            best_val = vals[best_model]
            status = "✅" if best_val >= 95 else ("⚠️" if best_val >= 70 else "❌")
            print(f"{scenario:<22} {desc:<18} {vals['dinov2']:>5.1f}%   {vals['clip']:>5.1f}%   {vals['sscd']:>5.1f}%   {vals['siglip2']:>5.1f}%   {status} {best_model}={best_val:.1f}%")

    # 保存
    with open("/data/imgsrch/task_logs/eval_10scenes_4models.json", "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\n详细结果: /data/imgsrch/task_logs/eval_10scenes_4models.json")

    # 清理临时 Collection
    print("\n清理临时 collections...")
    utility.drop_collection("eval_sscd_temp")
    utility.drop_collection("eval_siglip2_temp")
    print("✅ 已清理")


if __name__ == "__main__":
    main()
