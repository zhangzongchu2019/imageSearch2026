#!/usr/bin/env python3
"""
导入 H4 v0.3 Collection
- SSCD 向量 (全图) 从 vectors_all_sscd/
- SSCD 向量 (中心裁剪) 实时计算
- CLIP 标签 从 test_h4/ (dual 输出含 category_l1/l2)
- 模拟其他标签 + quality/scene
"""
import json, os, glob, time, random, hashlib
import numpy as np
from PIL import Image
from pymilvus import connections, Collection

COLLECTION = "h4v03_main"
SSCD_DIR = "/data/imgsrch/vectors_all_sscd"
DUAL_DIR = "/data/imgsrch/test_h4"  # 第1批的 dual 输出 (含 CLIP 标签)
IMAGE_DIR = "/data/test-images"
BATCH = 200

# 加载 SSCD 向量 → dict
def load_sscd_vectors():
    print("加载 SSCD 向量...")
    vecs = {}
    for f in sorted(glob.glob(os.path.join(SSCD_DIR, "sscd_gpu*.jsonl"))):
        with open(f) as fh:
            for line in fh:
                d = json.loads(line)
                vecs[d["image_pk"]] = d["sscd_vec"]
    print(f"  SSCD: {len(vecs):,}")
    return vecs

# 加载 CLIP 标签 → dict
def load_clip_tags():
    print("加载 CLIP 标签...")
    tags = {}
    for f in sorted(glob.glob(os.path.join(DUAL_DIR, "dual_gpu*.jsonl"))):
        with open(f) as fh:
            for line in fh:
                d = json.loads(line)
                tags[d["image_pk"]] = {
                    "category_l1": d.get("category_l1", 0),
                    "category_l2": d.get("category_l2", 0),
                }
    print(f"  CLIP: {len(tags):,}")
    return tags

def fake_extra_tags(pk):
    rng = random.Random(int(pk[:8], 16))
    return {
        "color_code": rng.randint(1, 20),
        "material_code": rng.randint(1, 30),
        "pattern_code": rng.randint(1, 15),
        "style_code": rng.randint(1, 15),
        "sleeve_code": rng.randint(0, 5),
        "season_code": rng.randint(1, 4),
        "gender_code": rng.randint(1, 3),
        "scene_type": rng.randint(1, 5),
        "quality_score": rng.randint(50, 100),
        "brand_id": rng.randint(1, 1000),
        "merchant_set_id": rng.randint(1, 100000),
    }

def compute_center_sscd(sscd_vec):
    """模拟中心裁剪 SSCD — 简化版: 给全图向量加噪声模拟"""
    # 生产中应该真正裁剪图片再推理
    # 这里为了速度用近似: 全图向量 + 小随机扰动
    vec = np.array(sscd_vec, dtype=np.float32)
    noise = np.random.randn(512).astype(np.float32) * 0.05
    center = vec + noise
    center = center / np.linalg.norm(center)
    return center.tolist()

def main():
    connections.connect(host="localhost", port="19530")
    coll = Collection(COLLECTION)
    print(f"Collection: {coll.num_entities:,}")

    sscd_vecs = load_sscd_vectors()
    clip_tags = load_clip_tags()

    # 只导入有 SSCD 向量 AND 有 CLIP 标签的
    common_pks = set(sscd_vecs.keys()) & set(clip_tags.keys())
    print(f"交集: {len(common_pks):,}")
    pks_list = sorted(common_pks)

    batch_data = {k: [] for k in [
        "image_pk","sscd_vec","center_vec","phash","dhash",
        "category_l1","category_l2","color_code",
        "material_code","pattern_code","style_code","sleeve_code","season_code","gender_code",
        "scene_type","quality_score","brand_id","merchant_set_id","ts_month","created_at"
    ]}

    t0 = time.time()
    inserted = 0
    zero_hash = b'\x00' * 8

    for pk in pks_list:
        sv = sscd_vecs[pk]
        ct = clip_tags[pk]
        extra = fake_extra_tags(pk)
        cv = compute_center_sscd(sv)

        batch_data["image_pk"].append(pk)
        batch_data["sscd_vec"].append(sv)
        batch_data["center_vec"].append(cv)
        batch_data["phash"].append(zero_hash)
        batch_data["dhash"].append(zero_hash)
        batch_data["category_l1"].append(ct["category_l1"])
        batch_data["category_l2"].append(ct["category_l2"])
        batch_data["color_code"].append(extra["color_code"])
        batch_data["material_code"].append(extra["material_code"])
        batch_data["pattern_code"].append(extra["pattern_code"])
        batch_data["style_code"].append(extra["style_code"])
        batch_data["sleeve_code"].append(extra["sleeve_code"])
        batch_data["season_code"].append(extra["season_code"])
        batch_data["gender_code"].append(extra["gender_code"])
        batch_data["scene_type"].append(extra["scene_type"])
        batch_data["quality_score"].append(extra["quality_score"])
        batch_data["brand_id"].append(extra["brand_id"])
        batch_data["merchant_set_id"].append(extra["merchant_set_id"])
        batch_data["ts_month"].append(202604)
        batch_data["created_at"].append(int(time.time()))

        if len(batch_data["image_pk"]) >= BATCH:
            for attempt in range(3):
                try:
                    coll.insert(list(batch_data.values()))
                    inserted += len(batch_data["image_pk"])
                    break
                except Exception as e:
                    if attempt < 2: time.sleep(2)
                    else: print(f"  跳过 {BATCH} 条: {str(e)[:60]}")
            for k in batch_data: batch_data[k].clear()

            if inserted % 50000 == 0:
                elapsed = time.time() - t0
                print(f"  [{inserted:,}/{len(pks_list):,}] {inserted/elapsed:.0f}/s")

    # 末批
    if batch_data["image_pk"]:
        try:
            coll.insert(list(batch_data.values()))
            inserted += len(batch_data["image_pk"])
        except: pass

    coll.flush()
    elapsed = time.time() - t0
    print(f"\n✅ 导入: {inserted:,}, {elapsed:.0f}s ({inserted/elapsed:.0f}/s)")
    print(f"   Collection: {coll.num_entities:,}")

if __name__ == "__main__":
    main()
