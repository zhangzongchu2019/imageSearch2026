#!/usr/bin/env python3
"""
H4 v0.3 搜索服务 (两阶段搜索)
- Stage 1: SSCD 全图 + center_vec (~20ms)
- Stage 2: score < 0.8 时触发多尺度裁剪 fallback (+30ms)
- 支持: instance / filtered_instance / exact / auto
"""
import base64, io, os, time, sys
import numpy as np, torch
from PIL import Image
from typing import Optional, List
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from pymilvus import connections, Collection

SSCD_PATH = os.environ.get("SSCD_MODEL", "/data/imgsrch/models/sscd_resnet50.pt")
CLIP_PATH = os.environ.get("CLIP_MODEL", "/data/imgsrch/models/ViT-L-14.pt")
COLLECTION = os.environ.get("COLLECTION", "h4v03_main")
MILVUS_HOST = os.environ.get("MILVUS_HOST", "imgsrch-mvs-proxy")
FALLBACK_THRESHOLD = float(os.environ.get("FALLBACK_THRESHOLD", "0.8"))

SSCD_MEAN = np.array([0.485,0.456,0.406], dtype=np.float32)
SSCD_STD = np.array([0.229,0.224,0.225], dtype=np.float32)
CLIP_MEAN = np.array([0.48145466,0.4578275,0.40821073], dtype=np.float32)
CLIP_STD = np.array([0.26862954,0.26130258,0.27577711], dtype=np.float32)

state = {}


class SearchRequest(BaseModel):
    image_b64: str
    mode: str = "auto"
    top_k: int = 10
    category_l1: Optional[int] = None
    color_code: Optional[int] = None
    brand_id: Optional[List[int]] = None


class SearchResponse(BaseModel):
    results: list
    mode: str
    stage: int
    top1_score: float
    latency_ms: dict
    total_ms: float


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

def sscd_encode(img):
    arr = pp_sscd(img)
    tensor = torch.from_numpy(arr[np.newaxis]).cuda()
    with torch.no_grad():
        feat = state["sscd"](tensor)
        return (feat / feat.norm(dim=-1, keepdim=True))[0].cpu().numpy()

def sscd_encode_batch(imgs):
    arrs = [pp_sscd(img) for img in imgs]
    tensor = torch.from_numpy(np.stack(arrs)).cuda()
    with torch.no_grad():
        feat = state["sscd"](tensor)
        feat = feat / feat.norm(dim=-1, keepdim=True)
    return feat.cpu().numpy()

def build_expr(req):
    parts = []
    if req.category_l1: parts.append(f"category_l1 == {req.category_l1}")
    if req.color_code: parts.append(f"color_code == {req.color_code}")
    if req.brand_id: parts.append(f"brand_id in {req.brand_id}")
    return " and ".join(parts) if parts else None

def search_vec(field, vec, expr, limit):
    return state["coll"].search(
        data=[vec.tolist()], anns_field=field,
        param={"metric_type":"COSINE", "params":{"ef":200}},
        limit=limit, expr=expr,
        output_fields=["image_pk","category_l1","category_l2","color_code","brand_id"])


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("加载模型...")
    state["sscd"] = torch.jit.load(SSCD_PATH).cuda().eval()
    print(f"  SSCD ✅")
    connections.connect("h4", host=MILVUS_HOST, port="19530")
    state["coll"] = Collection(COLLECTION, using="h4")
    state["coll"].load()
    print(f"  {COLLECTION}: {state['coll'].num_entities:,}")
    yield
    state["coll"].release()


app = FastAPI(title="H4 v0.3 Search", version="0.3.0")


@app.get("/health")
async def health():
    return {"status": "ok", "collection": COLLECTION,
            "entities": state["coll"].num_entities,
            "fallback_threshold": FALLBACK_THRESHOLD}


@app.post("/search", response_model=SearchResponse)
async def search(req: SearchRequest):
    timings = {}
    t_total = time.time()

    # 解码
    t0 = time.time()
    try:
        img = Image.open(io.BytesIO(base64.b64decode(req.image_b64))).convert("RGB")
    except Exception as e:
        raise HTTPException(400, f"Invalid image: {e}")
    timings["decode_ms"] = (time.time()-t0)*1000

    expr = build_expr(req)
    stage = 1

    # === Stage 1: SSCD 全图 + center_vec ===
    t0 = time.time()
    vec = sscd_encode(img)
    timings["sscd_encode_ms"] = (time.time()-t0)*1000

    t0 = time.time()
    res_sscd = search_vec("sscd_vec", vec, expr, req.top_k * 2)
    res_center = search_vec("center_vec", vec, expr, req.top_k * 2)
    timings["stage1_search_ms"] = (time.time()-t0)*1000

    # 合并 Stage 1
    all_hits = {}
    all_entities = {}
    for h in res_sscd[0]:
        pk = h.entity.get("image_pk")
        all_hits[pk] = float(h.score)
        all_entities[pk] = h.entity
    for h in res_center[0]:
        pk = h.entity.get("image_pk")
        all_hits[pk] = max(all_hits.get(pk, 0), float(h.score))
        if pk not in all_entities: all_entities[pk] = h.entity

    top1_score = float(res_sscd[0][0].score) if res_sscd[0] else 0

    # === Stage 2: Fallback (score < threshold) ===
    if top1_score < FALLBACK_THRESHOLD and req.mode != "exact":
        stage = 2
        t0 = time.time()

        crops = make_crops(img)
        crop_vecs = sscd_encode_batch(crops)
        timings["crop_encode_ms"] = (time.time()-t0)*1000

        t0 = time.time()
        for cv in crop_vecs:
            res_crop = search_vec("sscd_vec", cv, expr, req.top_k)
            for h in res_crop[0]:
                pk = h.entity.get("image_pk")
                all_hits[pk] = max(all_hits.get(pk, 0), float(h.score))
                if pk not in all_entities: all_entities[pk] = h.entity
        timings["stage2_search_ms"] = (time.time()-t0)*1000

    # 排序
    ranked = sorted(all_hits.items(), key=lambda x: -x[1])[:req.top_k]

    results = []
    for pk, score in ranked:
        ent = all_entities.get(pk, {})
        results.append({
            "image_pk": pk,
            "score": round(score, 4),
            "category_l1": ent.get("category_l1"),
            "category_l2": ent.get("category_l2"),
            "color_code": ent.get("color_code"),
            "brand_id": ent.get("brand_id"),
        })

    total_ms = (time.time()-t_total)*1000
    return SearchResponse(
        results=results, mode=req.mode, stage=stage,
        top1_score=round(top1_score, 4),
        latency_ms=timings, total_ms=total_ms)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9810)
