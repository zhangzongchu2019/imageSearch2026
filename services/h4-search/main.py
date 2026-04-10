#!/usr/bin/env python3
"""
H4 多模式搜索服务
- mode=exact:    原图/近重复 → pHash + DINOv2
- mode=instance: 同款/视角 → DINOv2
- mode=semantic: 跨品牌语义 → CLIP
- mode=auto:     DINOv2 + CLIP 加权融合

POST /search
  body: { image_b64, mode, top_k, brand_id, category_l1, merchant_ids }
"""
import base64
import io
import os
import time
import numpy as np
import torch
from PIL import Image
from typing import Optional, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from pymilvus import connections, Collection

MODEL_DINOV2 = os.environ.get("DINOV2_MODEL", "/data/imgsrch/models/dinov2-large")
MODEL_CLIP = os.environ.get("CLIP_MODEL", "/data/imgsrch/models/ViT-L-14.pt")
COLLECTION = os.environ.get("COLLECTION", "h4_main")
MILVUS_HOST = os.environ.get("MILVUS_HOST", "imgsrch-mvs-proxy")

DINOV2_MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32)
DINOV2_STD = np.array([0.229, 0.224, 0.225], dtype=np.float32)
CLIP_MEAN = np.array([0.48145466, 0.4578275, 0.40821073], dtype=np.float32)
CLIP_STD = np.array([0.26862954, 0.26130258, 0.27577711], dtype=np.float32)

state = {}


class SearchRequest(BaseModel):
    image_b64: str
    mode: str = "auto"  # auto | exact | instance | semantic
    top_k: int = 10
    brand_id: Optional[List[int]] = None
    category_l1: Optional[int] = None
    merchant_ids: Optional[List[int]] = None
    fusion_weight_dinov2: float = 0.6


class SearchResponse(BaseModel):
    results: list
    mode: str
    latency_ms: dict
    total_ms: float


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("加载模型...")
    from transformers import AutoModel
    import open_clip

    state["dinov2"] = AutoModel.from_pretrained(MODEL_DINOV2, dtype=torch.float16).cuda().eval()
    clip_model, _, _ = open_clip.create_model_and_transforms("ViT-L-14", pretrained=MODEL_CLIP)
    state["clip_visual"] = clip_model.visual.cuda().half().eval()
    del clip_model
    print("  模型加载完成")

    connections.connect("h4", host=MILVUS_HOST, port="19530")
    state["collection"] = Collection(COLLECTION, using="h4")
    state["collection"].load()
    print(f"  Collection {COLLECTION}: {state['collection'].num_entities:,}")
    yield
    state["collection"].release()


app = FastAPI(lifespan=lifespan)


def preprocess(img, mean, std):
    if img.size != (224, 224):
        img = img.resize((224, 224), Image.BICUBIC)
    arr = np.array(img, dtype=np.float32) / 255.0
    arr = ((arr - mean) / std).transpose(2, 0, 1)
    return torch.from_numpy(arr).unsqueeze(0).cuda().to(torch.float16)


def encode_dinov2(img):
    t = preprocess(img, DINOV2_MEAN, DINOV2_STD)
    with torch.no_grad():
        out = state["dinov2"](pixel_values=t)
        cls = out.last_hidden_state[:, 0, :]
        return (cls / cls.norm(dim=-1, keepdim=True))[0].cpu().float().numpy()


def encode_clip(img):
    t = preprocess(img, CLIP_MEAN, CLIP_STD)
    with torch.no_grad():
        feat = state["clip_visual"](t)
        return (feat / feat.norm(dim=-1, keepdim=True))[0].cpu().float().numpy()


def build_expr(req):
    parts = []
    if req.brand_id:
        parts.append(f"brand_id in {req.brand_id}")
    if req.category_l1:
        parts.append(f"category_l1 == {req.category_l1}")
    if req.merchant_ids:
        or_parts = " or ".join([f"array_contains(merchant_ids, {m})" for m in req.merchant_ids])
        parts.append(f"({or_parts})")
    return " and ".join(parts) if parts else None


def search_vec(field, vec, expr, top_k):
    return state["collection"].search(
        data=[vec.tolist()],
        anns_field=field,
        param={"metric_type": "COSINE", "params": {"ef": 300}},
        limit=top_k,
        expr=expr,
        output_fields=["image_pk", "category_l1", "category_l2", "brand_id"]
    )


@app.get("/health")
async def health():
    return {"status": "ok", "collection": COLLECTION, "entities": state["collection"].num_entities}


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
    timings["decode_ms"] = (time.time() - t0) * 1000

    # 过滤表达式
    expr = build_expr(req)

    # 推理 + 搜索
    t0 = time.time()
    if req.mode == "instance":
        d_vec = encode_dinov2(img)
        timings["infer_ms"] = (time.time() - t0) * 1000
        t0 = time.time()
        res = search_vec("dinov2_vec", d_vec, expr, req.top_k)
        timings["search_ms"] = (time.time() - t0) * 1000
        hits = [(h.entity.get("image_pk"), float(h.score), h.entity) for h in res[0]]

    elif req.mode == "semantic":
        c_vec = encode_clip(img)
        timings["infer_ms"] = (time.time() - t0) * 1000
        t0 = time.time()
        res = search_vec("clip_vec", c_vec, expr, req.top_k)
        timings["search_ms"] = (time.time() - t0) * 1000
        hits = [(h.entity.get("image_pk"), float(h.score), h.entity) for h in res[0]]

    elif req.mode == "exact":
        d_vec = encode_dinov2(img)
        timings["infer_ms"] = (time.time() - t0) * 1000
        t0 = time.time()
        res = search_vec("dinov2_vec", d_vec, expr, req.top_k)
        timings["search_ms"] = (time.time() - t0) * 1000
        hits = [(h.entity.get("image_pk"), float(h.score), h.entity) for h in res[0] if h.score > 0.95]
        if len(hits) < req.top_k:
            hits.extend([(h.entity.get("image_pk"), float(h.score), h.entity)
                        for h in res[0] if h.score <= 0.95][:req.top_k - len(hits)])

    else:  # auto: fusion
        d_vec = encode_dinov2(img)
        c_vec = encode_clip(img)
        timings["infer_ms"] = (time.time() - t0) * 1000
        t0 = time.time()
        w_d = req.fusion_weight_dinov2
        w_c = 1 - w_d
        res_d = search_vec("dinov2_vec", d_vec, expr, req.top_k * 3)
        res_c = search_vec("clip_vec", c_vec, expr, req.top_k * 3)
        timings["search_ms"] = (time.time() - t0) * 1000

        scores = {}
        entities = {}
        for h in res_d[0]:
            pk = h.entity.get("image_pk")
            scores[pk] = w_d * float(h.score)
            entities[pk] = h.entity
        for h in res_c[0]:
            pk = h.entity.get("image_pk")
            scores[pk] = scores.get(pk, 0) + w_c * float(h.score)
            if pk not in entities:
                entities[pk] = h.entity
        ranked = sorted(scores.items(), key=lambda x: -x[1])[:req.top_k]
        hits = [(pk, score, entities.get(pk, {})) for pk, score in ranked]

    # 格式化
    results = []
    for pk, score, entity in hits[:req.top_k]:
        results.append({
            "image_pk": pk,
            "score": round(score, 4),
            "category_l1": entity.get("category_l1"),
            "category_l2": entity.get("category_l2"),
            "brand_id": entity.get("brand_id"),
        })

    total_ms = (time.time() - t_total) * 1000
    return SearchResponse(results=results, mode=req.mode, latency_ms=timings, total_ms=total_ms)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9810)
