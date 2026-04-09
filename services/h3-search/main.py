#!/usr/bin/env python3
"""
H3 端到端搜索服务
- 输入: image base64 + 过滤条件
- 流程: pHash → DINOv2 推理 → HNSW_SQ8 召回 → fp16 精排 → Top 10
- 输出: Top 10 结果 + 各阶段延迟
"""
import base64
import time
import io
import os
import sys
from typing import Optional, List
from contextlib import asynccontextmanager

import numpy as np
import torch
from PIL import Image
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymilvus import connections, Collection

# 模型路径
MODEL_PATH = os.environ.get("DINOV2_MODEL", "/data/imgsrch/models/dinov2-large")
COLLECTION_NAME = os.environ.get("COLLECTION", "test_dinov2_h3")
MILVUS_HOST = os.environ.get("MILVUS_HOST", "localhost")
MILVUS_PORT = os.environ.get("MILVUS_PORT", "19530")

# DINOv2 预处理常量
DINOV2_MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32)
DINOV2_STD = np.array([0.229, 0.224, 0.225], dtype=np.float32)


class SearchRequest(BaseModel):
    image_b64: str
    top_k: int = 10
    brand_id: Optional[List[int]] = None
    category_l1: Optional[int] = None
    category_l2: Optional[int] = None
    merchant_ids: Optional[List[int]] = None
    ef_search: int = 300


class SearchResponse(BaseModel):
    results: list
    latency_ms: dict
    total_ms: float


# 全局模型引用
state = {"model": None, "collection": None}


def fast_preprocess(img: Image.Image) -> np.ndarray:
    """快速预处理: PIL → CHW float32"""
    if img.size != (224, 224):
        img = img.resize((224, 224), Image.BICUBIC)
    arr = np.array(img, dtype=np.float32) * (1.0 / 255.0)
    arr = (arr - DINOV2_MEAN) / DINOV2_STD
    return arr.transpose(2, 0, 1)


def compute_phash(img: Image.Image) -> bytes:
    """简化版 pHash (8x8 DCT)"""
    img_gray = img.convert("L").resize((32, 32), Image.BICUBIC)
    arr = np.array(img_gray, dtype=np.float32)
    # DCT 简化: 取 8x8 低频
    from scipy.fft import dct
    dct_arr = dct(dct(arr, axis=0), axis=1)
    low_freq = dct_arr[:8, :8]
    median = np.median(low_freq)
    bits = (low_freq > median).astype(np.uint8).flatten()
    # 64 bit → 8 bytes
    return np.packbits(bits).tobytes()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("加载 DINOv2-L 模型...")
    from transformers import AutoModel
    model = AutoModel.from_pretrained(MODEL_PATH, dtype=torch.float16)
    state["model"] = model.cuda().eval()
    print(f"  模型已加载到 GPU")

    print(f"连接 Milvus: {MILVUS_HOST}:{MILVUS_PORT}")
    connections.connect("h3", host=MILVUS_HOST, port=MILVUS_PORT)
    state["collection"] = Collection(COLLECTION_NAME, using="h3")
    state["collection"].load()
    n = state["collection"].num_entities
    print(f"  Collection {COLLECTION_NAME} 加载: {n:,} entities")

    yield

    state["collection"].release()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "collection": COLLECTION_NAME}


@app.post("/search", response_model=SearchResponse)
async def search(req: SearchRequest):
    timings = {}
    t_total = time.time()

    # === Stage 1: 图片解码 ===
    t0 = time.time()
    try:
        img_bytes = base64.b64decode(req.image_b64)
        img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    except Exception as e:
        raise HTTPException(400, f"Invalid image: {e}")
    timings["decode_ms"] = (time.time() - t0) * 1000

    # === Stage 2: DINOv2 推理 ===
    t0 = time.time()
    arr = fast_preprocess(img)
    tensor = torch.from_numpy(arr).unsqueeze(0).cuda().to(torch.float16)
    with torch.no_grad():
        out = state["model"](pixel_values=tensor)
        cls = out.last_hidden_state[:, 0, :]  # (1, 1024)
        cls = cls / cls.norm(dim=-1, keepdim=True)
    query_vec = cls[0].cpu().float().numpy().tolist()
    timings["inference_ms"] = (time.time() - t0) * 1000

    # === Stage 3: pHash (可选, 用于近重复快速命中) ===
    t0 = time.time()
    try:
        phash_bytes = compute_phash(img)
        # TODO: 实际查询 phash 需要 Milvus binary 搜索, 这里先跳过
    except Exception:
        phash_bytes = None
    timings["phash_ms"] = (time.time() - t0) * 1000

    # === Stage 4: 构建过滤表达式 ===
    expr_parts = []
    if req.brand_id:
        expr_parts.append(f"brand_id in {req.brand_id}")
    if req.category_l1:
        expr_parts.append(f"category_l1 == {req.category_l1}")
    if req.category_l2:
        expr_parts.append(f"category_l2 == {req.category_l2}")
    # ARRAY_CONTAINS 多值过滤
    if req.merchant_ids:
        # 任一商家匹配
        merchant_or = " or ".join([f"array_contains(merchant_ids, {m})" for m in req.merchant_ids])
        expr_parts.append(f"({merchant_or})")
    expr = " and ".join(expr_parts) if expr_parts else None

    # === Stage 5: Milvus HNSW_SQ8 召回 ===
    t0 = time.time()
    search_params = {
        "metric_type": "COSINE",
        "params": {"ef": req.ef_search}
    }
    results = state["collection"].search(
        data=[query_vec],
        anns_field="dinov2_vec",
        param=search_params,
        limit=req.top_k * 3,  # 候选数, 让 Milvus refine 工作
        expr=expr,
        output_fields=["image_pk", "category_l1", "category_l2", "brand_id"]
    )
    timings["milvus_ms"] = (time.time() - t0) * 1000

    # === Stage 6: 结果处理 ===
    t0 = time.time()
    out_results = []
    for hit in results[0][:req.top_k]:
        out_results.append({
            "image_pk": hit.entity.get("image_pk"),
            "score": float(hit.score),
            "category_l1": hit.entity.get("category_l1"),
            "category_l2": hit.entity.get("category_l2"),
            "brand_id": hit.entity.get("brand_id"),
        })
    timings["postprocess_ms"] = (time.time() - t0) * 1000

    total_ms = (time.time() - t_total) * 1000

    return SearchResponse(
        results=out_results,
        latency_ms=timings,
        total_ms=total_ms,
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9810)
