"""
inference-service — CLIP ViT-B/32 特征提取微服务

供 write-service（导入时）和 search-service（查询时）共同调用。
输出 256 维 L2 归一化向量，匹配 Milvus schema。
"""
from __future__ import annotations

import asyncio
import base64
import io
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

import numpy as np
import torch
from fastapi import FastAPI, HTTPException
from PIL import Image
from pydantic import BaseModel

logger = logging.getLogger("inference-service")

EMBEDDING_DIM = 256

# Global state filled at startup
_model = None
_preprocess = None
_projection: np.ndarray | None = None  # 512 → 256 线性投影
_infer_pool: ThreadPoolExecutor | None = None  # 推理线程池


MODEL_PATH = "/models/ViT-B-32.pt"


def _load_model():
    """加载 CLIP ViT-B/32 并初始化投影矩阵"""
    global _model, _preprocess, _projection
    import open_clip
    import os

    # PyTorch 2.6+ 默认 weights_only=True，与 CLIP TorchScript 格式不兼容
    _orig_torch_load = torch.load
    torch.load = lambda *a, **kw: _orig_torch_load(*a, **{**kw, "weights_only": False})

    local_path = os.environ.get("CLIP_MODEL_PATH", MODEL_PATH)
    if os.path.isfile(local_path):
        logger.info("Loading CLIP ViT-B/32 from local file: %s", local_path)
        model, _, preprocess = open_clip.create_model_and_transforms(
            "ViT-B-32", pretrained=local_path
        )
    else:
        logger.info("Local model not found at %s, downloading from OpenAI...", local_path)
        model, _, preprocess = open_clip.create_model_and_transforms(
            "ViT-B-32", pretrained="openai"
        )
    model.eval()
    torch.load = _orig_torch_load  # 恢复原始 torch.load
    _model = model
    _preprocess = preprocess

    # 固定随机种子生成投影矩阵（512 → 256），确保所有实例一致
    rng = np.random.RandomState(42)
    proj = rng.randn(512, EMBEDDING_DIM).astype(np.float32)
    # 正交化以保持距离关系
    u, _, vt = np.linalg.svd(proj, full_matrices=False)
    _projection = u  # (512, 256) 正交矩阵

    logger.info("CLIP ViT-B/32 loaded, projection matrix initialized (512 → %d)", EMBEDDING_DIM)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _infer_pool
    _load_model()
    _infer_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="infer")
    yield
    _infer_pool.shutdown(wait=False)


app = FastAPI(title="inference-service", lifespan=lifespan)


class ExtractRequest(BaseModel):
    image_b64: str


class ExtractResponse(BaseModel):
    global_vec: list[float]
    tags_pred: list[int]
    category_l1_pred: int


def _do_inference(image_bytes: bytes) -> list[float]:
    """CPU 推理 — 在线程池中执行，不阻塞事件循环"""
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    img_tensor = _preprocess(img).unsqueeze(0)  # (1, 3, 224, 224)
    with torch.no_grad():
        features = _model.encode_image(img_tensor)  # (1, 512)
    vec_512 = features[0].cpu().numpy().astype(np.float32)
    vec_256 = vec_512 @ _projection
    norm = np.linalg.norm(vec_256)
    if norm > 0:
        vec_256 = vec_256 / norm
    return vec_256.tolist()


@app.post("/api/v1/extract", response_model=ExtractResponse)
async def extract(req: ExtractRequest):
    try:
        image_bytes = base64.b64decode(req.image_b64)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid image: {e}")

    loop = asyncio.get_running_loop()
    global_vec = await loop.run_in_executor(_infer_pool, _do_inference, image_bytes)

    return ExtractResponse(
        global_vec=global_vec,
        tags_pred=[],
        category_l1_pred=0,
    )


@app.get("/healthz")
async def healthz():
    if _model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8090, log_level="info")
