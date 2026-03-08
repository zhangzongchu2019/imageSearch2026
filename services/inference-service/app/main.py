"""
inference-service — CLIP ViT-B/32 特征提取微服务 (GPU FP16)

供 write-service（导入时）和 search-service（查询时）共同调用。
输出 256 维 L2 归一化向量（FP32），匹配 Milvus schema。
"""
from __future__ import annotations

import asyncio
import base64
import io
import logging
import os
import threading
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
_projection: torch.Tensor | None = None  # 512 → 256 投影矩阵 (GPU)
_device: torch.device = torch.device("cpu")
_lock = threading.Lock()  # GPU 序列化锁

MODEL_PATH = "/models/ViT-B-32.pt"


def _load_model():
    """加载 CLIP ViT-B/32 并初始化投影矩阵"""
    global _model, _preprocess, _projection, _device
    import open_clip

    # 选择设备
    if torch.cuda.is_available():
        _device = torch.device("cuda")
        logger.info("Using GPU: %s", torch.cuda.get_device_name(0))
    else:
        _device = torch.device("cpu")
        logger.info("GPU not available, falling back to CPU")

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
    torch.load = _orig_torch_load

    model.eval()
    model = model.to(_device)

    # GPU FP16 半精度 — 推理速度翻倍，显存减半
    if _device.type == "cuda":
        model = model.half()
        logger.info("Model converted to FP16")

    _model = model
    _preprocess = preprocess

    # 投影矩阵 (512 → 256)，固定种子确保所有实例一致
    rng = np.random.RandomState(42)
    proj = rng.randn(512, EMBEDDING_DIM).astype(np.float32)
    u, _, vt = np.linalg.svd(proj, full_matrices=False)
    _projection = torch.from_numpy(u).to(_device)  # (512, 256)
    if _device.type == "cuda":
        _projection = _projection.half()

    logger.info(
        "CLIP ViT-B/32 loaded on %s, projection matrix initialized (512 → %d)",
        _device, EMBEDDING_DIM,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_model()
    yield


app = FastAPI(title="inference-service", lifespan=lifespan)


class ExtractRequest(BaseModel):
    image_b64: str


class ExtractResponse(BaseModel):
    global_vec: list[float]
    tags_pred: list[int]
    category_l1_pred: int


def _do_inference(image_bytes: bytes) -> list[float]:
    """GPU FP16 推理 — 通过锁序列化 GPU 访问"""
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    img_tensor = _preprocess(img).unsqueeze(0).to(_device)  # (1, 3, 224, 224)

    if _device.type == "cuda":
        img_tensor = img_tensor.half()

    with _lock:
        with torch.no_grad():
            features = _model.encode_image(img_tensor)  # (1, 512)

    # 投影 + L2 归一化，全程 GPU 计算
    vec_256 = (features[0] @ _projection).float()  # → FP32
    norm = torch.linalg.norm(vec_256)
    if norm > 0:
        vec_256 = vec_256 / norm

    return vec_256.cpu().numpy().tolist()


@app.post("/api/v1/extract", response_model=ExtractResponse)
async def extract(req: ExtractRequest):
    try:
        image_bytes = base64.b64decode(req.image_b64)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid image: {e}")

    loop = asyncio.get_running_loop()
    global_vec = await loop.run_in_executor(None, _do_inference, image_bytes)

    return ExtractResponse(
        global_vec=global_vec,
        tags_pred=[],
        category_l1_pred=0,
    )


@app.get("/healthz")
async def healthz():
    if _model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    return {"status": "ok", "device": str(_device)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8090, log_level="info")
