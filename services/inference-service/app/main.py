"""
inference-service — CLIP ViT-B/32 特征提取 + 零样本分类微服务 (GPU FP16)

v1.4: 新增零样本品类识别和标签提取 (CLIP zero-shot classification)
  - 图片向量与预定义文本向量做余弦相似度
  - 品类: 取最高分类目 + 置信度
  - 标签: 取 top-K 高分标签

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
from typing import Dict, List, Tuple

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
_tokenizer = None
_projection: torch.Tensor | None = None  # 512 → 256 投影矩阵 (GPU)
_device: torch.device = torch.device("cpu")
_lock = threading.Lock()  # GPU 序列化锁

# 零样本分类 — 预计算文本向量
_category_text_features: torch.Tensor | None = None  # (N_cat, 512)
_tag_text_features: torch.Tensor | None = None        # (N_tag, 512)
_category_text_features_256: torch.Tensor | None = None  # (N_cat, 256) 投影后
_tag_text_features_256: torch.Tensor | None = None        # (N_tag, 256) 投影后
_category_names: List[str] = []
_tag_names: List[str] = []

MODEL_PATH = "/models/ViT-B-32.pt"

# ── 品类和标签词表 ──

CATEGORIES = {
    "服装": ["a photo of clothing", "a photo of garment", "a photo of dress shirt jacket coat pants skirt"],
    "鞋帽": ["a photo of shoes", "a photo of boots sneakers heels hat cap"],
    "箱包": ["a photo of bag", "a photo of handbag backpack wallet luggage purse"],
    "珠宝玉器": ["a photo of jewelry", "a photo of jade gold diamond gem ring bracelet"],
    "首饰配饰": ["a photo of accessories", "a photo of necklace earring brooch hairpin"],
}

TAGS = {
    # 服装
    "上衣": "a photo of a top shirt blouse",
    "裤子": "a photo of pants trousers",
    "裙子": "a photo of a skirt dress",
    "外套": "a photo of a jacket coat outerwear",
    "T恤": "a photo of a t-shirt tee",
    # 鞋
    "运动鞋": "a photo of sneakers running shoes",
    "高跟鞋": "a photo of high heels pumps",
    "皮鞋": "a photo of leather shoes",
    "靴子": "a photo of boots",
    # 包
    "手提包": "a photo of a handbag tote bag",
    "双肩包": "a photo of a backpack",
    "钱包": "a photo of a wallet",
    "旅行箱": "a photo of luggage suitcase",
    # 珠宝
    "翡翠": "a photo of jade jadeite",
    "黄金": "a photo of gold jewelry",
    "钻石": "a photo of diamond",
    "手镯": "a photo of a bracelet bangle",
    "戒指": "a photo of a ring",
    "项链": "a photo of a necklace",
    "耳环": "a photo of earrings",
    # 材质/风格
    "真皮": "a photo of genuine leather product",
    "PU皮": "a photo of PU leather synthetic product",
    "棉质": "a photo of cotton fabric product",
    "复古风": "a photo of vintage retro style product",
    "简约风": "a photo of minimalist simple style product",
    # 颜色
    "红色": "a photo of a red product",
    "蓝色": "a photo of a blue product",
    "黑色": "a photo of a black product",
    "白色": "a photo of a white product",
    "棕色": "a photo of a brown product",
    # 季节
    "春季": "a photo of spring season fashion",
    "夏季": "a photo of summer season fashion",
    "秋季": "a photo of autumn fall season fashion",
    "冬季": "a photo of winter season fashion",
}


def _load_model():
    """加载 CLIP ViT-B/32 并初始化投影矩阵 + 零样本分类文本向量"""
    global _model, _preprocess, _tokenizer, _projection, _device
    global _category_text_features, _tag_text_features, _category_names, _tag_names
    global _category_text_features_256, _tag_text_features_256
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

    _tokenizer = open_clip.get_tokenizer("ViT-B-32")

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

    # ── 预计算零样本分类文本向量 ──
    logger.info("Computing zero-shot text features...")

    # 品类文本向量
    _category_names = list(CATEGORIES.keys())
    cat_texts = []
    for cat_name in _category_names:
        # 每个品类多个描述取平均
        cat_texts.extend(CATEGORIES[cat_name])

    cat_tokens = _tokenizer(cat_texts).to(_device)
    with torch.no_grad():
        cat_feats = _model.encode_text(cat_tokens)  # (N, 512)
    cat_feats = cat_feats / cat_feats.norm(dim=-1, keepdim=True)

    # 每个品类多个描述取平均
    idx = 0
    cat_avg = []
    for cat_name in _category_names:
        n = len(CATEGORIES[cat_name])
        avg = cat_feats[idx:idx + n].mean(dim=0)
        avg = avg / avg.norm()
        cat_avg.append(avg)
        idx += n
    _category_text_features = torch.stack(cat_avg)  # (5, 512)

    # 标签文本向量
    _tag_names = list(TAGS.keys())
    tag_texts = [TAGS[t] for t in _tag_names]
    tag_tokens = _tokenizer(tag_texts).to(_device)
    with torch.no_grad():
        tag_feats = _model.encode_text(tag_tokens)  # (N, 512)
    _tag_text_features = tag_feats / tag_feats.norm(dim=-1, keepdim=True)

    # ── 预计算 256 维投影文本特征（用于从已有 256 维向量分类）──
    proj_fp32 = _projection.float()
    cat_proj = (_category_text_features.float() @ proj_fp32)  # (N_cat, 256)
    cat_proj = cat_proj / cat_proj.norm(dim=-1, keepdim=True)
    _category_text_features_256 = cat_proj.to(_device)

    tag_proj = (_tag_text_features.float() @ proj_fp32)  # (N_tag, 256)
    tag_proj = tag_proj / tag_proj.norm(dim=-1, keepdim=True)
    _tag_text_features_256 = tag_proj.to(_device)

    logger.info(
        "CLIP ViT-B/32 loaded on %s, %d categories, %d tags ready (512d + 256d text features)",
        _device, len(_category_names), len(_tag_names),
    )


def _do_inference(image_bytes: bytes) -> dict:
    """GPU FP16 推理 — 向量提取 + 零样本品类/标签分类"""
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    img_tensor = _preprocess(img).unsqueeze(0).to(_device)  # (1, 3, 224, 224)

    if _device.type == "cuda":
        img_tensor = img_tensor.half()

    with _lock:
        with torch.no_grad():
            image_features = _model.encode_image(img_tensor)  # (1, 512)

    # ── 1. 投影 + L2 归一化 → 256d 向量 ──
    vec_256 = (image_features[0] @ _projection).float()
    norm = torch.linalg.norm(vec_256)
    if norm > 0:
        vec_256 = vec_256 / norm
    global_vec = vec_256.cpu().numpy().tolist()

    # ── 2. 零样本品类分类 ──
    img_feat_norm = image_features / image_features.norm(dim=-1, keepdim=True)  # (1, 512)
    cat_sims = (img_feat_norm @ _category_text_features.T).squeeze(0).float()  # (N_cat,)
    cat_idx = cat_sims.argmax().item()
    cat_conf = cat_sims[cat_idx].item()
    category_l1 = _category_names[cat_idx]

    # ── 3. 零样本标签提取 (top-8) ──
    tag_sims = (img_feat_norm @ _tag_text_features.T).squeeze(0).float()  # (N_tag,)
    top_k = min(8, len(_tag_names))
    top_indices = tag_sims.topk(top_k).indices.tolist()
    top_scores = tag_sims.topk(top_k).values.tolist()
    # 只保留相似度 > 0.2 的标签
    tags = [_tag_names[i] for i, s in zip(top_indices, top_scores) if s > 0.2]

    return {
        "global_vec": global_vec,
        "category_l1": category_l1,
        "category_l1_conf": round(cat_conf, 4),
        "tags": tags,
    }


def _classify_from_vec256(vec_256: List[float], top_k_tags: int = 8, tag_threshold: float = 0.2) -> dict:
    """从 256 维投影向量做零样本品类/标签分类"""
    vec_t = torch.tensor(vec_256, dtype=torch.float32, device=_device).unsqueeze(0)  # (1, 256)
    vec_t = vec_t / vec_t.norm(dim=-1, keepdim=True)

    # 品类分类
    cat_sims = (vec_t @ _category_text_features_256.float().T).squeeze(0)  # (N_cat,)
    cat_idx = cat_sims.argmax().item()
    cat_conf = cat_sims[cat_idx].item()
    category_l1 = _category_names[cat_idx]

    # 标签提取
    tag_sims = (vec_t @ _tag_text_features_256.float().T).squeeze(0)  # (N_tag,)
    top_k = min(top_k_tags, len(_tag_names))
    topk_result = tag_sims.topk(top_k)
    top_indices = topk_result.indices.tolist()
    top_scores = topk_result.values.tolist()
    tags = [
        {"name": _tag_names[i], "score": round(s, 4)}
        for i, s in zip(top_indices, top_scores)
        if s > tag_threshold
    ]

    return {
        "category_l1": category_l1,
        "category_l1_conf": round(cat_conf, 4),
        "category_l1_id": cat_idx,
        "tags": tags,
    }


def _classify_batch_vec256(vectors: List[List[float]], top_k_tags: int = 8, tag_threshold: float = 0.2) -> List[dict]:
    """批量 256 维向量分类"""
    batch_t = torch.tensor(vectors, dtype=torch.float32, device=_device)  # (B, 256)
    batch_t = batch_t / batch_t.norm(dim=-1, keepdim=True)

    cat_text = _category_text_features_256.float()  # (N_cat, 256)
    tag_text = _tag_text_features_256.float()        # (N_tag, 256)

    cat_sims = batch_t @ cat_text.T  # (B, N_cat)
    tag_sims = batch_t @ tag_text.T  # (B, N_tag)

    top_k = min(top_k_tags, len(_tag_names))
    results = []
    for i in range(len(vectors)):
        cat_idx = cat_sims[i].argmax().item()
        cat_conf = cat_sims[i][cat_idx].item()

        topk_result = tag_sims[i].topk(top_k)
        top_indices = topk_result.indices.tolist()
        top_scores = topk_result.values.tolist()
        tags = [
            {"name": _tag_names[j], "score": round(s, 4)}
            for j, s in zip(top_indices, top_scores)
            if s > tag_threshold
        ]

        results.append({
            "category_l1": _category_names[cat_idx],
            "category_l1_conf": round(cat_conf, 4),
            "category_l1_id": cat_idx,
            "tags": tags,
        })
    return results


@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_model()
    yield


app = FastAPI(title="inference-service", lifespan=lifespan)


class ExtractRequest(BaseModel):
    image_b64: str


class ExtractResponse(BaseModel):
    global_vec: list[float]
    category_l1: str
    category_l1_conf: float
    tags: list[str]
    tags_pred: list[int]       # 兼容旧接口
    category_l1_pred: int      # 兼容旧接口


@app.post("/api/v1/extract", response_model=ExtractResponse)
async def extract(req: ExtractRequest):
    try:
        image_bytes = base64.b64decode(req.image_b64)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid image: {e}")

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, _do_inference, image_bytes)

    # 标签编码 (简单 hash)
    tag_ids = [hash(t) % 4096 for t in result["tags"]]
    cat_id = _category_names.index(result["category_l1"]) if result["category_l1"] in _category_names else 0

    return ExtractResponse(
        global_vec=result["global_vec"],
        category_l1=result["category_l1"],
        category_l1_conf=result["category_l1_conf"],
        tags=result["tags"],
        tags_pred=tag_ids,
        category_l1_pred=cat_id,
    )


# ── 向量分类接口 ──


class TagItem(BaseModel):
    name: str
    score: float


class ClassifyRequest(BaseModel):
    global_vec: list[float]
    top_k_tags: int = 8
    tag_threshold: float = 0.2


class ClassifyResponse(BaseModel):
    category_l1: str
    category_l1_conf: float
    category_l1_id: int
    tags: list[TagItem]


class ClassifyBatchRequest(BaseModel):
    vectors: list[list[float]]
    top_k_tags: int = 8
    tag_threshold: float = 0.2


class ClassifyBatchResponse(BaseModel):
    results: list[ClassifyResponse]
    count: int


@app.post("/api/v1/classify", response_model=ClassifyResponse)
async def classify(req: ClassifyRequest):
    """从 256 维向量获取品类和标签"""
    if len(req.global_vec) != EMBEDDING_DIM:
        raise HTTPException(
            status_code=400,
            detail=f"Expected {EMBEDDING_DIM}-dim vector, got {len(req.global_vec)}",
        )
    result = _classify_from_vec256(req.global_vec, req.top_k_tags, req.tag_threshold)
    return ClassifyResponse(
        category_l1=result["category_l1"],
        category_l1_conf=result["category_l1_conf"],
        category_l1_id=result["category_l1_id"],
        tags=[TagItem(**t) for t in result["tags"]],
    )


@app.post("/api/v1/classify_batch", response_model=ClassifyBatchResponse)
async def classify_batch(req: ClassifyBatchRequest):
    """批量 256 维向量分类，单次最多 1000 条"""
    if len(req.vectors) > 1000:
        raise HTTPException(status_code=400, detail="Max 1000 vectors per batch")
    for i, v in enumerate(req.vectors):
        if len(v) != EMBEDDING_DIM:
            raise HTTPException(
                status_code=400,
                detail=f"Vector[{i}]: expected {EMBEDDING_DIM}-dim, got {len(v)}",
            )

    loop = asyncio.get_running_loop()
    results = await loop.run_in_executor(
        None, _classify_batch_vec256, req.vectors, req.top_k_tags, req.tag_threshold,
    )
    return ClassifyBatchResponse(
        results=[
            ClassifyResponse(
                category_l1=r["category_l1"],
                category_l1_conf=r["category_l1_conf"],
                category_l1_id=r["category_l1_id"],
                tags=[TagItem(**t) for t in r["tags"]],
            )
            for r in results
        ],
        count=len(results),
    )


@app.get("/healthz")
async def healthz():
    if _model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    return {"status": "ok", "device": str(_device), "categories": len(_category_names), "tags": len(_tag_names)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8090, log_level="info")
