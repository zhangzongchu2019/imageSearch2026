"""
inference-service — 两级分类特征提取微服务 (GPU FP16)

v2.0: ViT-L-14 + FashionSigLIP 两级分类架构
  Stage 1: ViT-L-14 → 768维特征 → 256维搜索向量 + L1大类分类 (18类)
  Stage 2: FashionSigLIP (时尚类 L2/L3) / ViT-L-14零样本 (其他类 L2/L3)

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
from typing import Dict, List, Optional

import numpy as np
import torch
from fastapi import FastAPI, HTTPException
from PIL import Image
from pydantic import BaseModel

from app.taxonomy import (
    FASHION_L1_CODES,
    FASHION_L2,
    FASHION_L3,
    L1_CATEGORIES,
    L1_CODES,
    L1_CODE_TO_NAME,
    L1_NAME_TO_CODE,
    L1_NAMES,
    NONFASHION_L2,
    TAGS,
    TAG_NAMES,
    get_l2_for_l1,
    get_l3_for_l2,
    is_fashion,
)

logger = logging.getLogger("inference-service")

EMBEDDING_DIM = 256
VITL14_DIM = 768
MODEL_VERSION = "vitl14-v2"

# ---------------------------------------------------------------------------
# Global state — filled at startup
# ---------------------------------------------------------------------------

# Stage 1: ViT-L-14 (主模型)
_vitl14_model = None
_vitl14_preprocess = None
_vitl14_tokenizer = None
_projection: torch.Tensor | None = None  # (768, 256)
_device: torch.device = torch.device("cpu")
_lock = threading.Lock()

# L1 分类文本特征 (ViT-L-14 空间)
_l1_text_features: torch.Tensor | None = None        # (N_l1, 768)
_l1_text_features_256: torch.Tensor | None = None     # (N_l1, 256)
_l1_names: List[str] = []
_l1_codes: List[int] = []

# 非时尚类 L2 文本特征 (ViT-L-14 空间)
_nonfashion_l2_features: Dict[int, torch.Tensor] = {}   # l1_code → (N_l2, 768)
_nonfashion_l2_names: Dict[int, List[str]] = {}          # l1_code → [name, ...]
_nonfashion_l2_codes: Dict[int, List[int]] = {}          # l1_code → [code, ...]

# 标签文本特征 (ViT-L-14 空间)
_tag_text_features: torch.Tensor | None = None        # (N_tag, 768)
_tag_text_features_256: torch.Tensor | None = None     # (N_tag, 256)
_tag_names: List[str] = []

# Stage 2: FashionSigLIP (时尚专用)
_fashion_model = None
_fashion_processor = None
_fashion_l2_features: Dict[int, torch.Tensor] = {}    # l1_code → (N_l2, dim)
_fashion_l2_names: Dict[int, List[str]] = {}
_fashion_l2_codes: Dict[int, List[int]] = {}
_fashion_l3_features: Dict[int, torch.Tensor] = {}    # l2_code → (N_l3, dim)
_fashion_l3_names: Dict[int, List[str]] = {}
_fashion_l3_codes: Dict[int, List[int]] = {}
_fashion_tokenizer = None
_fashion_dim: int = 0

# ---------------------------------------------------------------------------
# Model loading
# ---------------------------------------------------------------------------

VITL14_MODEL_PATH = "/models/ViT-L-14.pt"
FASHION_MODEL_PATH = "/models/marqo-fashionsiglip"
FASHION_TOKENIZER_PATH = "/models/vit-b-16-siglip-tokenize"


def _encode_text_features(tokenizer, model, prompts_dict, device, multi_prompt=True):
    """编码一组 {name: [prompt,...]} 或 {name: prompt} 的文本特征"""
    names = []
    all_texts = []
    counts = []  # 每个 name 对应几条 prompt

    for name_key, prompts in prompts_dict.items():
        names.append(name_key)
        if isinstance(prompts, str):
            prompts = [prompts]
        all_texts.extend(prompts)
        counts.append(len(prompts))

    tokens = tokenizer(all_texts).to(device)
    with torch.no_grad():
        feats = model.encode_text(tokens)
    feats = feats / feats.norm(dim=-1, keepdim=True)

    if multi_prompt and any(c > 1 for c in counts):
        # 多 prompt 取平均
        averaged = []
        idx = 0
        for c in counts:
            avg = feats[idx:idx + c].mean(dim=0)
            avg = avg / avg.norm()
            averaged.append(avg)
            idx += c
        return names, torch.stack(averaged)
    else:
        return names, feats


def _load_vitl14():
    """加载 Stage 1: ViT-L-14"""
    global _vitl14_model, _vitl14_preprocess, _vitl14_tokenizer, _projection
    global _l1_text_features, _l1_text_features_256, _l1_names, _l1_codes
    global _nonfashion_l2_features, _nonfashion_l2_names, _nonfashion_l2_codes
    global _tag_text_features, _tag_text_features_256, _tag_names
    global _device

    import open_clip

    # 设备选择
    if torch.cuda.is_available():
        _device = torch.device("cuda")
        logger.info("Using GPU: %s", torch.cuda.get_device_name(0))
    else:
        _device = torch.device("cpu")
        logger.info("GPU not available, falling back to CPU")

    # PyTorch 2.6+ 兼容
    _orig_torch_load = torch.load
    torch.load = lambda *a, **kw: _orig_torch_load(*a, **{**kw, "weights_only": False})

    local_path = os.environ.get("CLIP_MODEL_PATH", VITL14_MODEL_PATH)
    model_name = "ViT-L-14"
    if os.path.isfile(local_path):
        logger.info("Loading %s from local: %s", model_name, local_path)
        model, _, preprocess = open_clip.create_model_and_transforms(
            model_name, pretrained=local_path
        )
    else:
        logger.info("Downloading %s from OpenAI...", model_name)
        model, _, preprocess = open_clip.create_model_and_transforms(
            model_name, pretrained="openai"
        )
    torch.load = _orig_torch_load

    _vitl14_tokenizer = open_clip.get_tokenizer(model_name)
    model.eval()
    model = model.to(_device)
    if _device.type == "cuda":
        model = model.half()
        logger.info("ViT-L-14 converted to FP16")
    _vitl14_model = model
    _vitl14_preprocess = preprocess

    # 投影矩阵 (768 → 256)
    rng = np.random.RandomState(42)
    proj = rng.randn(VITL14_DIM, EMBEDDING_DIM).astype(np.float32)
    u, _, vt = np.linalg.svd(proj, full_matrices=False)
    _projection = torch.from_numpy(u).to(_device)
    if _device.type == "cuda":
        _projection = _projection.half()

    # ── L1 品类文本特征 ──
    l1_prompts = {}
    _l1_codes = L1_CODES
    _l1_names = [L1_CODE_TO_NAME[c] for c in _l1_codes]
    for code in _l1_codes:
        info = L1_CATEGORIES[code]
        l1_prompts[info["name"]] = info["prompts"]

    names, feats = _encode_text_features(
        _vitl14_tokenizer, _vitl14_model, l1_prompts, _device
    )
    _l1_text_features = feats  # (N_l1, 768)

    # 投影到 256 维
    proj_fp32 = _projection.float()
    l1_proj = (_l1_text_features.float() @ proj_fp32)
    l1_proj = l1_proj / l1_proj.norm(dim=-1, keepdim=True)
    _l1_text_features_256 = l1_proj.to(_device)

    # ── 非时尚类 L2 文本特征 ──
    for l1_code, l2_dict in NONFASHION_L2.items():
        prompts = {}
        codes = []
        for l2_code, info in l2_dict.items():
            prompts[info["name"]] = info["prompts"]
            codes.append(l2_code)
        l2_names, l2_feats = _encode_text_features(
            _vitl14_tokenizer, _vitl14_model, prompts, _device
        )
        _nonfashion_l2_features[l1_code] = l2_feats
        _nonfashion_l2_names[l1_code] = l2_names
        _nonfashion_l2_codes[l1_code] = codes

    # ── 标签文本特征 ──
    _tag_names = TAG_NAMES
    tag_prompts = {name: TAGS[name] for name in _tag_names}
    _, tag_feats = _encode_text_features(
        _vitl14_tokenizer, _vitl14_model, tag_prompts, _device
    )
    _tag_text_features = tag_feats

    tag_proj = (_tag_text_features.float() @ proj_fp32)
    tag_proj = tag_proj / tag_proj.norm(dim=-1, keepdim=True)
    _tag_text_features_256 = tag_proj.to(_device)

    logger.info(
        "ViT-L-14 loaded: %d L1 categories, %d tags, %d non-fashion L2 groups",
        len(_l1_names), len(_tag_names), len(_nonfashion_l2_features),
    )


def _load_fashion_siglip():
    """加载 Stage 2: FashionSigLIP"""
    global _fashion_model, _fashion_processor, _fashion_tokenizer, _fashion_dim
    global _fashion_l2_features, _fashion_l2_names, _fashion_l2_codes
    global _fashion_l3_features, _fashion_l3_names, _fashion_l3_codes

    model_path = os.environ.get("FASHION_MODEL_PATH", FASHION_MODEL_PATH)
    tokenizer_path = os.environ.get("FASHION_TOKENIZER_PATH", FASHION_TOKENIZER_PATH)

    try:
        import open_clip
        from open_clip.tokenizer import HFTokenizer

        # 离线加载: ViT-B-16-SigLIP 架构 + 本地权重 + 本地 tokenizer
        _orig_torch_load = torch.load
        torch.load = lambda *a, **kw: _orig_torch_load(*a, **{**kw, "weights_only": False})

        weights_file = os.path.join(model_path, "open_clip_pytorch_model.bin")
        model, _, preprocess = open_clip.create_model_and_transforms(
            "ViT-B-16-SigLIP", pretrained=weights_file,
        )
        torch.load = _orig_torch_load

        tokenizer = HFTokenizer(tokenizer_path, context_length=64, clean="canonicalize")

        model.eval()
        model = model.to(_device)
        if _device.type == "cuda":
            model = model.half()
            logger.info("FashionSigLIP converted to FP16")

        _fashion_model = model
        _fashion_processor = preprocess
        _fashion_tokenizer = tokenizer

        # 探测维度
        test_tokens = tokenizer(["test"]).to(_device)
        with torch.no_grad():
            test_feat = model.encode_text(test_tokens)
        _fashion_dim = test_feat.shape[-1]
        logger.info("FashionSigLIP loaded (offline), dim=%d", _fashion_dim)

    except Exception as e:
        logger.warning("Failed to load FashionSigLIP: %s — fashion L2/L3 will use ViT-L-14 fallback", e)
        _fashion_model = None
        return

    # ── 时尚类 L2 文本特征 (FashionSigLIP 空间) ──
    for l1_code, l2_dict in FASHION_L2.items():
        prompts = {}
        codes = []
        for l2_code, info in l2_dict.items():
            prompts[info["name"]] = info["prompts"]
            codes.append(l2_code)
        l2_names, l2_feats = _encode_text_features(
            _fashion_tokenizer, _fashion_model, prompts, _device
        )
        _fashion_l2_features[l1_code] = l2_feats
        _fashion_l2_names[l1_code] = l2_names
        _fashion_l2_codes[l1_code] = codes

    # ── 时尚类 L3 文本特征 (FashionSigLIP 空间) ──
    for l2_code, l3_dict in FASHION_L3.items():
        prompts = {}
        codes = []
        for l3_code, info in l3_dict.items():
            prompts[info["name"]] = info["prompts"]
            codes.append(l3_code)
        l3_names, l3_feats = _encode_text_features(
            _fashion_tokenizer, _fashion_model, prompts, _device
        )
        _fashion_l3_features[l2_code] = l3_feats
        _fashion_l3_names[l2_code] = l3_names
        _fashion_l3_codes[l2_code] = codes

    logger.info(
        "FashionSigLIP text features: %d L2 groups, %d L3 groups",
        len(_fashion_l2_features), len(_fashion_l3_features),
    )


def _load_models():
    """加载所有模型"""
    _load_vitl14()
    _load_fashion_siglip()
    logger.info("All models loaded successfully")


# ---------------------------------------------------------------------------
# Inference pipeline
# ---------------------------------------------------------------------------


def _classify_l2_l3_fashion(img: Image.Image, l1_code: int) -> dict:
    """Stage 2: FashionSigLIP 时尚类 L2/L3 分类"""
    if _fashion_model is None or l1_code not in _fashion_l2_features:
        return {"category_l2": "", "category_l2_conf": 0.0, "category_l2_code": 0,
                "category_l3": "", "category_l3_conf": 0.0, "category_l3_code": 0}

    # 编码图片
    img_tensor = _fashion_processor(img).unsqueeze(0).to(_device)
    if _device.type == "cuda":
        img_tensor = img_tensor.half()
    with _lock:
        with torch.no_grad():
            fashion_feat = _fashion_model.encode_image(img_tensor)  # (1, dim)
    fashion_feat = fashion_feat / fashion_feat.norm(dim=-1, keepdim=True)

    # L2 分类
    l2_feats = _fashion_l2_features[l1_code]  # (N_l2, dim)
    l2_sims = (fashion_feat @ l2_feats.T).squeeze(0).float()
    l2_idx = l2_sims.argmax().item()
    l2_conf = l2_sims[l2_idx].item()
    l2_name = _fashion_l2_names[l1_code][l2_idx]
    l2_code = _fashion_l2_codes[l1_code][l2_idx]

    # L3 分类 (如果该 L2 有 L3 定义)
    l3_name, l3_conf, l3_code = "", 0.0, 0
    if l2_code in _fashion_l3_features:
        l3_feats = _fashion_l3_features[l2_code]
        l3_sims = (fashion_feat @ l3_feats.T).squeeze(0).float()
        l3_idx = l3_sims.argmax().item()
        l3_conf = l3_sims[l3_idx].item()
        l3_name = _fashion_l3_names[l2_code][l3_idx]
        l3_code = _fashion_l3_codes[l2_code][l3_idx]

    return {
        "category_l2": l2_name, "category_l2_conf": round(l2_conf, 4), "category_l2_code": l2_code,
        "category_l3": l3_name, "category_l3_conf": round(l3_conf, 4), "category_l3_code": l3_code,
    }


def _classify_l2_nonfashion(img_feat_768: torch.Tensor, l1_code: int) -> dict:
    """Stage 2: ViT-L-14 零样本非时尚类 L2 分类 (复用 Stage 1 特征, 零额外开销)"""
    if l1_code not in _nonfashion_l2_features:
        return {"category_l2": "", "category_l2_conf": 0.0, "category_l2_code": 0,
                "category_l3": "", "category_l3_conf": 0.0, "category_l3_code": 0}

    l2_feats = _nonfashion_l2_features[l1_code]
    l2_sims = (img_feat_768 @ l2_feats.T).squeeze(0).float()
    l2_idx = l2_sims.argmax().item()
    l2_conf = l2_sims[l2_idx].item()
    l2_name = _nonfashion_l2_names[l1_code][l2_idx]
    l2_code = _nonfashion_l2_codes[l1_code][l2_idx]

    return {
        "category_l2": l2_name, "category_l2_conf": round(l2_conf, 4), "category_l2_code": l2_code,
        "category_l3": "", "category_l3_conf": 0.0, "category_l3_code": 0,
    }


def _do_inference(image_bytes: bytes) -> dict:
    """两级分类推理管线

    Stage 1: ViT-L-14 → 768维 → 256维搜索向量 + L1 分类 + 标签
    Stage 2: L2/L3 分类 (时尚→FashionSigLIP, 其他→ViT-L-14零样本)
    """
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    img_tensor = _vitl14_preprocess(img).unsqueeze(0).to(_device)
    if _device.type == "cuda":
        img_tensor = img_tensor.half()

    # ── Stage 1: ViT-L-14 特征提取 ──
    with _lock:
        with torch.no_grad():
            image_features = _vitl14_model.encode_image(img_tensor)  # (1, 768)

    # 1. 投影 + L2 归一化 → 256d 搜索向量
    vec_256 = (image_features[0] @ _projection).float()
    norm = torch.linalg.norm(vec_256)
    if norm > 0:
        vec_256 = vec_256 / norm
    global_vec = vec_256.cpu().numpy().tolist()

    # 2. L1 大类分类
    img_feat_norm = image_features / image_features.norm(dim=-1, keepdim=True)  # (1, 768)
    l1_sims = (img_feat_norm @ _l1_text_features.T).squeeze(0).float()  # (N_l1,)
    l1_idx = l1_sims.argmax().item()
    l1_conf = l1_sims[l1_idx].item()
    l1_name = _l1_names[l1_idx]
    l1_code = _l1_codes[l1_idx]

    # 3. 标签提取 (top-8, threshold > 0.2)
    tag_sims = (img_feat_norm @ _tag_text_features.T).squeeze(0).float()
    top_k = min(8, len(_tag_names))
    topk_result = tag_sims.topk(top_k)
    top_indices = topk_result.indices.tolist()
    top_scores = topk_result.values.tolist()
    tags = [_tag_names[i] for i, s in zip(top_indices, top_scores) if s > 0.2]

    # ── Stage 2: L2/L3 细分类 ──
    if is_fashion(l1_code):
        l2l3 = _classify_l2_l3_fashion(img, l1_code)
    else:
        l2l3 = _classify_l2_nonfashion(img_feat_norm, l1_code)

    return {
        "global_vec": global_vec,
        "category_l1": l1_name,
        "category_l1_conf": round(l1_conf, 4),
        "category_l1_code": l1_code,
        "tags": tags,
        **l2l3,
    }


# ---------------------------------------------------------------------------
# 256维向量分类 (用于已入库向量的重分类, 仅 L1)
# ---------------------------------------------------------------------------


def _classify_from_vec256(vec_256: List[float], top_k_tags: int = 8, tag_threshold: float = 0.2) -> dict:
    """从 256 维投影向量做 L1 品类/标签分类 (L2/L3 不可靠, 不返回)"""
    vec_t = torch.tensor(vec_256, dtype=torch.float32, device=_device).unsqueeze(0)
    vec_t = vec_t / vec_t.norm(dim=-1, keepdim=True)

    cat_sims = (vec_t @ _l1_text_features_256.float().T).squeeze(0)
    cat_idx = cat_sims.argmax().item()
    cat_conf = cat_sims[cat_idx].item()

    tag_sims = (vec_t @ _tag_text_features_256.float().T).squeeze(0)
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
        "category_l1": _l1_names[cat_idx],
        "category_l1_conf": round(cat_conf, 4),
        "category_l1_id": _l1_codes[cat_idx],
        "tags": tags,
    }


def _classify_batch_vec256(vectors: List[List[float]], top_k_tags: int = 8, tag_threshold: float = 0.2) -> List[dict]:
    """批量 256 维向量 L1 分类"""
    batch_t = torch.tensor(vectors, dtype=torch.float32, device=_device)
    batch_t = batch_t / batch_t.norm(dim=-1, keepdim=True)

    cat_text = _l1_text_features_256.float()
    tag_text = _tag_text_features_256.float()

    cat_sims = batch_t @ cat_text.T
    tag_sims = batch_t @ tag_text.T

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
            "category_l1": _l1_names[cat_idx],
            "category_l1_conf": round(cat_conf, 4),
            "category_l1_id": _l1_codes[cat_idx],
            "tags": tags,
        })
    return results


# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_models()
    yield


app = FastAPI(title="inference-service", version=MODEL_VERSION, lifespan=lifespan)


# ── Request / Response models ──

class ExtractRequest(BaseModel):
    image_b64: str


class ExtractResponse(BaseModel):
    global_vec: list[float]
    category_l1: str
    category_l1_conf: float
    category_l2: str = ""
    category_l2_conf: float = 0.0
    category_l3: str = ""
    category_l3_conf: float = 0.0
    tags: list[str]
    # 编码字段 (供 write-service / Milvus 使用)
    tags_pred: list[int]
    category_l1_pred: int
    category_l2_pred: int = 0
    category_l3_pred: int = 0
    model_version: str = MODEL_VERSION


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


# ── Endpoints ──


@app.post("/api/v1/extract", response_model=ExtractResponse)
async def extract(req: ExtractRequest):
    try:
        image_bytes = base64.b64decode(req.image_b64)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid image: {e}")

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, _do_inference, image_bytes)

    tag_ids = [hash(t) % 4096 for t in result["tags"]]

    return ExtractResponse(
        global_vec=result["global_vec"],
        category_l1=result["category_l1"],
        category_l1_conf=result["category_l1_conf"],
        category_l2=result.get("category_l2", ""),
        category_l2_conf=result.get("category_l2_conf", 0.0),
        category_l3=result.get("category_l3", ""),
        category_l3_conf=result.get("category_l3_conf", 0.0),
        tags=result["tags"],
        tags_pred=tag_ids,
        category_l1_pred=result["category_l1_code"],
        category_l2_pred=result.get("category_l2_code", 0),
        category_l3_pred=result.get("category_l3_code", 0),
        model_version=MODEL_VERSION,
    )


@app.post("/api/v1/classify", response_model=ClassifyResponse)
async def classify(req: ClassifyRequest):
    """从 256 维向量获取 L1 品类和标签"""
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
    """批量 256 维向量 L1 分类，单次最多 1000 条"""
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
    if _vitl14_model is None:
        raise HTTPException(status_code=503, detail="ViT-L-14 not loaded")
    return {
        "status": "ok",
        "device": str(_device),
        "model_version": MODEL_VERSION,
        "vitl14": "loaded",
        "fashion_siglip": "loaded" if _fashion_model is not None else "not_loaded",
        "l1_categories": len(_l1_names),
        "l2_fashion_groups": len(_fashion_l2_features),
        "l2_nonfashion_groups": len(_nonfashion_l2_features),
        "l3_fashion_groups": len(_fashion_l3_features),
        "tags": len(_tag_names),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8090, log_level="info")
