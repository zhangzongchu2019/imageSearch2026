"""
特征提取引擎 — 远程推理服务 / ONNX Runtime 本地推理

优先级:
  1. 远程推理服务 (INFERENCE_SERVICE_URL) — 生产环境推荐
  2. 本地 ONNX Runtime (需要 .onnx 模型文件)

协议:
  - 输入: RGB 224×224, ImageNet 归一化
  - 输出 0: global_vec (float32, dim=EMBEDDING_DIM, L2-normalized)
  - 输出 1: tags_logits (float32, dim=TAG_VOCAB_SIZE) → Top-K argmax
  - 输出 2: category_logits (float32, dim=CAT_L1_SIZE) → argmax
"""
from __future__ import annotations

import asyncio
import base64
import io
import os
from pathlib import Path
from typing import List, Optional

import numpy as np
import structlog
from PIL import Image

from app.core.config import get_settings
from app.core.metrics import METRICS

logger = structlog.get_logger(__name__)
settings = get_settings()

# 远程推理服务 URL (优先级最高, 支持多实例)
INFERENCE_SERVICE_URL = os.getenv("INFERENCE_SERVICE_URL")
_INFERENCE_SERVICE_URLS = [
    u.strip() for u in (os.getenv("INFERENCE_SERVICE_URLS") or os.getenv("INFERENCE_SERVICE_URL") or "").split(",") if u.strip()
]

# 模型常量 — 必须与训练侧一致
EMBEDDING_DIM = 256
TAG_VOCAB_SIZE = 1000
CAT_L1_SIZE = 200
INPUT_SIZE = 224
IMAGENET_MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32)
IMAGENET_STD = np.array([0.229, 0.224, 0.225], dtype=np.float32)

# 模型文件路径
MODEL_DIR = Path(os.getenv("IMGSRCH_MODEL_DIR", "/models"))
ONNX_MODEL_PATH = MODEL_DIR / "backbone_fp32.onnx"
TENSORRT_ENGINE_PATH = MODEL_DIR / "backbone_fp16.engine"



class FeatureResult:
    """特征提取结果 — 查询侧/写入侧通用"""

    __slots__ = ("global_vec", "tags_pred", "category_l1_pred", "category_l1_conf",
                 "sub_vecs", "model_version", "embedding_dim")

    def __init__(
        self,
        global_vec: List[float],
        tags_pred: List[int],
        category_l1_pred: int,
        category_l1_conf: float = 0.0,
        sub_vecs: Optional[List[List[float]]] = None,
        model_version: str = "unknown",
        embedding_dim: int = EMBEDDING_DIM,
    ):
        self.global_vec = global_vec
        self.tags_pred = tags_pred
        self.category_l1_pred = category_l1_pred
        self.category_l1_conf = category_l1_conf
        self.sub_vecs = sub_vecs
        self.model_version = model_version
        self.embedding_dim = embedding_dim


class FeatureExtractor:
    """GPU/CPU 双模式特征提取

    生产: ONNX Runtime (CPU/GPU provider)
    加速: TensorRT FP16 (可选, 需 .engine 文件)
    限流: CPU fallback 限 10 QPS (信号量)
    """

    def __init__(self):
        self._ort_session = None
        self._trt_engine = None
        self._model_version = "unknown"
        self._cpu_semaphore = asyncio.Semaphore(10)
        self._use_remote = bool(INFERENCE_SERVICE_URL)
        self._http_clients: list = []
        self._http_client = None
        self._rr_counter = 0

        if self._use_remote:
            logger.info(
                "REMOTE_INFERENCE_ENABLED",
                urls=_INFERENCE_SERVICE_URLS,
                count=len(_INFERENCE_SERVICE_URLS),
            )
        else:
            # 尝试加载本地模型
            self._ort_session = self._load_onnx()
            if self._ort_session:
                self._validate_model_outputs()
            else:
                raise RuntimeError(
                    f"No inference backend available. "
                    f"Set INFERENCE_SERVICE_URL for remote inference, "
                    f"or provide ONNX model at {ONNX_MODEL_PATH}."
                )

        # sub-image 特征提取需要单独模型 (当前未加载)
        # 降级行为: sub_vecs=None → pipeline._fallback 自动跳过子图召回路径
        logger.info("sub_image_extraction_degraded",
                     reason="sub-image model not loaded, fallback to tag-only recall")

    # ── 公共接口 ──

    async def extract_query(
        self, query_image_b64: str, device: str = "gpu"
    ) -> FeatureResult:
        """查询侧特征提取"""
        # 优先使用远程推理服务
        if self._use_remote:
            return await self._remote_infer(query_image_b64)

        img_bytes = base64.b64decode(query_image_b64)
        img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
        tensor = self._preprocess(img)

        loop = asyncio.get_event_loop()
        if device == "gpu":
            return await loop.run_in_executor(None, self._onnx_infer, tensor, False)
        else:
            async with self._cpu_semaphore:
                return await loop.run_in_executor(None, self._onnx_infer, tensor, False)

    async def extract_write(self, image_bytes: bytes) -> FeatureResult:
        """写入侧特征提取 — 同模型同归一化"""
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        tensor = self._preprocess(img)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._onnx_infer, tensor, True)

    # ── 远程推理 ──

    async def _remote_infer(self, image_b64: str) -> FeatureResult:
        """调用远程 inference-service 获取特征向量 — 多实例轮询"""
        import httpx

        # 懒初始化多个 httpx 客户端
        if not self._http_clients:
            for _ in _INFERENCE_SERVICE_URLS:
                self._http_clients.append(httpx.AsyncClient(timeout=120.0))

        # Round-robin
        idx = self._rr_counter % len(self._http_clients)
        self._rr_counter += 1
        client = self._http_clients[idx]
        url = _INFERENCE_SERVICE_URLS[idx]

        resp = await client.post(
            f"{url}/api/v1/extract",
            json={"image_b64": image_b64},
        )
        resp.raise_for_status()
        data = resp.json()

        return FeatureResult(
            global_vec=data["global_vec"],
            tags_pred=data.get("tags_pred", []),
            category_l1_pred=data.get("category_l1_pred", 0),
            category_l1_conf=data.get("category_l1_conf", 0.0),
            sub_vecs=None,
            model_version=data.get("model_version", "vitl14-v2"),
            embedding_dim=len(data["global_vec"]),
        )

    # ── 预处理 ──

    def _preprocess(self, img: Image.Image) -> np.ndarray:
        """Resize + ImageNet 归一化 → NCHW float32"""
        img = img.resize((INPUT_SIZE, INPUT_SIZE), Image.BILINEAR)
        arr = np.array(img, dtype=np.float32) / 255.0
        arr = (arr - IMAGENET_MEAN) / IMAGENET_STD
        arr = arr.transpose(2, 0, 1)  # HWC → CHW
        return np.expand_dims(arr, axis=0).astype(np.float32)  # NCHW

    # ── ONNX 推理 ──

    def _onnx_infer(self, tensor: np.ndarray, include_sub: bool) -> FeatureResult:
        """ONNX Runtime 推理 — 真实模型"""
        outputs = self._ort_session.run(None, {"input": tensor})

        # Output 0: global_vec (raw) → L2 normalize
        global_vec_raw = outputs[0][0]
        norm = np.linalg.norm(global_vec_raw)
        if norm > 0:
            global_vec_raw = global_vec_raw / norm
        global_vec = global_vec_raw.tolist()

        # Output 1: tags_logits → Top-5 argmax
        tags_logits = outputs[1][0] if len(outputs) > 1 else np.zeros(TAG_VOCAB_SIZE)
        tags_pred = np.argsort(tags_logits)[-5:][::-1].tolist()

        # Output 2: category_logits → argmax
        cat_logits = outputs[2][0] if len(outputs) > 2 else np.zeros(CAT_L1_SIZE)
        category_l1_pred = int(np.argmax(cat_logits))

        return FeatureResult(
            global_vec=global_vec,
            tags_pred=tags_pred,
            category_l1_pred=category_l1_pred,
            sub_vecs=None,  # sub-image 特征需单独模型, 当前降级跳过子图召回
            model_version=self._model_version,
            embedding_dim=len(global_vec),
        )

    # ── 模型加载 ──

    def _load_onnx(self):
        """加载 ONNX Runtime 引擎"""
        if not ONNX_MODEL_PATH.exists():
            logger.warning("onnx_model_not_found", path=str(ONNX_MODEL_PATH))
            return None
        try:
            import onnxruntime as ort

            providers = ["CUDAExecutionProvider", "CPUExecutionProvider"]
            available = ort.get_available_providers()
            use_providers = [p for p in providers if p in available]

            session = ort.InferenceSession(
                str(ONNX_MODEL_PATH), providers=use_providers
            )

            # 提取模型元信息
            meta = session.get_modelmeta()
            self._model_version = meta.custom_metadata_map.get("version", "unknown")

            logger.info(
                "onnx_model_loaded",
                path=str(ONNX_MODEL_PATH),
                providers=use_providers,
                model_version=self._model_version,
                inputs=[i.name for i in session.get_inputs()],
                outputs=[o.name for o in session.get_outputs()],
            )
            return session

        except Exception as e:
            logger.error("onnx_load_failed", error=str(e))
            return None

    def _validate_model_outputs(self):
        """校验模型输出维度与代码常量一致"""
        outputs = self._ort_session.get_outputs()
        if outputs:
            actual_dim = outputs[0].shape[-1]
            if actual_dim != EMBEDDING_DIM:
                raise RuntimeError(
                    f"Model embedding_dim={actual_dim} != code EMBEDDING_DIM={EMBEDDING_DIM}. "
                    f"请同步更新代码常量或重新训练模型。"
                )
            logger.info("model_dim_validated", embedding_dim=actual_dim)
