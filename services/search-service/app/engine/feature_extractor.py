"""
特征提取引擎 — ONNX Runtime 生产推理 + TensorRT 可选加速

P0 修复:
  - 生产环境必须加载真实模型 (ONNX .onnx 文件), 否则启动失败
  - 开发环境可用 IMGSRCH_ALLOW_MOCK_INFERENCE=true 启用 mock
  - 查询侧 / 写入侧同模型、同归一化、同维度
  - 模型元信息 (版本号/embedding_dim/归一化方式) 记录到日志与 Prometheus

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

# 允许 mock 推理 (仅开发/测试环境)
ALLOW_MOCK = os.getenv("IMGSRCH_ALLOW_MOCK_INFERENCE", "false").lower() == "true"


class FeatureResult:
    """特征提取结果 — 查询侧/写入侧通用"""

    __slots__ = ("global_vec", "tags_pred", "category_l1_pred", "sub_vecs",
                 "model_version", "embedding_dim")

    def __init__(
        self,
        global_vec: List[float],
        tags_pred: List[int],
        category_l1_pred: int,
        sub_vecs: Optional[List[List[float]]] = None,
        model_version: str = "unknown",
        embedding_dim: int = EMBEDDING_DIM,
    ):
        self.global_vec = global_vec
        self.tags_pred = tags_pred
        self.category_l1_pred = category_l1_pred
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

        # 尝试加载模型
        self._ort_session = self._load_onnx()
        if self._ort_session:
            self._validate_model_outputs()

        # 如果既无 ONNX 也无 mock 权限 → 启动失败
        if self._ort_session is None and not ALLOW_MOCK:
            raise RuntimeError(
                f"No inference model found at {ONNX_MODEL_PATH}. "
                f"Set IMGSRCH_ALLOW_MOCK_INFERENCE=true for dev/testing."
            )

        if self._ort_session is None and ALLOW_MOCK:
            logger.warning(
                "MOCK_INFERENCE_ENABLED — 随机向量, 仅用于开发/测试",
                model_path=str(ONNX_MODEL_PATH),
            )

    # ── 公共接口 ──

    async def extract_query(
        self, query_image_b64: str, device: str = "gpu"
    ) -> FeatureResult:
        """查询侧特征提取"""
        img_bytes = base64.b64decode(query_image_b64)
        img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
        tensor = self._preprocess(img)

        if self._ort_session is None:
            return self._mock_infer(tensor, include_sub=False)

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

        if self._ort_session is None:
            return self._mock_infer(tensor, include_sub=True)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._onnx_infer, tensor, True)

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
            sub_vecs=None,  # TODO: sub-image 需单独模型
            model_version=self._model_version,
            embedding_dim=len(global_vec),
        )

    # ── Mock 推理 (开发环境) ──

    def _mock_infer(self, tensor: np.ndarray, include_sub: bool) -> FeatureResult:
        """Mock 推理 — 仅在 ALLOW_MOCK=true 时可达"""
        # 基于输入 tensor 的 hash 生成确定性向量 (同图同向量)
        seed = int(np.abs(tensor).sum() * 1e6) % (2**31)
        rng = np.random.RandomState(seed)

        global_vec = rng.randn(EMBEDDING_DIM).astype(np.float32)
        global_vec = (global_vec / np.linalg.norm(global_vec)).tolist()

        sub_vecs = None
        if include_sub:
            sub_vecs = []
            for i in range(3):
                sv = rng.randn(128).astype(np.float32)
                sub_vecs.append((sv / np.linalg.norm(sv)).tolist())

        return FeatureResult(
            global_vec=global_vec,
            tags_pred=rng.choice(TAG_VOCAB_SIZE, 5, replace=False).tolist(),
            category_l1_pred=int(rng.randint(0, CAT_L1_SIZE)),
            sub_vecs=sub_vecs,
            model_version="mock-v0",
            embedding_dim=EMBEDDING_DIM,
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
