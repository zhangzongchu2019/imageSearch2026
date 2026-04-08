#!/usr/bin/env python3
"""
批量导入图片到 Milvus + PostgreSQL (两级分类架构 v2.0)

使用 ViT-L-14 (768维) + FashionSigLIP 两级分类:
  Stage 1: ViT-L-14 → L1 大类(18类) + 标签 + 768→256 搜索向量
  Stage 2: FashionSigLIP (时尚类 L2/L3) / ViT-L-14 零样本 (其他类 L2)
直接批量写入 Milvus global_images_hot 和 PG uri_dedup。

Usage:
    python3 scripts/batch_import_clothing.py --count 10000
    python3 scripts/batch_import_clothing.py --count 100 --local-dir /path/to/images
    python3 scripts/batch_import_clothing.py --count 10000 --skip-kafka
    python3 scripts/batch_import_clothing.py --count 10000 \\
        --proxies socks5://127.0.0.1:61081,socks5://127.0.0.1:61082
    python3 scripts/batch_import_clothing.py --count 100 --backup-dir ~/imgsrch_backup
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import struct
import sys
import time
from io import BytesIO
from pathlib import Path
from typing import Optional

import numpy as np
import torch
import open_clip
from PIL import Image

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("batch_import")
# 抑制 httpx 每条请求的 INFO 日志 (严重拖慢高并发下载)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def emit_progress(stage: str, completed: int, total: int, message: str):
    """Emit a ##PROGRESS## JSON line for SSE consumers."""
    import json as _json
    line = _json.dumps({"stage": stage, "completed": completed, "total": total, "message": message})
    print(f"##PROGRESS##{line}", flush=True)

# ── Constants ────────────────────────────────────────────────────────────────
EMBEDDING_DIM = 256
CLIP_DIM = 768  # ViT-L-14
def _detect_milvus_host():
    """Auto-detect Milvus host: try localhost first, fallback to imgsrch-milvus (for Docker containers)."""
    host = os.getenv("MILVUS_HOST")
    if host:
        return host
    import socket
    for candidate in ["localhost", "imgsrch-milvus"]:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((candidate, 19530))
            s.close()
            return candidate
        except:
            pass
    return "localhost"

MILVUS_HOST = _detect_milvus_host()
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))
PG_DSN = os.getenv("PG_DSN", "postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
COLLECTION_NAME = "global_images_hot"
PARTITION_NAME = os.getenv("PARTITION_NAME", "p_202604")
TS_MONTH = int(os.getenv("TS_MONTH", "202604"))
IS_EVERGREEN = False
DOWNLOAD_CONCURRENCY = 500    # per-channel concurrency (COS 缩图有服务端延迟, 需高并发)
GPU_BATCH_SIZE = 128
MILVUS_BATCH_SIZE = 5000
MERCHANT_ID_START = 10001
MERCHANT_ID_END = 12999
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "/tmp/batch_import_checkpoint.json")
TASK_LOG_DIR = os.getenv("TASK_LOG_DIR", "/data/imgsrch/task_logs")


# ── Task Tracker — 记录每次运行的详细信息 ──────────────────────────────────

class TaskTracker:
    """记录批量导入任务的详细执行信息，输出到 data/task_logs/。

    每次运行生成一个 JSON 日志文件:
      data/task_logs/batch_import_YYYYMMDD_HHMMSS.json

    包含: 运行参数、各阶段耗时/统计、失败记录、最终汇总。
    """

    def __init__(self, args: argparse.Namespace):
        os.makedirs(TASK_LOG_DIR, exist_ok=True)
        self.run_id = time.strftime("%Y%m%d_%H%M%S")
        self.log_file = os.path.join(TASK_LOG_DIR, f"batch_import_{self.run_id}.json")
        self.data = {
            "run_id": self.run_id,
            "start_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": None,
            "args": {k: v for k, v in vars(args).items()},
            "stages": {},
            "download_failures": [],
            "summary": {},
        }
        self._stage_t0: dict[str, float] = {}

    def stage_start(self, name: str, detail: dict | None = None):
        """标记阶段开始"""
        self._stage_t0[name] = time.time()
        self.data["stages"][name] = {
            "status": "running",
            "start_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "detail": detail or {},
        }

    def stage_end(self, name: str, detail: dict | None = None):
        """标记阶段结束，记录耗时"""
        elapsed = time.time() - self._stage_t0.get(name, time.time())
        stage = self.data["stages"].get(name, {})
        stage["status"] = "done"
        stage["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
        stage["elapsed_sec"] = round(elapsed, 1)
        if detail:
            stage["detail"].update(detail)
        self.data["stages"][name] = stage

    def record_download_failure(self, url: str, image_pk: str, error: str):
        """记录单条下载失败"""
        self.data["download_failures"].append({
            "url": url,
            "image_pk": image_pk,
            "error": error,
            "time": time.strftime("%Y-%m-%d %H:%M:%S"),
        })

    def set_summary(self, summary: dict):
        """设置最终汇总"""
        self.data["summary"] = summary
        self.data["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S")

    def save(self):
        """写入 JSON 日志文件"""
        with open(self.log_file, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
        n_fail = len(self.data["download_failures"])
        log.info(f"Task log saved: {self.log_file} "
                 f"(download failures: {n_fail})")

    def save_failures_csv(self):
        """单独输出失败 URL 列表 (方便重试)"""
        failures = self.data["download_failures"]
        if not failures:
            return
        csv_path = os.path.join(TASK_LOG_DIR, f"download_failures_{self.run_id}.csv")
        with open(csv_path, "w") as f:
            f.write("url,image_pk,error\n")
            for r in failures:
                err = r["error"].replace('"', "'")
                f.write(f'"{r["url"]}","{r["image_pk"]}","{err}"\n')
        log.info(f"Failure URLs saved: {csv_path} ({len(failures)} records)")


# ── Projection Matrix (matches inference-service exactly) ─────────────────
def build_projection_matrix() -> np.ndarray:
    """Build orthogonal projection matrix (768 → 256), seed=42.
    Identical to inference-service/app/main.py (ViT-L-14)."""
    rng = np.random.RandomState(42)
    proj = rng.randn(CLIP_DIM, EMBEDDING_DIM).astype(np.float32)
    u, _, vt = np.linalg.svd(proj, full_matrices=False)
    return u  # (768, 256) orthogonal matrix


PROJECTION_MATRIX = None  # lazy init


# ── 1. Image URL Collection ─────────────────────────────────────────────────

FASHIONPEDIA_MANIFEST = (
    "https://raw.githubusercontent.com/cvdfoundation/fashionpedia/main/train_images.txt"
)

OPEN_IMAGES_CLOTHING_LABELS = [
    "Shirt", "Dress", "Coat", "Jacket", "Jeans", "Skirt", "Suit",
    "T-shirt", "Sweater", "Shorts", "Blouse", "Pants", "Hoodie",
]


def _gen_image_pk(uri: str) -> str:
    """image_pk = hex(sha256(uri)[0:16]) → CHAR(32)"""
    digest = hashlib.sha256(uri.encode()).digest()
    return digest[:16].hex()


def _uri_hash(uri: str) -> str:
    """Full SHA256 hex for uri_dedup."""
    return hashlib.sha256(uri.encode()).hexdigest()


def save_checkpoint(data: dict):
    """Save import progress checkpoint to disk."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(data, f)
    log.info(f"Checkpoint saved: stage={data.get('stage')}, done={data.get('done', 0)}")


def load_checkpoint() -> Optional[dict]:
    """Load import progress checkpoint."""
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            data = json.load(f)
        log.info(f"Checkpoint loaded: stage={data.get('stage')}, done={data.get('done', 0)}")
        return data
    except Exception as e:
        log.warning(f"Failed to load checkpoint: {e}")
        return None


def clear_checkpoint():
    """Remove checkpoint file after successful completion."""
    if os.path.exists(CHECKPOINT_FILE):
        os.unlink(CHECKPOINT_FILE)
        log.info("Checkpoint cleared")


def collect_image_urls_from_file(url_file: str, count: int) -> list[dict]:
    """Collect image URLs from a text file (one URL per line)."""
    log.info(f"Reading URLs from file: {url_file}")
    urls = []
    with open(url_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if not line.startswith(("http://", "https://", "ftp://")):
                log.info(f"Skipping non-URL line: {line[:80]}")
                continue
            urls.append(line)
            if len(urls) >= count:
                break
    log.info(f"Read {len(urls)} URLs from file (requested {count})")
    results = []
    for i, url in enumerate(urls[:count]):
        results.append({
            "uri": url,
            "image_pk": _gen_image_pk(url),
            "uri_hash": _uri_hash(url),
            "local_path": None,
            "index": i,
        })
    return results


def collect_image_urls_from_local(local_dir: str, count: int) -> list[dict]:
    """Collect image paths from a local directory."""
    exts = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}
    paths = []
    for p in sorted(Path(local_dir).rglob("*")):
        if p.suffix.lower() in exts:
            paths.append(str(p))
        if len(paths) >= count:
            break
    if len(paths) < count:
        log.warning(f"Only found {len(paths)} images in {local_dir} (requested {count})")

    results = []
    for i, p in enumerate(paths[:count]):
        uri = f"file://{p}"
        results.append({
            "uri": uri,
            "image_pk": _gen_image_pk(uri),
            "uri_hash": _uri_hash(uri),
            "local_path": p,
            "index": i,
        })
    return results


def collect_image_urls_generated(count: int) -> list[dict]:
    """
    Generate image metadata using picsum.photos (public, no auth needed).
    Each URL is unique via query parameters.
    """
    categories = [
        "shirt", "dress", "jacket", "jeans", "skirt", "coat",
        "sweater", "hoodie", "pants", "blouse", "tshirt", "suit",
        "shorts", "blazer", "cardigan",
    ]
    results = []
    for i in range(count):
        cat = categories[i % len(categories)]
        url = f"https://picsum.photos/seed/clothing_{cat}_{i:05d}/224/224"
        results.append({
            "uri": url,
            "image_pk": _gen_image_pk(url),
            "uri_hash": _uri_hash(url),
            "local_path": None,
            "index": i,
        })
    return results


async def try_fetch_fashionpedia_urls(count: int) -> Optional[list[dict]]:
    """Try to fetch real Fashionpedia image URLs from the manifest."""
    import httpx
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(FASHIONPEDIA_MANIFEST)
            if resp.status_code != 200:
                return None
            lines = [l.strip() for l in resp.text.splitlines() if l.strip()]
            if len(lines) < count:
                log.info(f"Fashionpedia manifest has {len(lines)} URLs, need {count}")
            urls = lines[:count]
            results = []
            for i, url in enumerate(urls):
                results.append({
                    "uri": url,
                    "image_pk": _gen_image_pk(url),
                    "uri_hash": _uri_hash(url),
                    "local_path": None,
                    "index": i,
                })
            return results if len(results) >= count else None
    except Exception as e:
        log.warning(f"Failed to fetch Fashionpedia manifest: {e}")
        return None


async def collect_image_urls(count: int, local_dir: Optional[str] = None,
                             url_file: Optional[str] = None) -> list[dict]:
    """Collect image URLs from best available source."""
    if url_file:
        return collect_image_urls_from_file(url_file, count)
    if local_dir:
        log.info(f"Collecting {count} images from local directory: {local_dir}")
        return collect_image_urls_from_local(local_dir, count)

    log.info("Trying Fashionpedia manifest...")
    urls = await try_fetch_fashionpedia_urls(count)
    if urls and len(urls) >= count:
        log.info(f"Got {len(urls)} URLs from Fashionpedia")
        return urls

    log.info(f"Falling back to generated URLs (picsum.photos) for {count} images")
    return collect_image_urls_generated(count)


# ── 2. Image Download (multi-channel proxy support) ──────────────────────────

def _parse_proxies(proxies_str: Optional[str]) -> list[Optional[str]]:
    """Parse comma-separated proxy URLs. Always includes None (direct) as last channel."""
    channels = []
    if proxies_str:
        for p in proxies_str.split(","):
            p = p.strip()
            if p:
                channels.append(p)
    channels.append(None)  # direct connection channel
    return channels


async def download_images(
    image_metas: list[dict],
    download_dir: str = "/tmp/clothing_images",
    proxies_str: Optional[str] = None,
    tracker: Optional["TaskTracker"] = None,
) -> list[dict]:
    """Download images concurrently via multiple proxy channels + direct."""
    import httpx

    os.makedirs(download_dir, exist_ok=True)

    to_download = [m for m in image_metas if m["local_path"] is None]
    already_local = [m for m in image_metas if m["local_path"] is not None]

    if not to_download:
        log.info("All images are local, skipping download")
        return image_metas

    channels = _parse_proxies(proxies_str)
    log.info(f"Download channels: {len(channels)} "
             f"({len(channels)-1} proxies + 1 direct), "
             f"concurrency={DOWNLOAD_CONCURRENCY}/channel")

    success = 0
    failed = 0

    dl_headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        "Referer": "https://www.szwego.com/",
    }

    # Build one httpx client per channel
    clients = []
    sems = []
    for proxy in channels:
        transport = None
        if proxy:
            transport = httpx.AsyncHTTPTransport(proxy=proxy)
        if transport is None:
            transport = httpx.AsyncHTTPTransport(
                limits=httpx.Limits(max_connections=300, max_keepalive_connections=200),
            )
        client = httpx.AsyncClient(
            timeout=60,
            headers=dl_headers,
            transport=transport,
        )
        clients.append(client)
        sems.append(asyncio.Semaphore(DOWNLOAD_CONCURRENCY))

    async def _download_one(meta: dict, channel_idx: int):
        nonlocal success, failed
        client = clients[channel_idx]
        sem = sems[channel_idx]
        async with sem:
            fname = f"{meta['image_pk'][:16]}.jpg"
            fpath = os.path.join(download_dir, fname)
            if os.path.exists(fpath) and os.path.getsize(fpath) > 100:
                meta["local_path"] = fpath
                success += 1
                return
            try:
                # COS 服务端缩图: 224×224 (CLIP 输入尺寸), 减少 ~70x 传输量
                dl_url = meta["uri"]
                if "myqcloud.com" in dl_url:
                    sep = "&" if "?" in dl_url else "?"
                    dl_url += f"{sep}imageMogr2/thumbnail/224x224"
                resp = await client.get(dl_url, follow_redirects=True)
                ct = resp.headers.get("content-type", "")
                is_image = ct.startswith("image/") or len(resp.content) > 200
                if resp.status_code == 200 and is_image:
                    with open(fpath, "wb") as f:
                        f.write(resp.content)
                    meta["local_path"] = fpath
                    success += 1
                else:
                    failed += 1
                    err = f"status={resp.status_code}, content-type={ct}, size={len(resp.content)}"
                    if tracker:
                        tracker.record_download_failure(meta["uri"], meta["image_pk"], err)
            except Exception as e:
                failed += 1
                if tracker:
                    tracker.record_download_failure(meta["uri"], meta["image_pk"], str(e)[:200])
            # COS 内网无需限速

    # Round-robin assign tasks to channels
    tasks = []
    for i, meta in enumerate(to_download):
        channel_idx = i % len(channels)
        tasks.append(_download_one(meta, channel_idx))

    total_tasks = len(tasks)
    log.info(f"Downloading {total_tasks} images across {len(channels)} channels ...")

    chunk_size = 2000
    for i in range(0, len(tasks), chunk_size):
        chunk = tasks[i : i + chunk_size]
        await asyncio.gather(*chunk, return_exceptions=True)
        done_count = min(i + chunk_size, total_tasks)
        log.info(f"  Download progress: {done_count}/{total_tasks} "
                 f"(ok={success}, fail={failed})")
        emit_progress("download", done_count, total_tasks,
                      f"Downloaded {done_count}/{total_tasks} (ok={success}, fail={failed})")

    # Close all clients
    for client in clients:
        await client.aclose()

    log.info(f"Download complete: {success} ok, {failed} failed")

    result = already_local + [m for m in to_download if m["local_path"] is not None]
    return result


# ── 3. Feature Extraction (CLIP + Orthogonal Projection) ─────────────────────

class CLIPFeatureExtractor:
    """ViT-L-14 feature extractor with orthogonal projection 768→256.

    两级分类架构:
      Stage 1: ViT-L-14 → 768维 → L1 大类 + 标签 + 256维搜索向量
      Stage 2: FashionSigLIP (时尚类 L2/L3) / ViT-L-14零样本 (其他类 L2)

    与 inference-service v2.0 taxonomy.py 保持一致。
    """

    def __init__(self, device: str = "cuda", model_path: Optional[str] = None):
        global PROJECTION_MATRIX
        # 导入 taxonomy (inference-service 中定义)
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "services" / "inference-service"))
        from app.taxonomy import (
            L1_CATEGORIES, L1_CODES, L1_CODE_TO_NAME,
            FASHION_L1_CODES, FASHION_L2, FASHION_L3,
            NONFASHION_L2, TAGS, TAG_NAMES, is_fashion,
        )
        self._taxonomy = {
            "L1_CATEGORIES": L1_CATEGORIES, "L1_CODES": L1_CODES,
            "L1_CODE_TO_NAME": L1_CODE_TO_NAME,
            "FASHION_L1_CODES": FASHION_L1_CODES,
            "FASHION_L2": FASHION_L2, "FASHION_L3": FASHION_L3,
            "NONFASHION_L2": NONFASHION_L2,
            "TAGS": TAGS, "TAG_NAMES": TAG_NAMES,
            "is_fashion": is_fashion,
        }

        self.device = torch.device(device if torch.cuda.is_available() else "cpu")
        log.info(f"Loading ViT-L-14 on {self.device}...")

        _orig_torch_load = torch.load
        torch.load = lambda *a, **kw: _orig_torch_load(*a, **{**kw, "weights_only": False})

        model_name = "ViT-L-14"
        if model_path and os.path.exists(model_path):
            log.info(f"Loading from local: {model_path}")
            self.model, _, self.preprocess = open_clip.create_model_and_transforms(
                model_name, pretrained=model_path, device=self.device,
            )
        else:
            log.info("Downloading ViT-L-14 (openai pretrained)...")
            self.model, _, self.preprocess = open_clip.create_model_and_transforms(
                model_name, pretrained="openai", device=self.device,
            )
        torch.load = _orig_torch_load
        self.model.eval()
        self.tokenizer = open_clip.get_tokenizer(model_name)

        if PROJECTION_MATRIX is None:
            PROJECTION_MATRIX = build_projection_matrix()
        self.projection = PROJECTION_MATRIX

        # 预计算文本特征
        self._init_text_features()

        # FashionSigLIP (优化: 复用PIL对象 + 快速preprocess + 批量encode)
        self.fashion_model = None
        self._init_fashion_model()

        log.info(f"Models loaded: ViT-L-14 (768→256), "
                 f"{len(self.l1_names)} L1, {len(self.tag_names)} tags, "
                 f"FashionSigLIP={'loaded' if self.fashion_model else 'not loaded'}")

    def _encode_text(self, prompts_dict, multi_prompt=True):
        """编码文本特征 (复用 inference-service 逻辑)"""
        names, all_texts, counts = [], [], []
        for name_key, prompts in prompts_dict.items():
            names.append(name_key)
            if isinstance(prompts, str):
                prompts = [prompts]
            all_texts.extend(prompts)
            counts.append(len(prompts))

        tokens = self.tokenizer(all_texts).to(self.device)
        with torch.no_grad():
            feats = self.model.encode_text(tokens)
        feats = feats / feats.norm(dim=-1, keepdim=True)

        if multi_prompt and any(c > 1 for c in counts):
            averaged = []
            idx = 0
            for c in counts:
                avg = feats[idx:idx + c].mean(dim=0)
                avg = avg / avg.norm()
                averaged.append(avg)
                idx += c
            return names, torch.stack(averaged)
        return names, feats

    def _init_text_features(self):
        """预计算 L1 品类 + 标签 + 非时尚 L2 文本特征"""
        T = self._taxonomy

        # L1 品类
        self.l1_codes = T["L1_CODES"]
        self.l1_names = [T["L1_CODE_TO_NAME"][c] for c in self.l1_codes]
        l1_prompts = {T["L1_CATEGORIES"][c]["name"]: T["L1_CATEGORIES"][c]["prompts"]
                      for c in self.l1_codes}
        _, self.l1_text_features = self._encode_text(l1_prompts)

        # 标签
        self.tag_names = T["TAG_NAMES"]
        tag_prompts = {n: T["TAGS"][n] for n in self.tag_names}
        _, self.tag_text_features = self._encode_text(tag_prompts)

        # 非时尚类 L2
        self.nf_l2_features = {}
        self.nf_l2_names = {}
        self.nf_l2_codes = {}
        for l1_code, l2_dict in T["NONFASHION_L2"].items():
            prompts = {info["name"]: info["prompts"] for info in l2_dict.values()}
            codes = list(l2_dict.keys())
            names, feats = self._encode_text(prompts)
            self.nf_l2_features[l1_code] = feats
            self.nf_l2_names[l1_code] = names
            self.nf_l2_codes[l1_code] = codes

    def _init_fashion_model(self):
        """尝试加载 FashionSigLIP"""
        T = self._taxonomy
        try:
            from open_clip.tokenizer import HFTokenizer
            fashion_path = os.environ.get("FASHION_MODEL_PATH", "/data/imgsrch/models/marqo-fashionsiglip")
            tokenizer_path = os.environ.get("FASHION_TOKENIZER_PATH", "/data/imgsrch/models/vit-b-16-siglip-tokenize")
            weights_file = os.path.join(fashion_path, "open_clip_pytorch_model.bin")

            _orig = torch.load
            torch.load = lambda *a, **kw: _orig(*a, **{**kw, "weights_only": False})
            self.fashion_model, _, self.fashion_preprocess = open_clip.create_model_and_transforms(
                "ViT-B-16-SigLIP", pretrained=weights_file,
            )
            torch.load = _orig
            self.fashion_tokenizer = HFTokenizer(tokenizer_path, context_length=64, clean="canonicalize")
            self.fashion_model.eval().to(self.device)
            log.info("FashionSigLIP loaded (offline)")

            # 时尚 L2/L3 文本特征
            self.fashion_l2_features = {}
            self.fashion_l2_names = {}
            self.fashion_l2_codes = {}
            for l1_code, l2_dict in T["FASHION_L2"].items():
                prompts = {info["name"]: info["prompts"] for info in l2_dict.values()}
                codes = list(l2_dict.keys())
                tokens_list = []
                for p_list in prompts.values():
                    tokens_list.extend(p_list if isinstance(p_list, list) else [p_list])
                names = list(prompts.keys())
                toks = self.fashion_tokenizer(tokens_list).to(self.device)
                with torch.no_grad():
                    feats = self.fashion_model.encode_text(toks)
                feats = feats / feats.norm(dim=-1, keepdim=True)
                self.fashion_l2_features[l1_code] = feats
                self.fashion_l2_names[l1_code] = names
                self.fashion_l2_codes[l1_code] = codes

            self.fashion_l3_features = {}
            self.fashion_l3_names = {}
            self.fashion_l3_codes = {}
            for l2_code, l3_dict in T["FASHION_L3"].items():
                prompts = {info["name"]: info["prompts"] for info in l3_dict.values()}
                codes = list(l3_dict.keys())
                tokens_list = []
                for p_list in prompts.values():
                    tokens_list.extend(p_list if isinstance(p_list, list) else [p_list])
                names = list(prompts.keys())
                toks = self.fashion_tokenizer(tokens_list).to(self.device)
                with torch.no_grad():
                    feats = self.fashion_model.encode_text(toks)
                feats = feats / feats.norm(dim=-1, keepdim=True)
                self.fashion_l3_features[l2_code] = feats
                self.fashion_l3_names[l2_code] = names
                self.fashion_l3_codes[l2_code] = codes

        except Exception as e:
            log.warning(f"FashionSigLIP not loaded: {e}")
            self.fashion_model = None

    def _load_image(self, path: str) -> Optional[tuple[torch.Tensor, Image.Image]]:
        """返回 (preprocessed_tensor, pil_image) — pil_image 供 FashionSigLIP 复用"""
        try:
            img = Image.open(path).convert("RGB")
            if img.size == (224, 224):
                return self._fast_preprocess(img), img
            return self.preprocess(img), img
        except Exception:
            return None

    _fast_transform = None
    _fashion_fast_transform = None

    def _fast_preprocess(self, img):
        """跳过 Resize/CenterCrop, 直接 ToTensor + Normalize (已经是 224x224)"""
        if self._fast_transform is None:
            from torchvision import transforms
            self.__class__._fast_transform = transforms.Compose([
                transforms.ToTensor(),
                transforms.Normalize(
                    mean=(0.48145466, 0.4578275, 0.40821073),
                    std=(0.26862954, 0.26130258, 0.27577711),
                ),
            ])
        return self._fast_transform(img)

    def _fast_fashion_preprocess(self, img):
        """FashionSigLIP 快速路径 (224x224 已匹配, 跳过 Resize)"""
        if self._fashion_fast_transform is None:
            from torchvision import transforms
            # FashionSigLIP (ViT-B-16-SigLIP) 同样用 224x224 输入, 同样的 normalize
            self.__class__._fashion_fast_transform = transforms.Compose([
                transforms.ToTensor(),
                transforms.Normalize(
                    mean=(0.48145466, 0.4578275, 0.40821073),
                    std=(0.26862954, 0.26130258, 0.27577711),
                ),
            ])
        return self._fashion_fast_transform(img)

    def _project(self, features_768: np.ndarray) -> np.ndarray:
        """Project 768-dim features to 256-dim using orthogonal matrix + L2 normalize."""
        reduced = features_768 @ self.projection  # (N, 256)
        norms = np.linalg.norm(reduced, axis=1, keepdims=True)
        norms = np.maximum(norms, 1e-8)
        return (reduced / norms).astype(np.float32)

    def _classify_batch_l1(self, features_t: torch.Tensor,
                           top_k: int = 8, threshold: float = 0.2) -> list[dict]:
        """用 768 维原始特征做 L1 品类/标签分类"""
        feat_norm = features_t.float()
        feat_norm = feat_norm / feat_norm.norm(dim=-1, keepdim=True)

        l1_text = self.l1_text_features.float().to(self.device)
        tag_text = self.tag_text_features.float().to(self.device)

        l1_sims = feat_norm @ l1_text.T
        tag_sims = feat_norm @ tag_text.T

        top_k = min(top_k, len(self.tag_names))
        results = []
        for i in range(len(features_t)):
            l1_idx = l1_sims[i].argmax().item()
            l1_conf = l1_sims[i][l1_idx].item()
            l1_name = self.l1_names[l1_idx]
            l1_code = self.l1_codes[l1_idx]

            topk_result = tag_sims[i].topk(top_k)
            tags = [
                self.tag_names[j]
                for j, s in zip(topk_result.indices.tolist(), topk_result.values.tolist())
                if s > threshold
            ]
            tag_ids = [hash(t) % 4096 for t in tags]

            # L2 (非时尚类直接用 ViT-L-14)
            l2_name, l2_code, l2_conf = "", 0, 0.0
            if not self._taxonomy["is_fashion"](l1_code) and l1_code in self.nf_l2_features:
                l2_feats = self.nf_l2_features[l1_code].float().to(self.device)
                l2_sims = (feat_norm[i:i+1] @ l2_feats.T).squeeze(0)
                l2_idx = l2_sims.argmax().item()
                l2_conf = l2_sims[l2_idx].item()
                l2_name = self.nf_l2_names[l1_code][l2_idx]
                l2_code = self.nf_l2_codes[l1_code][l2_idx]

            results.append({
                "category_l1": l1_name,
                "category_l1_id": l1_code,
                "category_l1_conf": round(l1_conf, 4),
                "category_l2": l2_name,
                "category_l2_id": l2_code,
                "category_l3": "",
                "category_l3_id": 0,
                "tags_name": tags,
                "tags": tag_ids,
                "_is_fashion": self._taxonomy["is_fashion"](l1_code),
            })
        return results

    def _classify_fashion_l2l3_fast(self, pil_images: list, classifications: list[dict]):
        """FashionSigLIP L2/L3 批量分类 (复用已加载的 PIL 对象, 无重复 IO)"""
        if self.fashion_model is None:
            return

        fashion_indices = []
        for i, cls in enumerate(classifications):
            if cls["_is_fashion"] and cls["category_l1_id"] in self.fashion_l2_features:
                fashion_indices.append(i)

        if not fashion_indices:
            return

        # 批量预处理 (复用 PIL 对象, 快速路径跳过 Resize)
        tensors = []
        valid_indices = []
        for idx in fashion_indices:
            try:
                img = pil_images[idx]
                if img.size == (224, 224):
                    tensors.append(self._fast_fashion_preprocess(img))
                else:
                    tensors.append(self.fashion_preprocess(img))
                valid_indices.append(idx)
            except Exception:
                pass

        if not tensors:
            return

        batch = torch.stack(tensors).to(self.device)
        with torch.no_grad():
            feats = self.fashion_model.encode_image(batch)
        feats = feats / feats.norm(dim=-1, keepdim=True)
        feats = feats.float()

        for feat_idx, orig_idx in enumerate(valid_indices):
            cls = classifications[orig_idx]
            l1_code = cls["category_l1_id"]
            feat = feats[feat_idx:feat_idx + 1]
            try:
                l2_feats = self.fashion_l2_features[l1_code].float()
                l2_sims = (feat @ l2_feats.T).squeeze(0)
                l2_idx = l2_sims.argmax().item()
                cls["category_l2"] = self.fashion_l2_names[l1_code][l2_idx]
                cls["category_l2_id"] = self.fashion_l2_codes[l1_code][l2_idx]
                l2_code = cls["category_l2_id"]
                if l2_code in self.fashion_l3_features:
                    l3_feats = self.fashion_l3_features[l2_code].float()
                    l3_sims = (feat @ l3_feats.T).squeeze(0)
                    l3_idx = l3_sims.argmax().item()
                    cls["category_l3"] = self.fashion_l3_names[l2_code][l3_idx]
                    cls["category_l3_id"] = self.fashion_l3_codes[l2_code][l3_idx]
            except Exception:
                pass

    def _classify_fashion_l2l3(self, paths: list[str], classifications: list[dict]):
        """对时尚类图片做 FashionSigLIP L2/L3 批量分类。

        优化: 收集整个 batch 中的时尚类图片, 一次 encode_image,
        再按 L1 code 分组做 L2/L3 分类 (矩阵运算)。
        """
        if self.fashion_model is None:
            return

        # 1. 收集时尚类图片 index
        fashion_indices = []
        for i, cls in enumerate(classifications):
            if cls["_is_fashion"] and cls["category_l1_id"] in self.fashion_l2_features:
                fashion_indices.append(i)

        if not fashion_indices:
            return

        # 2. 批量加载 + 预处理
        tensors = []
        valid_indices = []  # 成功加载的 index
        for idx in fashion_indices:
            try:
                img = Image.open(paths[idx]).convert("RGB")
                tensors.append(self.fashion_preprocess(img))
                valid_indices.append(idx)
            except Exception as e:
                log.warning(f"FashionSigLIP load failed for {paths[idx]}: {e}")

        if not tensors:
            return

        # 3. 一次批量 encode
        batch = torch.stack(tensors).to(self.device)
        with torch.no_grad():
            feats = self.fashion_model.encode_image(batch)  # (N, D)
        feats = feats / feats.norm(dim=-1, keepdim=True)
        feats = feats.float()

        # 4. 按 L1 code 分组做 L2/L3 分类 (矩阵运算)
        for feat_idx, orig_idx in enumerate(valid_indices):
            cls = classifications[orig_idx]
            l1_code = cls["category_l1_id"]
            feat = feats[feat_idx:feat_idx + 1]  # (1, D)

            try:
                # L2
                l2_feats = self.fashion_l2_features[l1_code].float()
                l2_sims = (feat @ l2_feats.T).squeeze(0)
                l2_idx = l2_sims.argmax().item()
                cls["category_l2"] = self.fashion_l2_names[l1_code][l2_idx]
                cls["category_l2_id"] = self.fashion_l2_codes[l1_code][l2_idx]

                # L3
                l2_code = cls["category_l2_id"]
                if l2_code in self.fashion_l3_features:
                    l3_feats = self.fashion_l3_features[l2_code].float()
                    l3_sims = (feat @ l3_feats.T).squeeze(0)
                    l3_idx = l3_sims.argmax().item()
                    cls["category_l3"] = self.fashion_l3_names[l2_code][l3_idx]
                    cls["category_l3_id"] = self.fashion_l3_codes[l2_code][l3_idx]
            except Exception as e:
                log.warning(f"FashionSigLIP L2/L3 failed for {paths[orig_idx]}: {e}")

    @torch.no_grad()
    def extract_batch(self, paths: list[str]) -> tuple[list[str], np.ndarray, list[dict]]:
        """Extract 256-dim features + two-stage classification for a batch."""
        tensors = []
        pil_images = []  # 保留 PIL 对象供 FashionSigLIP 复用
        valid_paths = []
        for p in paths:
            result = self._load_image(p)
            if result is not None:
                tensor, pil_img = result
                tensors.append(tensor)
                pil_images.append(pil_img)
                valid_paths.append(p)

        if not tensors:
            return [], np.empty((0, EMBEDDING_DIM), dtype=np.float32), []

        batch = torch.stack(tensors).to(self.device)
        features = self.model.encode_image(batch)  # (B, 768)

        # Stage 1: L1 分类 + 标签 + 非时尚 L2
        classifications = self._classify_batch_l1(features)

        # Stage 2: 时尚类 L2/L3 (FashionSigLIP 批量推理, 复用 PIL 对象)
        self._classify_fashion_l2l3_fast(pil_images, classifications)

        features_768 = features.cpu().numpy().astype(np.float32)
        features_256 = self._project(features_768)

        return valid_paths, features_256, classifications

    def extract_all(self, image_metas: list[dict]) -> list[dict]:
        """Extract features + two-stage classification for all images (single GPU)."""
        paths = [m["local_path"] for m in image_metas]
        path_to_meta = {m["local_path"]: m for m in image_metas}

        all_256 = []
        all_valid_paths = []
        all_classifications = []

        total_batches = (len(paths) + GPU_BATCH_SIZE - 1) // GPU_BATCH_SIZE
        log.info(f"Extracting features + classifying: {len(paths)} images, "
                 f"{total_batches} batches (batch_size={GPU_BATCH_SIZE})")

        t0 = time.time()
        for i in range(0, len(paths), GPU_BATCH_SIZE):
            batch_paths = paths[i : i + GPU_BATCH_SIZE]
            valid_paths, features_256, classifications = self.extract_batch(batch_paths)
            all_valid_paths.extend(valid_paths)
            all_classifications.extend(classifications)
            if len(features_256) > 0:
                all_256.append(features_256)

            batch_num = i // GPU_BATCH_SIZE + 1
            if batch_num % 10 == 0 or batch_num == total_batches:
                elapsed = time.time() - t0
                rate = len(all_valid_paths) / elapsed if elapsed > 0 else 0
                log.info(f"  Batch {batch_num}/{total_batches}, "
                         f"{len(all_valid_paths)} done, {rate:.0f} img/sec")
                emit_progress("extract", len(all_valid_paths), len(paths),
                              f"Batch {batch_num}/{total_batches}, {rate:.0f} img/sec")

        if not all_256:
            log.error("No features extracted!")
            return []

        all_256 = np.concatenate(all_256, axis=0)
        log.info(f"Projection done: {all_256.shape} in {time.time()-t0:.1f}s")

        # 统计品类分布
        cat_counts: dict[str, int] = {}
        for cls in all_classifications:
            cat_counts[cls["category_l1"]] = cat_counts.get(cls["category_l1"], 0) + 1
        log.info(f"Category distribution: {cat_counts}")

        # Map back to metas
        result = []
        for path, vec, cls in zip(all_valid_paths, all_256, all_classifications):
            meta = path_to_meta[path]
            meta["global_vec"] = vec.tolist()
            meta["category_l1"] = cls["category_l1_id"]
            meta["category_l1_name"] = cls["category_l1"]
            meta["category_l1_conf"] = cls["category_l1_conf"]
            meta["category_l2"] = cls.get("category_l2_id", 0)
            meta["category_l3"] = cls.get("category_l3_id", 0)
            meta["tags"] = cls["tags"]
            meta["tags_name"] = cls["tags_name"]
            result.append(meta)

        log.info(f"Feature extraction + classification complete: {len(result)} images")
        return result


# ── Multi-GPU 并行特征提取 ──────────────────────────────────────────────────

def _gpu_worker(args_tuple):
    """单个 GPU worker: 加载模型到指定 GPU，处理分配到的图片子集。
    在独立进程中运行，避免 GIL 和 CUDA context 冲突。"""
    gpu_id, paths_chunk, model_path, progress_file = args_tuple
    import json as _json

    device = f"cuda:{gpu_id}"
    log.info(f"[GPU{gpu_id}] Loading models on {device}, {len(paths_chunk)} images...")
    extractor = CLIPFeatureExtractor(device=device, model_path=model_path)

    all_256 = []
    all_valid_paths = []
    all_classifications = []
    total_batches = (len(paths_chunk) + GPU_BATCH_SIZE - 1) // GPU_BATCH_SIZE
    t0 = time.time()

    for i in range(0, len(paths_chunk), GPU_BATCH_SIZE):
        batch_paths = paths_chunk[i : i + GPU_BATCH_SIZE]
        valid_paths, features_256, classifications = extractor.extract_batch(batch_paths)
        all_valid_paths.extend(valid_paths)
        all_classifications.extend(classifications)
        if len(features_256) > 0:
            all_256.append(features_256)

        batch_num = i // GPU_BATCH_SIZE + 1
        if batch_num % 10 == 0 or batch_num == total_batches:
            elapsed = time.time() - t0
            rate = len(all_valid_paths) / elapsed if elapsed > 0 else 0
            log.info(f"  [GPU{gpu_id}] Batch {batch_num}/{total_batches}, "
                     f"{len(all_valid_paths)} done, {rate:.0f} img/sec")
            # 写进度到临时文件供主进程汇总
            try:
                with open(progress_file, "w") as f:
                    _json.dump({"gpu": gpu_id, "done": len(all_valid_paths),
                                "total": len(paths_chunk), "rate": round(rate, 1)}, f)
            except Exception:
                pass

    if not all_256:
        return [], [], []

    all_256_np = np.concatenate(all_256, axis=0)
    elapsed = time.time() - t0
    log.info(f"[GPU{gpu_id}] Done: {len(all_valid_paths)} images in {elapsed:.1f}s "
             f"({len(all_valid_paths)/elapsed:.0f} img/s)")

    # 返回 (paths, vectors_as_list, classifications)
    return all_valid_paths, all_256_np.tolist(), all_classifications


def extract_all_multi_gpu(image_metas: list[dict], model_path: str = None,
                          num_gpus: int = None) -> list[dict]:
    """4 卡并行特征提取 + 两级分类。用 multiprocessing.Pool spawn 模式。"""
    import torch.multiprocessing as mp

    if num_gpus is None:
        num_gpus = torch.cuda.device_count()
    num_gpus = min(num_gpus, torch.cuda.device_count())
    if num_gpus <= 1:
        log.info("Only 1 GPU available, falling back to single-GPU extract_all")
        ext = CLIPFeatureExtractor(device="cuda", model_path=model_path)
        return ext.extract_all(image_metas)

    paths = [m["local_path"] for m in image_metas]
    path_to_meta = {m["local_path"]: m for m in image_metas}
    total = len(paths)

    # 均分到各 GPU
    chunk_size = (total + num_gpus - 1) // num_gpus
    chunks = [paths[i:i + chunk_size] for i in range(0, total, chunk_size)]
    log.info(f"Multi-GPU extraction: {total} images across {len(chunks)} GPUs "
             f"(~{chunk_size} each)")

    # 进度文件
    progress_dir = "/tmp/gpu_progress"
    os.makedirs(progress_dir, exist_ok=True)
    progress_files = [f"{progress_dir}/gpu_{i}.json" for i in range(len(chunks))]

    # 启动进度汇总线程
    import threading
    _stop_progress = threading.Event()

    def _progress_reporter():
        while not _stop_progress.is_set():
            time.sleep(5)
            total_done = 0
            rates = []
            for pf in progress_files:
                try:
                    with open(pf) as f:
                        d = json.load(f)
                    total_done += d.get("done", 0)
                    rates.append(d.get("rate", 0))
                except Exception:
                    pass
            agg_rate = sum(rates)
            log.info(f"  [ALL GPUs] {total_done}/{total} done, {agg_rate:.0f} img/sec total")
            emit_progress("extract", total_done, total,
                          f"{len(chunks)} GPUs, {agg_rate:.0f} img/sec total")

    reporter = threading.Thread(target=_progress_reporter, daemon=True)
    reporter.start()

    # 构造参数
    worker_args = [
        (i, chunks[i], model_path, progress_files[i])
        for i in range(len(chunks))
    ]

    t0 = time.time()
    try:
        ctx = mp.get_context("spawn")
        with ctx.Pool(processes=len(chunks)) as pool:
            results = pool.map(_gpu_worker, worker_args)
    finally:
        _stop_progress.set()
        reporter.join(timeout=2)

    # 合并结果
    all_valid_paths = []
    all_256 = []
    all_classifications = []
    for vpaths, vecs_list, clss in results:
        if vpaths:
            all_valid_paths.extend(vpaths)
            all_256.append(np.array(vecs_list, dtype=np.float32))
            all_classifications.extend(clss)

    if not all_256:
        log.error("Multi-GPU extraction produced no results!")
        return []

    all_256_np = np.concatenate(all_256, axis=0)
    elapsed = time.time() - t0
    log.info(f"Multi-GPU done: {len(all_valid_paths)} images, {all_256_np.shape}, "
             f"{elapsed:.1f}s ({len(all_valid_paths)/elapsed:.0f} img/s total)")

    # 统计品类分布
    cat_counts: dict[str, int] = {}
    for cls in all_classifications:
        cat_counts[cls["category_l1"]] = cat_counts.get(cls["category_l1"], 0) + 1
    log.info(f"Category distribution: {cat_counts}")

    # Map back
    result = []
    for path, vec, cls in zip(all_valid_paths, all_256_np, all_classifications):
        meta = path_to_meta[path]
        meta["global_vec"] = vec.tolist()
        meta["category_l1"] = cls["category_l1_id"]
        meta["category_l1_name"] = cls["category_l1"]
        meta["category_l1_conf"] = cls["category_l1_conf"]
        meta["category_l2"] = cls.get("category_l2_id", 0)
        meta["category_l3"] = cls.get("category_l3_id", 0)
        meta["tags"] = cls["tags"]
        meta["tags_name"] = cls["tags_name"]
        result.append(meta)

    log.info(f"Multi-GPU extraction + classification complete: {len(result)} images")
    return result


# ── 4. Milvus Insert ─────────────────────────────────────────────────────────

def insert_milvus(image_metas: list[dict]):
    """Batch upsert into Milvus global_images_hot."""
    from pymilvus import connections, Collection, utility

    # Try configured host, fallback to container name if in Docker
    milvus_host = MILVUS_HOST
    for candidate in [MILVUS_HOST, "imgsrch-milvus", "localhost"]:
        try:
            log.info(f"Connecting to Milvus at {candidate}:{MILVUS_PORT}...")
            connections.connect(alias="default", host=candidate, port=MILVUS_PORT, timeout=10)
            milvus_host = candidate
            break
        except Exception as e:
            log.warning(f"Failed to connect to {candidate}:{MILVUS_PORT}: {e}")
            try:
                connections.disconnect("default")
            except:
                pass
    else:
        log.error("Cannot connect to Milvus on any host!")
        sys.exit(1)

    if not utility.has_collection(COLLECTION_NAME):
        log.error(f"Collection {COLLECTION_NAME} does not exist! "
                  "Please create it first via the write-service.")
        sys.exit(1)

    coll = Collection(COLLECTION_NAME)

    # Ensure partition exists (skip for independent collections like img_999999)
    if PARTITION_NAME != "_default" and not coll.has_partition(PARTITION_NAME):
        log.info(f"Creating partition {PARTITION_NAME}...")
        coll.create_partition(PARTITION_NAME)

    total = len(image_metas)
    log.info(f"Inserting {total} records into Milvus (batch={MILVUS_BATCH_SIZE})...")

    for i in range(0, total, MILVUS_BATCH_SIZE):
        batch = image_metas[i : i + MILVUS_BATCH_SIZE]

        rows = []
        now_ms = int(time.time() * 1000)
        for m in batch:
            rows.append({
                "image_pk": m["image_pk"],
                "global_vec": m["global_vec"],
                "product_id": m.get("product_id", f"P0{m['index']:07d}"),
                "is_evergreen": IS_EVERGREEN,
                "category_l1": m.get("category_l1", 0),
                "category_l2": m.get("category_l2", 0),
                "category_l3": m.get("category_l3", 0),
                "tags": m.get("tags", []),
                "color_code": 0,
                "material_code": 0,
                "style_code": 0,
                "season_code": 0,
                "ts_month": TS_MONTH,
                "promoted_at": 0,
                "created_at": now_ms,
            })

        coll.upsert(rows, partition_name=PARTITION_NAME)
        done_count = min(i + MILVUS_BATCH_SIZE, total)
        log.info(f"  Milvus upsert {done_count}/{total}")
        emit_progress("milvus", done_count, total, f"Milvus upsert {done_count}/{total}")

    log.info("Flushing Milvus...")
    coll.flush()
    log.info(f"Milvus insert done. Collection entity count: {coll.num_entities}")


# ── 5. PostgreSQL Insert (with URI) ──────────────────────────────────────────

def insert_pg_dedup(image_metas: list[dict]):
    """Batch insert into uri_dedup table, including URI column."""
    import psycopg2
    from psycopg2.extras import execute_values

    log.info(f"Connecting to PostgreSQL...")
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()

    PG_BATCH = 2000
    total = len(image_metas)
    inserted = 0

    log.info(f"Inserting {total} records into uri_dedup (with uri)...")

    for i in range(0, total, PG_BATCH):
        batch = image_metas[i : i + PG_BATCH]
        values = [(m["image_pk"], m["uri_hash"], m["uri"], TS_MONTH) for m in batch]
        execute_values(
            cur,
            "INSERT INTO uri_dedup (image_pk, uri_hash, uri, ts_month) VALUES %s "
            "ON CONFLICT (image_pk) DO UPDATE SET uri = EXCLUDED.uri",
            values,
        )
        inserted += len(batch)
        log.info(f"  PG insert {min(inserted, total)}/{total}")

    conn.commit()
    cur.close()
    conn.close()
    log.info("PostgreSQL insert done")


# ── 6. Kafka Merchant Events ─────────────────────────────────────────────────

def emit_kafka_events(image_metas: list[dict]):
    """Emit merchant binding events to Kafka."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        log.warning("kafka-python not installed, skipping Kafka events")
        return

    log.info(f"Connecting to Kafka at {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
    except Exception as e:
        log.warning(f"Failed to connect to Kafka: {e}. Skipping events.")
        return

    topic = "image-search.merchant-events"
    now_ms = int(time.time() * 1000)
    count = 0

    for m in image_metas:
        merchant_id = m.get("merchant_id", f"T20260101{(m['index'] % (MERCHANT_ID_END - MERCHANT_ID_START + 1)) + MERCHANT_ID_START}")

        event = {
            "event_type": "ADD",
            "image_pk": m["image_pk"],
            "merchant_id": merchant_id,
            "source": "batch-import",
            "timestamp": now_ms,
            "trace_id": hashlib.md5(m["image_pk"].encode()).hexdigest(),
        }
        producer.send(topic, key=m["image_pk"], value=event)
        count += 1

    producer.flush()
    producer.close()
    log.info(f"Kafka: sent {count} merchant events")


# ── 7. Metadata Enrichment ────────────────────────────────────────────────────

CLOTHING_CATEGORIES_L1 = {
    "shirt": 1, "dress": 2, "jacket": 3, "jeans": 4, "skirt": 5,
    "coat": 6, "sweater": 7, "hoodie": 8, "pants": 9, "blouse": 10,
    "tshirt": 11, "suit": 12, "shorts": 13, "blazer": 14, "cardigan": 15,
}

CLOTHING_CATEGORIES_L2 = {
    "shirt": 101, "dress": 201, "jacket": 301, "jeans": 401, "skirt": 501,
    "coat": 601, "sweater": 701, "hoodie": 801, "pants": 901, "blouse": 1001,
    "tshirt": 1101, "suit": 1201, "shorts": 1301, "blazer": 1401, "cardigan": 1501,
}


def enrich_metadata(image_metas: list[dict]):
    """Add metadata: product_id, merchant_id. Category/tags from CLIP zero-shot."""
    import random as _random

    for m in image_metas:
        # category_l1 / tags 已由 CLIPFeatureExtractor 填充，这里只补充缺省值
        m.setdefault("category_l1", 0)
        m.setdefault("category_l2", 0)
        m["category_l3"] = 0
        m["product_id"] = f"P0{m['index']:07d}"
        merchant_num = _random.randint(MERCHANT_ID_START, MERCHANT_ID_END)
        m["merchant_id"] = f"T20260101{merchant_num}"
        m.setdefault("tags", [])


# ── 8. Local Backup ──────────────────────────────────────────────────────────

def save_backup(image_metas: list[dict], backup_dir: str):
    """Save metadata.jsonl and copy images to backup directory."""
    os.makedirs(backup_dir, exist_ok=True)
    img_dir = os.path.join(backup_dir, "images")
    os.makedirs(img_dir, exist_ok=True)

    import shutil

    meta_path = os.path.join(backup_dir, "metadata.jsonl")
    count = 0
    with open(meta_path, "w") as f:
        for m in image_metas:
            # Copy image file
            src = m.get("local_path")
            if src and os.path.exists(src):
                dst = os.path.join(img_dir, os.path.basename(src))
                if not os.path.exists(dst):
                    shutil.copy2(src, dst)

            record = {
                "uri": m["uri"],
                "image_pk": m["image_pk"],
                "global_vec": m.get("global_vec"),
                "category_l1": m.get("category_l1"),
                "category_l1_name": m.get("category_l1_name"),
                "category_l1_conf": m.get("category_l1_conf"),
                "category_l2": m.get("category_l2"),
                "product_id": m.get("product_id"),
                "tags": m.get("tags"),
                "tags_name": m.get("tags_name"),
                "local_image": os.path.basename(src) if src else None,
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1

    log.info(f"Backup saved to {backup_dir}: {count} records in metadata.jsonl")


# ── 9. Main ──────────────────────────────────────────────────────────────────

async def async_main(args):
    # ── Task Tracker ────────────────────────────────────────────────────
    tracker = TaskTracker(args)
    task_t0 = time.time()

    # ── Resume logic ────────────────────────────────────────────────────
    checkpoint = load_checkpoint() if args.resume else None
    resume_stage = checkpoint.get("stage") if checkpoint else None
    resume_done = checkpoint.get("done", 0) if checkpoint else 0
    if checkpoint:
        args.url_file = checkpoint.get("url_file", args.url_file)
        args.count = checkpoint.get("count", args.count)
        args.download_dir = checkpoint.get("download_dir", args.download_dir)
        args.skip_kafka = checkpoint.get("skip_kafka", args.skip_kafka)
        args.model_path = checkpoint.get("model_path", args.model_path)
        args.local_dir = checkpoint.get("local_dir", args.local_dir)
        log.info(f"Resuming from stage={resume_stage}, done={resume_done}")

    log.info(f"=== Batch Import: {args.count} clothing images ===")
    log.info(f"Milvus: {MILVUS_HOST}:{MILVUS_PORT}, Collection: {COLLECTION_NAME}")
    log.info(f"Partition: {PARTITION_NAME}, ts_month: {TS_MONTH}")
    if args.proxies:
        log.info(f"Proxies: {args.proxies}")
    if args.backup_dir:
        log.info(f"Backup dir: {args.backup_dir}")

    STAGES = ["collect", "download", "extract", "milvus", "pg", "kafka"]

    def at_or_past_stage(stage: str) -> bool:
        if not resume_stage:
            return False
        return STAGES.index(resume_stage) >= STAGES.index(stage)

    def past_stage(stage: str) -> bool:
        if not resume_stage:
            return False
        return STAGES.index(resume_stage) > STAGES.index(stage)

    def _save_ckpt(stage: str, done: int):
        save_checkpoint({
            "stage": stage, "done": done,
            "url_file": args.url_file, "count": args.count,
            "download_dir": args.download_dir, "skip_kafka": args.skip_kafka,
            "model_path": args.model_path, "local_dir": args.local_dir,
        })

    # ── Resume: decide whether to skip collect+download ──────────────
    url_file_available = args.url_file and os.path.exists(args.url_file)
    download_dir_has_images = (
        os.path.isdir(args.download_dir) and len(os.listdir(args.download_dir)) > 0
    )

    if checkpoint and at_or_past_stage("download") and download_dir_has_images:
        log.info(f"Resuming past download stage, using local images from {args.download_dir}")
        image_metas = collect_image_urls_from_local(args.download_dir, args.count)
        log.info(f"Found {len(image_metas)} local images")
        emit_progress("download", len(image_metas), len(image_metas),
                       f"Using {len(image_metas)} already-downloaded images")
        tracker.stage_start("collect", {"source": "resume_local"})
        tracker.stage_end("collect", {"count": len(image_metas)})
        tracker.stage_start("download", {"source": "resume_local"})
        tracker.stage_end("download", {"success": len(image_metas), "failed": 0})
    elif checkpoint and not url_file_available and download_dir_has_images:
        log.info(f"URL file missing ({args.url_file}), using {args.download_dir}")
        image_metas = collect_image_urls_from_local(args.download_dir, args.count)
        log.info(f"Found {len(image_metas)} local images")
        emit_progress("download", len(image_metas), len(image_metas),
                       f"Using {len(image_metas)} already-downloaded images")
        tracker.stage_start("collect", {"source": "local_fallback"})
        tracker.stage_end("collect", {"count": len(image_metas)})
        tracker.stage_start("download", {"source": "local_fallback"})
        tracker.stage_end("download", {"success": len(image_metas), "failed": 0})
    else:
        tracker.stage_start("collect", {"url_file": args.url_file, "local_dir": args.local_dir})
        emit_progress("collect", 0, args.count, "Collecting image URLs...")
        image_metas = await collect_image_urls(args.count, args.local_dir, args.url_file)
        log.info(f"Collected {len(image_metas)} image URLs/paths")
        emit_progress("collect", len(image_metas), len(image_metas), f"Collected {len(image_metas)} URLs")
        tracker.stage_end("collect", {"count": len(image_metas)})

        if not image_metas:
            log.error("No images collected, aborting")
            emit_progress("error", 0, 0, "No images collected, aborting")
            tracker.set_summary({"status": "error", "reason": "no images collected"})
            tracker.save()
            return

        _save_ckpt("collect", len(image_metas))

        total_to_download = len(image_metas)
        tracker.stage_start("download", {"total": total_to_download, "proxies": args.proxies})
        emit_progress("download", 0, len(image_metas), "Downloading images...")
        image_metas = await download_images(
            image_metas, args.download_dir, proxies_str=args.proxies, tracker=tracker,
        )
        image_metas = [m for m in image_metas if m.get("local_path")]
        n_failed = total_to_download - len(image_metas)
        log.info(f"Have {len(image_metas)} images ready for processing")
        emit_progress("download", len(image_metas), len(image_metas), f"Downloaded {len(image_metas)} images")
        tracker.stage_end("download", {"success": len(image_metas), "failed": n_failed})
        tracker.save_failures_csv()

    if not image_metas:
        log.error("No images downloaded, aborting")
        emit_progress("error", 0, 0, "No images downloaded, aborting")
        return

    _save_ckpt("download", len(image_metas))

    # Step 3: Extract features + zero-shot classification
    tracker.stage_start("extract", {"device": "cuda" if torch.cuda.is_available() else "cpu",
                                     "model_path": args.model_path, "input_count": len(image_metas)})
    emit_progress("extract", 0, len(image_metas), "Loading CLIP model...")
    device = "cuda" if torch.cuda.is_available() else "cpu"
    if device == "cpu":
        log.warning("CUDA not available, falling back to CPU (will be slow!)")
    extractor = CLIPFeatureExtractor(device=device, model_path=args.model_path)
    image_metas = extractor.extract_all(image_metas)

    if not image_metas:
        log.error("Feature extraction produced no results, aborting")
        emit_progress("error", 0, 0, "Feature extraction produced no results")
        tracker.stage_end("extract", {"output_count": 0})
        tracker.set_summary({"status": "error", "reason": "feature extraction failed"})
        tracker.save()
        return
    log.info(f"Features extracted + classified for {len(image_metas)} images")
    emit_progress("extract", len(image_metas), len(image_metas),
                  f"Extracted + classified {len(image_metas)} images")

    # 统计品类分布
    cat_dist: dict[str, int] = {}
    for m in image_metas:
        cat_name = m.get("category_l1_name", str(m.get("category_l1", "unknown")))
        cat_dist[cat_name] = cat_dist.get(cat_name, 0) + 1
    tracker.stage_end("extract", {"output_count": len(image_metas), "category_distribution": cat_dist})

    # Step 4: Enrich metadata (product_id, merchant_id — categories already filled)
    enrich_metadata(image_metas)
    _save_ckpt("extract", len(image_metas))

    # Step 5: Write to Milvus
    if past_stage("milvus"):
        log.info("Skipping Milvus insert (already done in previous run)")
        tracker.stage_start("milvus", {"skipped": True})
        tracker.stage_end("milvus", {"skipped": True})
    else:
        tracker.stage_start("milvus", {"host": MILVUS_HOST, "collection": COLLECTION_NAME,
                                        "count": len(image_metas)})
        emit_progress("milvus", 0, len(image_metas), "Inserting into Milvus...")
        insert_milvus(image_metas)
        emit_progress("milvus", len(image_metas), len(image_metas), "Milvus insert complete")
        _save_ckpt("milvus", len(image_metas))
        tracker.stage_end("milvus", {"upserted": len(image_metas)})

    # Step 6: Write to PostgreSQL (with URI)
    if past_stage("pg"):
        log.info("Skipping PG insert (already done in previous run)")
        tracker.stage_start("pg", {"skipped": True})
        tracker.stage_end("pg", {"skipped": True})
    else:
        tracker.stage_start("pg", {"dsn_host": "localhost:5432", "count": len(image_metas)})
        emit_progress("pg", 0, len(image_metas), "Inserting into PostgreSQL...")
        insert_pg_dedup(image_metas)
        emit_progress("pg", len(image_metas), len(image_metas), "PostgreSQL insert complete")
        _save_ckpt("pg", len(image_metas))
        tracker.stage_end("pg", {"inserted": len(image_metas)})

    # Step 7: Kafka events (optional)
    if not args.skip_kafka:
        tracker.stage_start("kafka", {"broker": KAFKA_BROKER})
        emit_kafka_events(image_metas)
        tracker.stage_end("kafka", {"emitted": len(image_metas)})
    else:
        log.info("Skipping Kafka events (--skip-kafka)")

    # Step 8: Local backup (optional)
    if args.backup_dir:
        tracker.stage_start("backup", {"dir": args.backup_dir})
        save_backup(image_metas, args.backup_dir)
        tracker.stage_end("backup", {"count": len(image_metas)})

    clear_checkpoint()

    # ── 最终汇总 ──
    total_elapsed = time.time() - task_t0
    tracker.set_summary({
        "status": "success",
        "total_images": len(image_metas),
        "total_elapsed_sec": round(total_elapsed, 1),
        "download_failures": len(tracker.data["download_failures"]),
        "category_distribution": cat_dist,
    })
    tracker.save()

    log.info(f"=== Import complete: {len(image_metas)} images in {total_elapsed:.0f}s ===")
    emit_progress("done", len(image_metas), len(image_metas), f"Import complete: {len(image_metas)} images")


def main():
    parser = argparse.ArgumentParser(description="Batch import clothing images")
    parser.add_argument("--count", type=int, default=10000,
                        help="Number of images to import (default: 10000)")
    parser.add_argument("--url-file", type=str, default=None,
                        help="Text file with image URLs (one per line)")
    parser.add_argument("--local-dir", type=str, default=None,
                        help="Local directory with images (skip download)")
    parser.add_argument("--download-dir", type=str, default="/tmp/clothing_images",
                        help="Directory to download images to")
    parser.add_argument("--model-path", type=str,
                        default=os.path.expanduser("~/ViT-B-32.pt"),
                        help="Local path to CLIP model weights")
    parser.add_argument("--skip-kafka", action="store_true",
                        help="Skip Kafka merchant events")
    parser.add_argument("--device", type=str, default="cuda",
                        help="PyTorch device (cuda/cpu)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last checkpoint")
    parser.add_argument("--proxies", type=str, default=None,
                        help="Comma-separated SOCKS5 proxy URLs "
                             "(e.g. socks5://127.0.0.1:61081,socks5://127.0.0.1:61082)")
    parser.add_argument("--backup-dir", type=str, default=None,
                        help="Local backup directory (images + metadata.jsonl)")
    parser.add_argument("--partition", type=str, default=None,
                        help="Milvus partition name (e.g. p_202604_svip)")
    parser.add_argument("--ts-month", type=int, default=None,
                        help="ts_month value (e.g. 202604)")
    parser.add_argument("--evergreen", action="store_true",
                        help="Set is_evergreen=True for evergreen partition")
    parser.add_argument("--collection", type=str, default=None,
                        help="Milvus collection name (default: global_images_hot)")
    args = parser.parse_args()

    # 命令行参数覆盖全局常量
    global PARTITION_NAME, TS_MONTH, IS_EVERGREEN, COLLECTION_NAME
    if args.partition:
        PARTITION_NAME = args.partition
    if args.ts_month:
        TS_MONTH = args.ts_month
    if args.evergreen:
        IS_EVERGREEN = True
    if args.collection:
        COLLECTION_NAME = args.collection

    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
