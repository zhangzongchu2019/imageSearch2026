#!/usr/bin/env python3
"""
批量导入服装图片到 Milvus + PostgreSQL

使用 open-clip-torch (ViT-B/32, openai 预训练) 提取 512 维特征，
正交随机投影降维到 256 维（与 inference-service seed=42 一致），
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


def emit_progress(stage: str, completed: int, total: int, message: str):
    """Emit a ##PROGRESS## JSON line for SSE consumers."""
    import json as _json
    line = _json.dumps({"stage": stage, "completed": completed, "total": total, "message": message})
    print(f"##PROGRESS##{line}", flush=True)

# ── Constants ────────────────────────────────────────────────────────────────
EMBEDDING_DIM = 256
CLIP_DIM = 512
MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))
PG_DSN = os.getenv("PG_DSN", "postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
COLLECTION_NAME = "global_images_hot"
PARTITION_NAME = "p_202603"
TS_MONTH = 202603
DOWNLOAD_CONCURRENCY = 6      # per-channel concurrency
GPU_BATCH_SIZE = 128
MILVUS_BATCH_SIZE = 5000
MERCHANT_ID_START = 10001
MERCHANT_ID_END = 12999
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "/tmp/batch_import_checkpoint.json")


# ── Projection Matrix (matches inference-service exactly) ─────────────────
def build_projection_matrix() -> np.ndarray:
    """Build orthogonal projection matrix (512 → 256), seed=42.
    Identical to inference-service/app/main.py."""
    rng = np.random.RandomState(42)
    proj = rng.randn(CLIP_DIM, EMBEDDING_DIM).astype(np.float32)
    u, _, vt = np.linalg.svd(proj, full_matrices=False)
    return u  # (512, 256) orthogonal matrix


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
        client = httpx.AsyncClient(
            timeout=30,
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
            if os.path.exists(fpath) and os.path.getsize(fpath) > 1000:
                meta["local_path"] = fpath
                success += 1
                return
            try:
                resp = await client.get(meta["uri"], follow_redirects=True)
                ct = resp.headers.get("content-type", "")
                is_image = ct.startswith("image/") or len(resp.content) > 2000
                if resp.status_code == 200 and is_image:
                    with open(fpath, "wb") as f:
                        f.write(resp.content)
                    meta["local_path"] = fpath
                    success += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
            # Small delay to avoid bursting
            await asyncio.sleep(0.05)

    # Round-robin assign tasks to channels
    tasks = []
    for i, meta in enumerate(to_download):
        channel_idx = i % len(channels)
        tasks.append(_download_one(meta, channel_idx))

    total_tasks = len(tasks)
    log.info(f"Downloading {total_tasks} images across {len(channels)} channels ...")

    chunk_size = 500
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
    """CLIP ViT-B/32 feature extractor with orthogonal projection 512→256.

    Uses the 'openai' pretrained weights and seed=42 projection matrix,
    matching inference-service exactly.

    v0.3: 新增零样本品类识别 + 标签提取 (与 inference-service 一致)
    """

    # 品类词表 — 与 inference-service 保持一致
    CATEGORIES = {
        "服装": ["a photo of clothing", "a photo of garment", "a photo of dress shirt jacket coat pants skirt"],
        "鞋帽": ["a photo of shoes", "a photo of boots sneakers heels hat cap"],
        "箱包": ["a photo of bag", "a photo of handbag backpack wallet luggage purse"],
        "珠宝玉器": ["a photo of jewelry", "a photo of jade gold diamond gem ring bracelet"],
        "首饰配饰": ["a photo of accessories", "a photo of necklace earring brooch hairpin"],
    }

    # 标签词表 — 与 inference-service 保持一致
    TAGS = {
        "上衣": "a photo of a top shirt blouse",
        "裤子": "a photo of pants trousers",
        "裙子": "a photo of a skirt dress",
        "外套": "a photo of a jacket coat outerwear",
        "T恤": "a photo of a t-shirt tee",
        "运动鞋": "a photo of sneakers running shoes",
        "高跟鞋": "a photo of high heels pumps",
        "皮鞋": "a photo of leather shoes",
        "靴子": "a photo of boots",
        "手提包": "a photo of a handbag tote bag",
        "双肩包": "a photo of a backpack",
        "钱包": "a photo of a wallet",
        "旅行箱": "a photo of luggage suitcase",
        "翡翠": "a photo of jade jadeite",
        "黄金": "a photo of gold jewelry",
        "钻石": "a photo of diamond",
        "手镯": "a photo of a bracelet bangle",
        "戒指": "a photo of a ring",
        "项链": "a photo of a necklace",
        "耳环": "a photo of earrings",
        "真皮": "a photo of genuine leather product",
        "PU皮": "a photo of PU leather synthetic product",
        "棉质": "a photo of cotton fabric product",
        "复古风": "a photo of vintage retro style product",
        "简约风": "a photo of minimalist simple style product",
        "红色": "a photo of a red product",
        "蓝色": "a photo of a blue product",
        "黑色": "a photo of a black product",
        "白色": "a photo of a white product",
        "棕色": "a photo of a brown product",
        "春季": "a photo of spring season fashion",
        "夏季": "a photo of summer season fashion",
        "秋季": "a photo of autumn fall season fashion",
        "冬季": "a photo of winter season fashion",
    }

    # 品类名 → 整数编码 (Milvus schema 用 INT32)
    CATEGORY_ID_MAP = {name: idx for idx, name in enumerate(CATEGORIES.keys())}

    def __init__(self, device: str = "cuda", model_path: Optional[str] = None):
        global PROJECTION_MATRIX

        self.device = torch.device(device if torch.cuda.is_available() else "cpu")
        log.info(f"Loading CLIP ViT-B/32 on {self.device}...")

        # Monkey-patch torch.load for PyTorch 2.6+ compatibility
        _orig_torch_load = torch.load
        torch.load = lambda *a, **kw: _orig_torch_load(*a, **{**kw, "weights_only": False})

        if model_path and os.path.exists(model_path):
            log.info(f"Loading from local checkpoint: {model_path}")
            self.model, _, self.preprocess = open_clip.create_model_and_transforms(
                "ViT-B-32", pretrained=model_path, device=self.device,
            )
        else:
            log.info("Downloading CLIP ViT-B/32 (openai pretrained) ...")
            self.model, _, self.preprocess = open_clip.create_model_and_transforms(
                "ViT-B-32", pretrained="openai", device=self.device,
            )

        torch.load = _orig_torch_load  # restore
        self.model.eval()

        # Build projection matrix (same as inference-service)
        if PROJECTION_MATRIX is None:
            PROJECTION_MATRIX = build_projection_matrix()
        self.projection = PROJECTION_MATRIX

        # 预计算零样本分类文本向量 (512 维)
        self._init_zero_shot_features()

        log.info("CLIP model loaded, projection matrix initialized (512→256, seed=42), "
                 f"{len(self.category_names)} categories, {len(self.tag_names)} tags ready")

    def _init_zero_shot_features(self):
        """预计算品类/标签文本向量，用于零样本分类"""
        tokenizer = open_clip.get_tokenizer("ViT-B-32")

        # 品类文本向量
        self.category_names = list(self.CATEGORIES.keys())
        cat_texts = []
        for cat_name in self.category_names:
            cat_texts.extend(self.CATEGORIES[cat_name])

        cat_tokens = tokenizer(cat_texts).to(self.device)
        with torch.no_grad():
            cat_feats = self.model.encode_text(cat_tokens)
        cat_feats = cat_feats / cat_feats.norm(dim=-1, keepdim=True)

        # 每个品类多个描述取平均
        idx = 0
        cat_avg = []
        for cat_name in self.category_names:
            n = len(self.CATEGORIES[cat_name])
            avg = cat_feats[idx:idx + n].mean(dim=0)
            avg = avg / avg.norm()
            cat_avg.append(avg)
            idx += n
        self.category_text_features = torch.stack(cat_avg).float()  # (N_cat, 512)

        # 标签文本向量
        self.tag_names = list(self.TAGS.keys())
        tag_texts = [self.TAGS[t] for t in self.tag_names]
        tag_tokens = tokenizer(tag_texts).to(self.device)
        with torch.no_grad():
            tag_feats = self.model.encode_text(tag_tokens)
        self.tag_text_features = (tag_feats / tag_feats.norm(dim=-1, keepdim=True)).float()  # (N_tag, 512)

    def _load_image(self, path: str) -> Optional[torch.Tensor]:
        try:
            img = Image.open(path).convert("RGB")
            return self.preprocess(img)
        except Exception:
            return None

    def _project(self, features_512: np.ndarray) -> np.ndarray:
        """Project 512-dim features to 256-dim using orthogonal matrix + L2 normalize."""
        reduced = features_512 @ self.projection  # (N, 256)
        norms = np.linalg.norm(reduced, axis=1, keepdims=True)
        norms = np.maximum(norms, 1e-8)
        return (reduced / norms).astype(np.float32)

    def _classify_batch_512(self, features_512_t: torch.Tensor,
                            top_k: int = 8, threshold: float = 0.2) -> list[dict]:
        """用 512 维原始特征做零样本品类/标签分类（精度最高）"""
        feat_norm = features_512_t.float()
        feat_norm = feat_norm / feat_norm.norm(dim=-1, keepdim=True)

        cat_text = self.category_text_features.to(self.device)  # (N_cat, 512)
        tag_text = self.tag_text_features.to(self.device)       # (N_tag, 512)

        cat_sims = feat_norm @ cat_text.T  # (B, N_cat)
        tag_sims = feat_norm @ tag_text.T  # (B, N_tag)

        top_k = min(top_k, len(self.tag_names))
        results = []
        for i in range(len(features_512_t)):
            cat_idx = cat_sims[i].argmax().item()
            cat_conf = cat_sims[i][cat_idx].item()
            cat_name = self.category_names[cat_idx]

            topk_result = tag_sims[i].topk(top_k)
            tags = [
                self.tag_names[j]
                for j, s in zip(topk_result.indices.tolist(), topk_result.values.tolist())
                if s > threshold
            ]
            # 标签 hash 编码 (与 inference-service 一致)
            tag_ids = [hash(t) % 4096 for t in tags]

            results.append({
                "category_l1": cat_name,
                "category_l1_id": self.CATEGORY_ID_MAP[cat_name],
                "category_l1_conf": round(cat_conf, 4),
                "tags_name": tags,
                "tags": tag_ids,
            })
        return results

    @torch.no_grad()
    def extract_batch(self, paths: list[str]) -> tuple[list[str], np.ndarray, list[dict]]:
        """Extract 256-dim features + zero-shot classification for a batch.

        Returns (valid_paths, features_256, classifications).
        """
        tensors = []
        valid_paths = []
        for p in paths:
            t = self._load_image(p)
            if t is not None:
                tensors.append(t)
                valid_paths.append(p)

        if not tensors:
            return [], np.empty((0, EMBEDDING_DIM), dtype=np.float32), []

        batch = torch.stack(tensors).to(self.device)
        features = self.model.encode_image(batch)

        # 零样本分类 (用 512 维原始特征，精度最高)
        classifications = self._classify_batch_512(features)

        features_512 = features.cpu().numpy().astype(np.float32)
        # Project to 256-dim
        features_256 = self._project(features_512)

        return valid_paths, features_256, classifications

    def extract_all(self, image_metas: list[dict]) -> list[dict]:
        """Extract features + zero-shot classification for all images.

        Updates metas with 'global_vec', 'category_l1', 'tags', etc.
        """
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
            meta["tags"] = cls["tags"]
            meta["tags_name"] = cls["tags_name"]
            result.append(meta)

        log.info(f"Feature extraction + classification complete: {len(result)} images")
        return result


# ── 4. Milvus Insert ─────────────────────────────────────────────────────────

def insert_milvus(image_metas: list[dict]):
    """Batch upsert into Milvus global_images_hot."""
    from pymilvus import connections, Collection, utility

    log.info(f"Connecting to Milvus at {MILVUS_HOST}:{MILVUS_PORT}...")
    connections.connect(alias="default", host=MILVUS_HOST, port=MILVUS_PORT)

    if not utility.has_collection(COLLECTION_NAME):
        log.error(f"Collection {COLLECTION_NAME} does not exist! "
                  "Please create it first via the write-service.")
        sys.exit(1)

    coll = Collection(COLLECTION_NAME)

    # Ensure partition exists
    if not coll.has_partition(PARTITION_NAME):
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
                "is_evergreen": False,
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
    elif checkpoint and not url_file_available and download_dir_has_images:
        log.info(f"URL file missing ({args.url_file}), using {args.download_dir}")
        image_metas = collect_image_urls_from_local(args.download_dir, args.count)
        log.info(f"Found {len(image_metas)} local images")
        emit_progress("download", len(image_metas), len(image_metas),
                       f"Using {len(image_metas)} already-downloaded images")
    else:
        emit_progress("collect", 0, args.count, "Collecting image URLs...")
        image_metas = await collect_image_urls(args.count, args.local_dir, args.url_file)
        log.info(f"Collected {len(image_metas)} image URLs/paths")
        emit_progress("collect", len(image_metas), len(image_metas), f"Collected {len(image_metas)} URLs")

        if not image_metas:
            log.error("No images collected, aborting")
            emit_progress("error", 0, 0, "No images collected, aborting")
            return

        _save_ckpt("collect", len(image_metas))

        emit_progress("download", 0, len(image_metas), "Downloading images...")
        image_metas = await download_images(
            image_metas, args.download_dir, proxies_str=args.proxies,
        )
        image_metas = [m for m in image_metas if m.get("local_path")]
        log.info(f"Have {len(image_metas)} images ready for processing")
        emit_progress("download", len(image_metas), len(image_metas), f"Downloaded {len(image_metas)} images")

    if not image_metas:
        log.error("No images downloaded, aborting")
        emit_progress("error", 0, 0, "No images downloaded, aborting")
        return

    _save_ckpt("download", len(image_metas))

    # Step 3: Extract features + zero-shot classification
    emit_progress("extract", 0, len(image_metas), "Loading CLIP model...")
    device = "cuda" if torch.cuda.is_available() else "cpu"
    if device == "cpu":
        log.warning("CUDA not available, falling back to CPU (will be slow!)")
    extractor = CLIPFeatureExtractor(device=device, model_path=args.model_path)
    image_metas = extractor.extract_all(image_metas)

    if not image_metas:
        log.error("Feature extraction produced no results, aborting")
        emit_progress("error", 0, 0, "Feature extraction produced no results")
        return
    log.info(f"Features extracted + classified for {len(image_metas)} images")
    emit_progress("extract", len(image_metas), len(image_metas),
                  f"Extracted + classified {len(image_metas)} images")

    # Step 4: Enrich metadata (product_id, merchant_id — categories already filled)
    enrich_metadata(image_metas)
    _save_ckpt("extract", len(image_metas))

    # Step 5: Write to Milvus
    if past_stage("milvus"):
        log.info("Skipping Milvus insert (already done in previous run)")
    else:
        emit_progress("milvus", 0, len(image_metas), "Inserting into Milvus...")
        insert_milvus(image_metas)
        emit_progress("milvus", len(image_metas), len(image_metas), "Milvus insert complete")
        _save_ckpt("milvus", len(image_metas))

    # Step 6: Write to PostgreSQL (with URI)
    if past_stage("pg"):
        log.info("Skipping PG insert (already done in previous run)")
    else:
        emit_progress("pg", 0, len(image_metas), "Inserting into PostgreSQL...")
        insert_pg_dedup(image_metas)
        emit_progress("pg", len(image_metas), len(image_metas), "PostgreSQL insert complete")
        _save_ckpt("pg", len(image_metas))

    # Step 7: Kafka events (optional)
    if not args.skip_kafka:
        emit_kafka_events(image_metas)
    else:
        log.info("Skipping Kafka events (--skip-kafka)")

    # Step 8: Local backup (optional)
    if args.backup_dir:
        save_backup(image_metas, args.backup_dir)

    clear_checkpoint()
    log.info(f"=== Import complete: {len(image_metas)} images ===")
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
    args = parser.parse_args()

    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
