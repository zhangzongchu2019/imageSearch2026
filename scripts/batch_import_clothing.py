#!/usr/bin/env python3
"""
批量导入 10,000 张服装图片到 Milvus + PostgreSQL

使用 open-clip-torch (ViT-B/32) 提取 512 维特征，PCA 降维到 256 维，
直接批量写入 Milvus global_images_hot 和 PG uri_dedup。

Usage:
    python3 /tmp/batch_import_clothing.py --count 10000
    python3 /tmp/batch_import_clothing.py --count 100 --local-dir /path/to/images
    python3 /tmp/batch_import_clothing.py --count 10000 --skip-kafka
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
DOWNLOAD_CONCURRENCY = 10
GPU_BATCH_SIZE = 128
MILVUS_BATCH_SIZE = 5000
MERCHANT_ID_START = 10001
MERCHANT_ID_END = 12999
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "/tmp/batch_import_checkpoint.json")


# ── 1. Image URL Collection ─────────────────────────────────────────────────

# Curated list of public clothing image URL patterns.
# We use Open Images V7 downloader-compatible URLs (hosted on Flickr/Google).
# For reliability, we also provide a Fashionpedia fallback and local-dir mode.

FASHIONPEDIA_MANIFEST = (
    "https://raw.githubusercontent.com/cvdfoundation/fashionpedia/main/train_images.txt"
)

# Backup: generate deterministic synthetic URLs from known open image datasets.
# These are placeholder patterns; real deployment should use actual manifest files.
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
            # Skip header lines (e.g. "full_url", "url", column names)
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
    Generate image metadata using Unsplash Source (public, no auth needed).
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
        # Use picsum.photos for reliable public images (clothing-like via seed)
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


# ── 2. Image Download ────────────────────────────────────────────────────────

async def download_images(
    image_metas: list[dict],
    download_dir: str = "/tmp/clothing_images",
) -> list[dict]:
    """Download images concurrently. Returns updated metas with local_path set."""
    import httpx

    os.makedirs(download_dir, exist_ok=True)

    # Skip already-local images
    to_download = [m for m in image_metas if m["local_path"] is None]
    already_local = [m for m in image_metas if m["local_path"] is not None]

    if not to_download:
        log.info("All images are local, skipping download")
        return image_metas

    log.info(f"Downloading {len(to_download)} images (concurrency={DOWNLOAD_CONCURRENCY})...")

    sem = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
    success = 0
    failed = 0

    async def _download_one(client: httpx.AsyncClient, meta: dict):
        nonlocal success, failed
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

    dl_headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        "Referer": "https://www.szwego.com/",
    }
    async with httpx.AsyncClient(timeout=30, headers=dl_headers) as client:
        tasks = [_download_one(client, m) for m in to_download]
        # Process in chunks to show progress
        chunk_size = 500
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i : i + chunk_size]
            await asyncio.gather(*chunk, return_exceptions=True)
            done_count = min(i + chunk_size, len(tasks))
            log.info(f"  Download progress: {done_count}/{len(tasks)} "
                     f"(ok={success}, fail={failed})")
            emit_progress("download", done_count, len(tasks),
                          f"Downloaded {done_count}/{len(tasks)} (ok={success}, fail={failed})")

    log.info(f"Download complete: {success} ok, {failed} failed")

    # Filter out failed downloads
    result = already_local + [m for m in to_download if m["local_path"] is not None]
    return result


# ── 3. Feature Extraction (CLIP + PCA) ───────────────────────────────────────

class CLIPFeatureExtractor:
    """CLIP ViT-B/32 feature extractor with PCA 512→256."""

    def __init__(self, device: str = "cuda", model_path: Optional[str] = None):
        self.device = torch.device(device if torch.cuda.is_available() else "cpu")
        log.info(f"Loading CLIP ViT-B/32 on {self.device}...")

        if model_path and os.path.exists(model_path):
            # Load from local checkpoint
            log.info(f"Loading from local checkpoint: {model_path}")
            self.model, _, self.preprocess = open_clip.create_model_and_transforms(
                "ViT-B-32", pretrained=model_path, device=self.device,
            )
        else:
            # Download from HuggingFace
            self.model, _, self.preprocess = open_clip.create_model_and_transforms(
                "ViT-B-32", pretrained="laion2b_s34b_b79k", device=self.device,
            )
        self.model.eval()
        self.pca_components = None  # Fitted after first batch
        self.pca_mean = None
        log.info("CLIP model loaded")

    def _load_image(self, path: str) -> Optional[torch.Tensor]:
        try:
            img = Image.open(path).convert("RGB")
            return self.preprocess(img)
        except Exception:
            return None

    def _fit_pca(self, features_512: np.ndarray):
        """Fit PCA on the first batch of features."""
        n_samples = len(features_512)
        log.info(f"Fitting PCA ({CLIP_DIM} → {EMBEDDING_DIM}) on {n_samples} samples...")
        self.pca_mean = features_512.mean(axis=0)
        centered = features_512 - self.pca_mean

        if n_samples < EMBEDDING_DIM:
            # Not enough samples for full PCA; use random projection as fallback
            log.warning(f"Only {n_samples} samples < {EMBEDDING_DIM} dims, "
                        "using truncated projection (results may be suboptimal)")
            rng = np.random.RandomState(42)
            proj = rng.randn(CLIP_DIM, EMBEDDING_DIM).astype(np.float32)
            # Orthogonalize
            Q, _ = np.linalg.qr(proj)
            self.pca_components = Q[:, :EMBEDDING_DIM]
        else:
            # SVD for PCA
            U, S, Vt = np.linalg.svd(centered, full_matrices=False)
            self.pca_components = Vt[:EMBEDDING_DIM].T  # (512, 256)
        log.info("PCA fitted")

    def _apply_pca(self, features_512: np.ndarray) -> np.ndarray:
        centered = features_512 - self.pca_mean
        reduced = centered @ self.pca_components  # (N, 256)
        # L2 normalize
        norms = np.linalg.norm(reduced, axis=1, keepdims=True)
        norms = np.maximum(norms, 1e-8)
        return (reduced / norms).astype(np.float32)

    @torch.no_grad()
    def extract_batch(self, paths: list[str]) -> tuple[list[str], np.ndarray]:
        """Extract 256-dim features for a batch of image paths.

        Returns (valid_paths, features_256) where failed images are excluded.
        """
        tensors = []
        valid_paths = []
        for p in paths:
            t = self._load_image(p)
            if t is not None:
                tensors.append(t)
                valid_paths.append(p)

        if not tensors:
            return [], np.empty((0, EMBEDDING_DIM), dtype=np.float32)

        batch = torch.stack(tensors).to(self.device)
        features = self.model.encode_image(batch)
        features_512 = features.cpu().numpy().astype(np.float32)

        # L2 normalize 512-dim first
        norms = np.linalg.norm(features_512, axis=1, keepdims=True)
        features_512 = features_512 / np.maximum(norms, 1e-8)

        return valid_paths, features_512

    def extract_all(self, image_metas: list[dict]) -> list[dict]:
        """Extract features for all images. Updates metas with 'global_vec'."""
        paths = [m["local_path"] for m in image_metas]
        path_to_meta = {m["local_path"]: m for m in image_metas}

        all_512 = []
        all_valid_paths = []

        total_batches = (len(paths) + GPU_BATCH_SIZE - 1) // GPU_BATCH_SIZE
        log.info(f"Extracting features: {len(paths)} images, {total_batches} batches "
                 f"(batch_size={GPU_BATCH_SIZE})")

        t0 = time.time()
        for i in range(0, len(paths), GPU_BATCH_SIZE):
            batch_paths = paths[i : i + GPU_BATCH_SIZE]
            valid_paths, features_512 = self.extract_batch(batch_paths)
            all_valid_paths.extend(valid_paths)
            if len(features_512) > 0:
                all_512.append(features_512)

            batch_num = i // GPU_BATCH_SIZE + 1
            if batch_num % 10 == 0 or batch_num == total_batches:
                elapsed = time.time() - t0
                rate = len(all_valid_paths) / elapsed if elapsed > 0 else 0
                log.info(f"  Batch {batch_num}/{total_batches}, "
                         f"{len(all_valid_paths)} done, {rate:.0f} img/sec")
                emit_progress("extract", len(all_valid_paths), len(paths),
                              f"Batch {batch_num}/{total_batches}, {rate:.0f} img/sec")

        if not all_512:
            log.error("No features extracted!")
            return []

        all_512 = np.concatenate(all_512, axis=0)
        log.info(f"Got {len(all_512)} x {CLIP_DIM}-dim features in {time.time()-t0:.1f}s")

        # Fit PCA on all data (or a large sample)
        if len(all_512) > 2000:
            pca_sample = all_512[np.random.choice(len(all_512), 2000, replace=False)]
        else:
            pca_sample = all_512
        self._fit_pca(pca_sample)

        # Apply PCA
        all_256 = self._apply_pca(all_512)
        log.info(f"PCA done: {all_256.shape}")

        # Map back to metas
        result = []
        for path, vec in zip(all_valid_paths, all_256):
            meta = path_to_meta[path]
            meta["global_vec"] = vec.tolist()
            result.append(meta)

        log.info(f"Feature extraction complete: {len(result)} images")
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
        for m in batch:
            rows.append({
                "image_pk": m["image_pk"],
                "global_vec": m["global_vec"],
                "product_id": m.get("product_id", f"P0{m['index']:07d}"),
                "is_evergreen": False,
                "category_l1": m.get("category_l1", 0),
                "category_l2": m.get("category_l2", 0),
                "tags": m.get("tags", []),
                "ts_month": TS_MONTH,
            })

        coll.upsert(rows, partition_name=PARTITION_NAME)
        done_count = min(i + MILVUS_BATCH_SIZE, total)
        log.info(f"  Milvus upsert {done_count}/{total}")
        emit_progress("milvus", done_count, total, f"Milvus upsert {done_count}/{total}")

    log.info("Flushing Milvus...")
    coll.flush()
    log.info(f"Milvus insert done. Collection entity count: {coll.num_entities}")


# ── 5. PostgreSQL Insert ──────────────────────────────────────────────────────

def insert_pg_dedup(image_metas: list[dict]):
    """Batch insert into uri_dedup table."""
    import psycopg2
    from psycopg2.extras import execute_values

    log.info(f"Connecting to PostgreSQL...")
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()

    PG_BATCH = 2000
    total = len(image_metas)
    inserted = 0

    log.info(f"Inserting {total} records into uri_dedup...")

    for i in range(0, total, PG_BATCH):
        batch = image_metas[i : i + PG_BATCH]
        values = [(m["image_pk"], m["uri_hash"], TS_MONTH) for m in batch]
        execute_values(
            cur,
            "INSERT INTO uri_dedup (image_pk, uri_hash, ts_month) VALUES %s "
            "ON CONFLICT (image_pk) DO NOTHING",
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
    """Add category/tag metadata based on URI patterns or index."""
    import random as _random

    for m in image_metas:
        # 一级分类: 服装 (clothing → 1)
        m["category_l1"] = 1   # shirt/clothing
        m["category_l2"] = 101
        m["category_l3"] = 0

        # 商品 ID: P0 + 7位数字
        m["product_id"] = f"P0{m['index']:07d}"

        # 商家 ID: T20260101{10001~12999}
        merchant_num = _random.randint(MERCHANT_ID_START, MERCHANT_ID_END)
        m["merchant_id"] = f"T20260101{merchant_num}"

        m["tags"] = [m["category_l1"]]


# ── 8. Main ──────────────────────────────────────────────────────────────────

async def async_main(args):
    # ── Resume logic ────────────────────────────────────────────────────
    checkpoint = load_checkpoint() if args.resume else None
    resume_stage = checkpoint.get("stage") if checkpoint else None
    resume_done = checkpoint.get("done", 0) if checkpoint else 0
    # Restore original args from checkpoint if resuming
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

    STAGES = ["collect", "download", "extract", "milvus", "pg", "kafka"]

    def at_or_past_stage(stage: str) -> bool:
        """Return True if the checkpoint stage is at or past the given stage."""
        if not resume_stage:
            return False
        return STAGES.index(resume_stage) >= STAGES.index(stage)

    def past_stage(stage: str) -> bool:
        """Return True if the checkpoint stage is strictly past the given stage."""
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
        # Case 1: checkpoint says download is done, use local images
        log.info(f"Resuming past download stage, using local images from {args.download_dir}")
        image_metas = collect_image_urls_from_local(args.download_dir, args.count)
        log.info(f"Found {len(image_metas)} local images")
        emit_progress("download", len(image_metas), len(image_metas),
                       f"Using {len(image_metas)} already-downloaded images")
    elif checkpoint and not url_file_available and download_dir_has_images:
        # Case 2: URL file deleted but images still on disk
        log.info(f"URL file missing ({args.url_file}), using {args.download_dir}")
        image_metas = collect_image_urls_from_local(args.download_dir, args.count)
        log.info(f"Found {len(image_metas)} local images")
        emit_progress("download", len(image_metas), len(image_metas),
                       f"Using {len(image_metas)} already-downloaded images")
    else:
        # Normal flow: collect URLs, then download
        emit_progress("collect", 0, args.count, "Collecting image URLs...")
        image_metas = await collect_image_urls(args.count, args.local_dir, args.url_file)
        log.info(f"Collected {len(image_metas)} image URLs/paths")
        emit_progress("collect", len(image_metas), len(image_metas), f"Collected {len(image_metas)} URLs")

        if not image_metas:
            log.error("No images collected, aborting")
            emit_progress("error", 0, 0, "No images collected, aborting")
            return

        _save_ckpt("collect", len(image_metas))

        # Download images (already-downloaded are auto-skipped by file exists check)
        emit_progress("download", 0, len(image_metas), "Downloading images...")
        image_metas = await download_images(image_metas, args.download_dir)
        image_metas = [m for m in image_metas if m.get("local_path")]
        log.info(f"Have {len(image_metas)} images ready for processing")
        emit_progress("download", len(image_metas), len(image_metas), f"Downloaded {len(image_metas)} images")

    if not image_metas:
        log.error("No images downloaded, aborting")
        emit_progress("error", 0, 0, "No images downloaded, aborting")
        return

    _save_ckpt("download", len(image_metas))

    # Step 3: Enrich metadata
    enrich_metadata(image_metas)

    # Step 4: Extract features
    # On resume past extract, skip (but we always re-extract since vectors are in memory only)
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
    log.info(f"Features extracted for {len(image_metas)} images")
    emit_progress("extract", len(image_metas), len(image_metas), f"Extracted features for {len(image_metas)} images")
    _save_ckpt("extract", len(image_metas))

    # Step 5: Write to Milvus (upsert is idempotent, safe to re-run)
    if past_stage("milvus"):
        log.info("Skipping Milvus insert (already done in previous run)")
    else:
        emit_progress("milvus", 0, len(image_metas), "Inserting into Milvus...")
        insert_milvus(image_metas)
        emit_progress("milvus", len(image_metas), len(image_metas), "Milvus insert complete")
        _save_ckpt("milvus", len(image_metas))

    # Step 6: Write to PostgreSQL (ON CONFLICT DO NOTHING, idempotent)
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
                        default="/home/zzc/open_clip_model.safetensors",
                        help="Local path to CLIP model weights (.safetensors)")
    parser.add_argument("--skip-kafka", action="store_true",
                        help="Skip Kafka merchant events")
    parser.add_argument("--device", type=str, default="cuda",
                        help="PyTorch device (cuda/cpu)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last checkpoint")
    args = parser.parse_args()

    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
