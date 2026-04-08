#!/usr/bin/env python3
"""
4 卡并行特征提取 + 两级分类。

将已下载图片分成 N 份，每 GPU 一个独立进程，结果写到 JSONL。
主进程等待全部完成后合并。

Usage:
    python3 scripts/extract_multi_gpu.py --image-dir /tmp/clothing_images \
        --model-path /data/imgsrch/models/ViT-L-14.pt --num-gpus 4
"""
import argparse
import json
import logging
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

import numpy as np
import torch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("extract_multi_gpu")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# 添加 scripts 目录到 path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

OUTPUT_DIR = "/data/imgsrch/task_logs/extract_output"
PROGRESS_DIR = "/tmp/gpu_progress"
GPU_BATCH_SIZE = 128


def gpu_worker(gpu_id: int, image_paths: list[str], model_path: str,
               output_file: str, progress_file: str):
    """单 GPU worker 进程：加载模型 → 提取特征 → 写 JSONL"""
    # 重新配置 logging（spawn 模式下需要）
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [GPU{gpu_id}] %(message)s",
                        datefmt="%H:%M:%S", force=True)
    wlog = logging.getLogger(f"gpu{gpu_id}")

    device = f"cuda:{gpu_id}"
    wlog.info(f"Starting on {device}, {len(image_paths)} images")

    # 导入 CLIPFeatureExtractor
    from batch_import_clothing import CLIPFeatureExtractor, EMBEDDING_DIM

    extractor = CLIPFeatureExtractor(device=device, model_path=model_path)
    wlog.info(f"Models loaded on {device}")

    total = len(image_paths)
    total_batches = (total + GPU_BATCH_SIZE - 1) // GPU_BATCH_SIZE
    t0 = time.time()
    done = 0

    with open(output_file, "w") as out_f:
        for i in range(0, total, GPU_BATCH_SIZE):
            batch_paths = image_paths[i:i + GPU_BATCH_SIZE]
            valid_paths, features_256, classifications = extractor.extract_batch(batch_paths)

            # 写每条记录为一行 JSONL
            for path, vec, cls in zip(valid_paths, features_256, classifications):
                record = {
                    "local_path": path,
                    "global_vec": vec.tolist(),
                    "category_l1_id": cls["category_l1_id"],
                    "category_l1": cls["category_l1"],
                    "category_l1_conf": cls["category_l1_conf"],
                    "category_l2_id": cls.get("category_l2_id", 0),
                    "category_l3_id": cls.get("category_l3_id", 0),
                    "tags": cls["tags"],
                    "tags_name": cls["tags_name"],
                }
                out_f.write(json.dumps(record, ensure_ascii=False) + "\n")

            done += len(valid_paths)
            batch_num = i // GPU_BATCH_SIZE + 1

            if batch_num % 10 == 0 or batch_num == total_batches:
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                wlog.info(f"Batch {batch_num}/{total_batches}, {done} done, {rate:.0f} img/s")

            # 写进度文件
            try:
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                with open(progress_file, "w") as pf:
                    json.dump({"gpu": gpu_id, "done": done, "total": total,
                               "rate": round(rate, 1)}, pf)
            except Exception:
                pass

    elapsed = time.time() - t0
    wlog.info(f"DONE: {done} images in {elapsed:.1f}s ({done/elapsed:.0f} img/s)")
    return done


def emit_progress(stage, completed, total, message):
    line = json.dumps({"stage": stage, "completed": completed, "total": total, "message": message})
    print(f"##PROGRESS##{line}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-dir", default="/tmp/clothing_images")
    parser.add_argument("--model-path", default="/data/imgsrch/models/ViT-L-14.pt")
    parser.add_argument("--num-gpus", type=int, default=None)
    parser.add_argument("--output-dir", default=OUTPUT_DIR)
    args = parser.parse_args()

    num_gpus = args.num_gpus or torch.cuda.device_count()
    num_gpus = min(num_gpus, torch.cuda.device_count())
    log.info(f"Available GPUs: {torch.cuda.device_count()}, using: {num_gpus}")

    # 收集图片
    exts = {".jpg", ".jpeg", ".png", ".webp"}
    image_paths = sorted([
        str(p) for p in Path(args.image_dir).iterdir()
        if p.suffix.lower() in exts and p.stat().st_size > 100
    ])
    log.info(f"Found {len(image_paths)} images in {args.image_dir}")
    if not image_paths:
        log.error("No images found!")
        return

    # 均分
    chunk_size = (len(image_paths) + num_gpus - 1) // num_gpus
    chunks = [image_paths[i:i + chunk_size] for i in range(0, len(image_paths), chunk_size)]
    log.info(f"Split into {len(chunks)} chunks of ~{chunk_size} images each")

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(PROGRESS_DIR, exist_ok=True)

    output_files = [f"{args.output_dir}/gpu_{i}.jsonl" for i in range(len(chunks))]
    progress_files = [f"{PROGRESS_DIR}/gpu_{i}.json" for i in range(len(chunks))]

    # 清空旧进度
    for pf in progress_files:
        try:
            os.unlink(pf)
        except FileNotFoundError:
            pass

    total_images = len(image_paths)
    emit_progress("extract", 0, total_images, f"Starting {num_gpus} GPU workers...")

    # 启动进程
    t0 = time.time()
    ctx = mp.get_context("spawn")
    processes = []
    for i in range(len(chunks)):
        p = ctx.Process(target=gpu_worker,
                        args=(i, chunks[i], args.model_path, output_files[i], progress_files[i]))
        p.start()
        processes.append(p)
        log.info(f"Started GPU{i} worker (PID={p.pid}), {len(chunks[i])} images")

    # 等待完成，同时汇报进度
    while any(p.is_alive() for p in processes):
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
        elapsed = time.time() - t0
        eta = (total_images - total_done) / agg_rate if agg_rate > 0 else 0
        eta_min = int(eta / 60)
        per_gpu = " | ".join(f"GPU{i}:{r:.0f}" for i, r in enumerate(rates))
        log.info(f"[ALL] {total_done:,}/{total_images:,} ({agg_rate:.0f} img/s, "
                 f"ETA {eta_min}min) [{per_gpu}]")
        emit_progress("extract", total_done, total_images,
                      f"{num_gpus} GPUs, {agg_rate:.0f} img/s, ETA {eta_min}min")

    # 等进程退出
    for p in processes:
        p.join()

    elapsed = time.time() - t0
    log.info(f"All workers done in {elapsed:.1f}s")

    # 合并结果
    total_records = 0
    merged_file = f"{args.output_dir}/merged.jsonl"
    with open(merged_file, "w") as mf:
        for of in output_files:
            if os.path.exists(of):
                with open(of) as f:
                    for line in f:
                        mf.write(line)
                        total_records += 1

    log.info(f"Merged {total_records} records → {merged_file}")
    emit_progress("extract", total_records, total_records,
                  f"Done: {total_records} images, {elapsed:.1f}s, {total_records/elapsed:.0f} img/s")
    log.info(f"Total: {total_records} images in {elapsed:.1f}s "
             f"({total_records/elapsed:.0f} img/s across {num_gpus} GPUs)")


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()
