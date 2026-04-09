#!/bin/bash
# 4 GPU 并行 DINOv2 推理
set -e

IMAGE_DIR="${1:-/data/test-images}"
OUTPUT_DIR="${2:-/data/imgsrch/test_dinov2}"
LOG="/data/imgsrch/task_logs/dinov2_inference.log"

mkdir -p "$OUTPUT_DIR"
mkdir -p /data/imgsrch/task_logs

# 通过 docker exec 在 gpu_worker 中创建文件 (避免 root 权限问题)
for gpu in 0 1 2 3; do
  out="$OUTPUT_DIR/dinov2_gpu${gpu}.jsonl"
  docker exec gpu_worker bash -c ": > $out"
done

echo "$(date '+%Y-%m-%d %H:%M:%S') 启动 4 GPU DINOv2 推理" | tee "$LOG"
echo "  IMAGE_DIR: $IMAGE_DIR" | tee -a "$LOG"
echo "  OUTPUT_DIR: $OUTPUT_DIR" | tee -a "$LOG"

# 启动 4 个 docker exec 进程
for gpu in 0 1 2 3; do
  out="$OUTPUT_DIR/dinov2_gpu${gpu}.jsonl"
  cmd="CUDA_VISIBLE_DEVICES=$gpu python3 /workspace/scripts/infer_dinov2_fast.py \
    --image-dir $IMAGE_DIR \
    --output $out \
    --gpu $gpu --shard $gpu --total-shards 4 \
    --batch-size 64 --workers 16"
  docker exec -d gpu_worker bash -c "$cmd > /tmp/dinov2_gpu${gpu}.log 2>&1"
  echo "  GPU $gpu started" | tee -a "$LOG"
done

echo "$(date '+%Y-%m-%d %H:%M:%S') 全部 4 GPU 进程已启动" | tee -a "$LOG"
