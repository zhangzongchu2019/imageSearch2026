#!/bin/bash
# 4 GPU 并行 H4 双模型推理 (DINOv2 + CLIP)
set -e

IMAGE_DIR="${1:-/data/test-images}"
OUTPUT_DIR="${2:-/data/imgsrch/test_h4}"
LOG="/data/imgsrch/task_logs/dual_inference.log"

mkdir -p /data/imgsrch/task_logs

# 在 gpu_worker 中创建目录和清空文件
docker exec gpu_worker bash -c "mkdir -p $OUTPUT_DIR"
for gpu in 0 1 2 3; do
  out="$OUTPUT_DIR/dual_gpu${gpu}.jsonl"
  docker exec gpu_worker bash -c ": > $out"
done

echo "$(date '+%Y-%m-%d %H:%M:%S') 启动 4 GPU H4 双模型推理" | tee "$LOG"
echo "  IMAGE_DIR: $IMAGE_DIR" | tee -a "$LOG"
echo "  OUTPUT_DIR: $OUTPUT_DIR" | tee -a "$LOG"

# 启动 4 个进程
for gpu in 0 1 2 3; do
  out="$OUTPUT_DIR/dual_gpu${gpu}.jsonl"
  cmd="CUDA_VISIBLE_DEVICES=$gpu python3 /workspace/scripts/infer_dual_fast.py \
    --image-dir $IMAGE_DIR \
    --output $out \
    --gpu $gpu --shard $gpu --total-shards 4 \
    --batch-size 64 --workers 16"
  docker exec -d gpu_worker bash -c "$cmd > /tmp/dual_gpu${gpu}.log 2>&1"
  echo "  GPU $gpu started" | tee -a "$LOG"
done

echo "$(date '+%Y-%m-%d %H:%M:%S') 全部 4 GPU 进程已启动" | tee -a "$LOG"
