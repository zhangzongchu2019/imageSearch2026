#!/bin/bash
# 第 2 批 99.9 万图, 4 模型向量提取
# 策略: 已有向量的 pk 跳过 (resume), 只处理新增图片
# Phase 1: SSCD (GPU 0-1) + DINOv2 (GPU 2-3) 并行
# Phase 2: CLIP (GPU 0-1) + SigLIP2 (GPU 2-3) 并行

set -e
LOG="/data/imgsrch/task_logs/batch2_4models.log"
IMAGE_DIR="/data/test-images"

echo "$(date '+%Y-%m-%d %H:%M:%S') === 第 2 批 4 模型向量提取 ===" | tee "$LOG"
echo "  图片目录: $IMAGE_DIR ($(ls $IMAGE_DIR | wc -l) 张)" | tee -a "$LOG"

# ===== Phase 1: SSCD + DINOv2 并行 =====
echo "" | tee -a "$LOG"
echo "$(date '+%Y-%m-%d %H:%M:%S') Phase 1: SSCD (GPU 0-1) + DINOv2 (GPU 2-3)" | tee -a "$LOG"

# SSCD (GPU 0-1, resume 跳过已有)
for gpu in 0 1; do
  out="/data/imgsrch/vectors_sscd/sscd_gpu${gpu}.jsonl"
  docker exec -d gpu_worker bash -c "CUDA_VISIBLE_DEVICES=$gpu python3 /workspace/scripts/infer_sscd_fast.py \
    --image-dir $IMAGE_DIR --output $out \
    --gpu $gpu --shard $gpu --total-shards 2 --batch-size 128 --workers 16 \
    > /tmp/b2_sscd_gpu${gpu}.log 2>&1"
  echo "  SSCD GPU $gpu started" | tee -a "$LOG"
done

# DINOv2 (GPU 2-3)
for gpu in 2 3; do
  shard=$((gpu - 2))
  out="/data/imgsrch/test_dinov2_b2/dinov2_gpu${shard}.jsonl"
  docker exec gpu_worker bash -c "mkdir -p /data/imgsrch/test_dinov2_b2 && : > $out"
  docker exec -d gpu_worker bash -c "CUDA_VISIBLE_DEVICES=$gpu python3 /workspace/scripts/infer_dinov2_fast.py \
    --image-dir $IMAGE_DIR --output $out \
    --gpu $gpu --shard $shard --total-shards 2 --batch-size 64 --workers 16 \
    > /tmp/b2_dinov2_gpu${shard}.log 2>&1"
  echo "  DINOv2 GPU $gpu (shard $shard) started" | tee -a "$LOG"
done

echo "$(date '+%Y-%m-%d %H:%M:%S') Phase 1 已启动, 等待完成..." | tee -a "$LOG"

# 监控 Phase 1
while true; do
  sleep 30
  sscd_total=0
  for gpu in 0 1; do
    cnt=$(docker exec gpu_worker bash -c "wc -l < /data/imgsrch/vectors_sscd/sscd_gpu${gpu}.jsonl 2>/dev/null" 2>/dev/null || echo 0)
    sscd_total=$((sscd_total + cnt))
  done
  dinov2_total=0
  for s in 0 1; do
    cnt=$(docker exec gpu_worker bash -c "wc -l < /data/imgsrch/test_dinov2_b2/dinov2_gpu${s}.jsonl 2>/dev/null" 2>/dev/null || echo 0)
    dinov2_total=$((dinov2_total + cnt))
  done
  echo "  SSCD: $sscd_total, DINOv2: $dinov2_total" | tee -a "$LOG"

  # 检查是否都完成 (进程不在了)
  sscd_alive=$(docker exec gpu_worker ps aux 2>/dev/null | grep infer_sscd | grep -v grep | wc -l)
  dinov2_alive=$(docker exec gpu_worker ps aux 2>/dev/null | grep infer_dinov2 | grep -v grep | wc -l)
  if [ "$sscd_alive" -eq 0 ] && [ "$dinov2_alive" -eq 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') Phase 1 完成!" | tee -a "$LOG"
    break
  fi
done

# ===== Phase 2: CLIP + SigLIP2 并行 =====
echo "" | tee -a "$LOG"
echo "$(date '+%Y-%m-%d %H:%M:%S') Phase 2: CLIP (GPU 0-1) + SigLIP2 (GPU 2-3)" | tee -a "$LOG"

# CLIP (GPU 0-1): 用 dual 脚本但只取 CLIP 部分
# 复用 infer_dual_fast.py, 它同时出 DINOv2+CLIP
# 但 DINOv2 已经跑了, 这里只需要 CLIP
# 简化: 直接用 dual 脚本, 输出包含 clip_vec
mkdir -p /data/imgsrch/test_h4_b2
for gpu in 0 1; do
  out="/data/imgsrch/test_h4_b2/dual_gpu${gpu}.jsonl"
  docker exec gpu_worker bash -c ": > $out"
  docker exec -d gpu_worker bash -c "CUDA_VISIBLE_DEVICES=$gpu python3 /workspace/scripts/infer_dual_fast.py \
    --image-dir $IMAGE_DIR --output $out \
    --gpu $gpu --shard $gpu --total-shards 2 --batch-size 64 --workers 16 \
    > /tmp/b2_dual_gpu${gpu}.log 2>&1"
  echo "  CLIP+DINOv2(dual) GPU $gpu started" | tee -a "$LOG"
done

# SigLIP2 (GPU 2-3)
mkdir -p /data/imgsrch/vectors_siglip2_b2
for gpu in 2 3; do
  shard=$((gpu - 2))
  out="/data/imgsrch/vectors_siglip2_b2/siglip2_gpu${shard}.jsonl"
  docker exec gpu_worker bash -c ": > $out"
  docker exec -d gpu_worker bash -c "CUDA_VISIBLE_DEVICES=$gpu python3 /workspace/scripts/infer_siglip2_fast.py \
    --image-dir $IMAGE_DIR --output $out \
    --gpu $gpu --shard $shard --total-shards 2 --batch-size 64 --workers 16 \
    > /tmp/b2_siglip2_gpu${shard}.log 2>&1"
  echo "  SigLIP2 GPU $gpu (shard $shard) started" | tee -a "$LOG"
done

echo "$(date '+%Y-%m-%d %H:%M:%S') Phase 2 已启动, 等待完成..." | tee -a "$LOG"

# 监控 Phase 2
while true; do
  sleep 30
  dual_total=0
  for gpu in 0 1; do
    cnt=$(docker exec gpu_worker bash -c "wc -l < /data/imgsrch/test_h4_b2/dual_gpu${gpu}.jsonl 2>/dev/null" 2>/dev/null || echo 0)
    dual_total=$((dual_total + cnt))
  done
  siglip_total=0
  for s in 0 1; do
    cnt=$(docker exec gpu_worker bash -c "wc -l < /data/imgsrch/vectors_siglip2_b2/siglip2_gpu${s}.jsonl 2>/dev/null" 2>/dev/null || echo 0)
    siglip_total=$((siglip_total + cnt))
  done
  echo "  CLIP(dual): $dual_total, SigLIP2: $siglip_total" | tee -a "$LOG"

  dual_alive=$(docker exec gpu_worker ps aux 2>/dev/null | grep infer_dual | grep -v grep | wc -l)
  siglip_alive=$(docker exec gpu_worker ps aux 2>/dev/null | grep infer_siglip2 | grep -v grep | wc -l)
  if [ "$dual_alive" -eq 0 ] && [ "$siglip_alive" -eq 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') Phase 2 完成!" | tee -a "$LOG"
    break
  fi
done

echo "" | tee -a "$LOG"
echo "$(date '+%Y-%m-%d %H:%M:%S') === 全部 4 模型向量提取完成 ===" | tee -a "$LOG"
