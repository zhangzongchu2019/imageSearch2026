#!/bin/bash
# 3批100万图片流水线导入
# Batch 1 下载完 → 启动 Batch 1 推理+写入 & Batch 2 下载
# Batch 2 下载完 → 启动 Batch 2 推理+写入 & Batch 3 下载
# Batch 3 下载完 → Batch 3 推理+写入

set -e
SCRIPT=/workspace/scripts/import_pipeline.py
LOG_DIR=/data/imgsrch/task_logs
COLLECTION=img_202603_svip
TS_MONTH=202603

echo "$(date '+%Y-%m-%d %H:%M:%S') === 3×100万图片流水线导入 ==="

# Batch 1 已经在跑, 等它下载完成
echo "等待 Batch 1 下载完成..."
while true; do
    if grep -q "\[下载完成\]" $LOG_DIR/import_pipeline_b1.log 2>/dev/null; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') Batch 1 下载完成!"
        break
    fi
    if grep -q "\[推理\]" $LOG_DIR/import_pipeline_b1.log 2>/dev/null; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') Batch 1 已进入推理阶段"
        break
    fi
    sleep 30
done

# 启动 Batch 2 下载 (和 Batch 1 推理并行)
echo "$(date '+%Y-%m-%d %H:%M:%S') 启动 Batch 2 下载..."
PYTHONUNBUFFERED=1 python3 $SCRIPT \
    --url-file /data/imgsrch/urls/urls_new_batch2.txt \
    --collection $COLLECTION --ts-month $TS_MONTH \
    --download-dir /tmp/import_batch2 --batch-id 2 \
    > $LOG_DIR/import_pipeline_b2.log 2>&1 &
B2_PID=$!
echo "Batch 2 PID: $B2_PID"

# 等 Batch 2 下载完成
echo "等待 Batch 2 下载完成..."
while true; do
    if grep -q "\[推理\]" $LOG_DIR/import_pipeline_b2.log 2>/dev/null; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') Batch 2 进入推理阶段"
        break
    fi
    if ! kill -0 $B2_PID 2>/dev/null; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') Batch 2 进程结束"
        break
    fi
    sleep 30
done

# 启动 Batch 3 (和 Batch 2 推理并行)
echo "$(date '+%Y-%m-%d %H:%M:%S') 启动 Batch 3..."
PYTHONUNBUFFERED=1 python3 $SCRIPT \
    --url-file /data/imgsrch/urls/urls_new_batch3.txt \
    --collection $COLLECTION --ts-month $TS_MONTH \
    --download-dir /tmp/import_batch3 --batch-id 3 \
    > $LOG_DIR/import_pipeline_b3.log 2>&1 &
B3_PID=$!
echo "Batch 3 PID: $B3_PID"

# 等全部完成
echo "等待所有批次完成..."
wait $B2_PID 2>/dev/null
wait $B3_PID 2>/dev/null

echo "$(date '+%Y-%m-%d %H:%M:%S') === 全部 3 批完成 ==="
for i in 1 2 3; do
    echo "--- Batch $i ---"
    tail -5 $LOG_DIR/import_pipeline_b${i}.log
done
