#!/bin/bash
set -e
LOG=/data/imgsrch/task_logs
SCRIPT=/workspace/scripts/import_pipeline_v2.py

echo "$(date '+%Y-%m-%d %H:%M:%S') === 3×100万 流水线 v2 (4GPU 直接推理) ==="

# Batch 1: 下载在跑 (import_pipeline_b1), 等它下载完
echo "等待 Batch 1 下载完成..."
while true; do
    if grep -q "\[下载完成\]" $LOG/import_pipeline_b1.log 2>/dev/null; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') Batch 1 下载完成!"
        break
    fi
    if grep -q "\[推理\]" $LOG/import_pipeline_b1.log 2>/dev/null; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') Batch 1 进入了旧推理, 终止它"
        pkill -f "import_pipeline.py.*batch-id 1" 2>/dev/null || true
        break
    fi
    sleep 15
done

# 停旧 Batch 1 进程 (如果还在)
pkill -f "import_pipeline.py.*batch-id 1" 2>/dev/null || true
sleep 3

# Batch 1: 4GPU 直接推理 + 写入 (后台)
echo "$(date '+%Y-%m-%d %H:%M:%S') Batch 1: 启动 4GPU 推理+写入..."
PYTHONUNBUFFERED=1 python3 $SCRIPT \
    --url-file /data/imgsrch/urls/urls_new_batch1.txt \
    --collection img_202603_svip --ts-month 202603 \
    --download-dir /tmp/import_batch1 \
    --infer-dir /data/imgsrch/infer_b1 \
    --batch-id 1 --skip-download \
    > $LOG/pipeline_v2_b1.log 2>&1 &
B1_PID=$!
echo "  PID: $B1_PID"

# 同时 Batch 2: 下载 (后台)
echo "$(date '+%Y-%m-%d %H:%M:%S') Batch 2: 启动下载..."
PYTHONUNBUFFERED=1 python3 $SCRIPT \
    --url-file /data/imgsrch/urls/urls_new_batch2.txt \
    --collection img_202603_svip --ts-month 202603 \
    --download-dir /tmp/import_batch2 \
    --infer-dir /data/imgsrch/infer_b2 \
    --batch-id 2 \
    > $LOG/pipeline_v2_b2.log 2>&1 &
B2_PID=$!
echo "  PID: $B2_PID"

# 等 Batch 1 推理+写入完成
echo "等待 Batch 1 推理+写入..."
wait $B1_PID
echo "$(date '+%Y-%m-%d %H:%M:%S') Batch 1 完成!"

# Batch 2 可能还在推理, 等它
# 同时启动 Batch 3 下载
echo "$(date '+%Y-%m-%d %H:%M:%S') Batch 3: 启动..."
PYTHONUNBUFFERED=1 python3 $SCRIPT \
    --url-file /data/imgsrch/urls/urls_new_batch3.txt \
    --collection img_202603_svip --ts-month 202603 \
    --download-dir /tmp/import_batch3 \
    --infer-dir /data/imgsrch/infer_b3 \
    --batch-id 3 \
    > $LOG/pipeline_v2_b3.log 2>&1 &
B3_PID=$!

wait $B2_PID
echo "$(date '+%Y-%m-%d %H:%M:%S') Batch 2 完成!"
wait $B3_PID
echo "$(date '+%Y-%m-%d %H:%M:%S') Batch 3 完成!"

echo "$(date '+%Y-%m-%d %H:%M:%S') === 全部 3 批完成 ==="
for i in 1 2 3; do
    echo "--- Batch $i ---"
    tail -3 $LOG/pipeline_v2_b${i}.log
done
