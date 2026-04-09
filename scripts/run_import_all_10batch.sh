#!/bin/bash
# 10 批流水线导入: ~955 万图片
# 流水线: Batch N 推理+写入 同时 Batch N+1 下载
set -euo pipefail

LOG=/data/imgsrch/task_logs
SCRIPT=/workspace/scripts/import_pipeline_v2.py
COLLECTION=img_202603_svip
TS_MONTH=202603

# 批次定义: 文件 → 下载目录
declare -A BATCHES
BATCHES[g1]="/data/imgsrch/urls/urls_new_batch1.txt|/tmp/import_batch1"
BATCHES[g2]="/data/imgsrch/urls/urls_new_batch2.txt|/tmp/import_batch2"
BATCHES[g3]="/data/imgsrch/urls/urls_new_batch3.txt|/tmp/import_batch3"
BATCHES[g4]="/data/imgsrch/urls/urls_batch_g4.txt|/tmp/import_g4"
BATCHES[g5]="/data/imgsrch/urls/urls_batch_g5.txt|/tmp/import_g5"
BATCHES[g6]="/data/imgsrch/urls/urls_batch_g6.txt|/tmp/import_g6"
BATCHES[g7]="/data/imgsrch/urls/urls_batch_g7.txt|/tmp/import_g7"
BATCHES[g8]="/data/imgsrch/urls/urls_batch_g8.txt|/tmp/import_g8"
BATCHES[g9]="/data/imgsrch/urls/urls_batch_g9.txt|/tmp/import_g9"
BATCHES[g10]="/data/imgsrch/urls/urls_batch_g10.txt|/tmp/import_g10"

ORDER=(g1 g2 g3 g4 g5 g6 g7 g8 g9 g10)
TOTAL=${#ORDER[@]}

echo "$(date '+%Y-%m-%d %H:%M:%S') === ${TOTAL} 批流水线导入 (4GPU 直接推理) ==="

run_batch() {
    local batch_id=$1
    local skip_dl=$2
    local url_file=$(echo ${BATCHES[$batch_id]} | cut -d'|' -f1)
    local dl_dir=$(echo ${BATCHES[$batch_id]} | cut -d'|' -f2)
    local infer_dir="/data/imgsrch/infer_${batch_id}"
    local log_file="$LOG/pipeline_v2_${batch_id}.log"
    local extra_args=""
    if [ "$skip_dl" = "true" ]; then
        extra_args="--skip-download"
    fi
    PYTHONUNBUFFERED=1 python3 $SCRIPT \
        --url-file "$url_file" \
        --collection $COLLECTION --ts-month $TS_MONTH \
        --download-dir "$dl_dir" --infer-dir "$infer_dir" \
        --batch-id "$batch_id" $extra_args \
        > "$log_file" 2>&1
}

# g1: 下载已完成 (import_pipeline_b1 跑的), 直接推理+写入
# 同时启动 g2 下载
echo "$(date '+%Y-%m-%d %H:%M:%S') g1: 等下载完成 → 推理+写入..."
# 等 g1 下载完
while ! grep -q "\[下载完成\]\|\[推理\]" $LOG/import_pipeline_b1.log 2>/dev/null; do
    sleep 10
done
pkill -f "import_pipeline.py.*batch-id 1" 2>/dev/null || true
sleep 3

# g1 推理+写入 (后台) + g2 全流程 (后台, 含下载)
echo "$(date '+%Y-%m-%d %H:%M:%S') g1: 推理+写入 | g2: 下载开始"
run_batch g1 true &
PID_PREV=$!

for i in $(seq 1 $((TOTAL-1))); do
    CURR=${ORDER[$i]}
    NEXT=""
    if [ $i -lt $((TOTAL-1)) ]; then
        NEXT=${ORDER[$((i+1))]}
    fi

    # 等前一批完成
    wait $PID_PREV 2>/dev/null
    echo "$(date '+%Y-%m-%d %H:%M:%S') ${ORDER[$((i-1))]} 完成!"

    # 当前批推理+写入 (如果下载已完成)
    # 如果是 g2, 它整个流程已在后台跑
    if [ $i -eq 1 ]; then
        # g2 全流程已启动, 只需启动 g3
        run_batch $CURR false &
        PID_PREV=$!
        if [ -n "$NEXT" ]; then
            echo "$(date '+%Y-%m-%d %H:%M:%S') $NEXT: 预下载..."
            # 预下载下一批 (只下载不推理, 但 v2 脚本不支持只下载)
            # 让下一批全流程跑就行, 下载阶段自然并行
        fi
    else
        run_batch $CURR false &
        PID_PREV=$!
    fi

    echo "$(date '+%Y-%m-%d %H:%M:%S') $CURR 启动 (PID: $PID_PREV)"
done

wait $PID_PREV 2>/dev/null
echo "$(date '+%Y-%m-%d %H:%M:%S') ${ORDER[$((TOTAL-1))]} 完成!"

echo ""
echo "$(date '+%Y-%m-%d %H:%M:%S') === 全部 $TOTAL 批完成 ==="
for b in ${ORDER[@]}; do
    echo "--- $b ---"
    tail -2 $LOG/pipeline_v2_${b}.log 2>/dev/null || echo "(无日志)"
done
