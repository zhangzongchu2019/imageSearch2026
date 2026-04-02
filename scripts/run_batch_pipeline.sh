#!/usr/bin/env bash
# ============================================================
# 批量导入流水线 — 从 urls_cos.txt 逐批导入 1 亿条记录
#
# 每批流程:
#   1. 从 urls_cos.txt 提取 10 万 URL (腾讯云 COS 域名)
#   2. 下载 + CLIP 特征提取 + 零样本品类/标签
#   3. 衍生 ×10 → Milvus + PG
#   4. 清理临时图片
#
# Usage:
#   ./scripts/run_batch_pipeline.sh                    # 从断点继续
#   ./scripts/run_batch_pipeline.sh --start 4          # 从 batch 004 开始
#   ./scripts/run_batch_pipeline.sh --start 4 --end 20 # 只跑 batch 004~020
#   ./scripts/run_batch_pipeline.sh --dry-run          # 预览不执行
# ============================================================
set -euo pipefail

# ── 配置 ──
DATA_ROOT="/data/imgsrch"
URL_FILE="${DATA_ROOT}/urls/urls_cos.txt"
MODEL_PATH="${DATA_ROOT}/models/ViT-B-32.pt"
BATCH_SIZE=100000
TOTAL_URLS=10000000     # urls_cos.txt 有 1000 万 URL (无 header)
DERIVE_TARGET=1000000   # 每批衍生到 100 万

# 默认: 从 batch 004 开始 (002=batch2, 003=batch3 已完成)
START_BATCH=${START_BATCH:-4}
END_BATCH=${END_BATCH:-102}  # 100 批 (004~102, 含已完成的 002-003)
DRY_RUN=${DRY_RUN:-0}

# ── 参数解析 ──
while [[ $# -gt 0 ]]; do
    case "$1" in
        --start)  START_BATCH="$2"; shift 2 ;;
        --end)    END_BATCH="$2"; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

# ── 状态文件 ──
STATE_FILE="${DATA_ROOT}/task_logs/pipeline_state.json"

save_state() {
    local batch=$1 status=$2
    echo "{\"last_batch\": $batch, \"status\": \"$status\", \"timestamp\": \"$(date -Iseconds)\"}" > "$STATE_FILE"
}

load_state() {
    if [[ -f "$STATE_FILE" ]]; then
        python3 -c "import json; d=json.load(open('$STATE_FILE')); print(d.get('last_batch', 0))"
    else
        echo 0
    fi
}

# ── 自动恢复断点 ──
if [[ "$START_BATCH" -eq 4 ]]; then
    LAST=$(load_state)
    if [[ "$LAST" -gt 3 ]]; then
        START_BATCH=$((LAST + 1))
        echo "[RESUME] 从断点恢复: batch $(printf '%03d' $START_BATCH)"
    fi
fi

# ── 预览 ──
TOTAL_BATCHES=$(( (END_BATCH - START_BATCH + 1) ))
echo "============================================================"
echo "  批量导入流水线"
echo "  数据根目录: ${DATA_ROOT}"
echo "  URL 文件:   ${URL_FILE}"
echo "  批次范围:   $(printf '%03d' $START_BATCH) ~ $(printf '%03d' $END_BATCH) (${TOTAL_BATCHES} 批)"
echo "  每批:       ${BATCH_SIZE} URL → 下载 → CLIP → 衍生×10"
echo "============================================================"

if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[DRY-RUN] 预览模式, 不执行"
    for batch_num in $(seq $START_BATCH $END_BATCH); do
        BATCH_ID=$(printf '%03d' $batch_num)
        # CSV 行号: header(1行) + 已跳过行数
        LINE_START=$(( (batch_num - 1) * BATCH_SIZE + 1 ))  # 无 header, batch 001 从第1行开始
        LINE_END=$(( LINE_START + BATCH_SIZE - 1 ))
        echo "  batch_${BATCH_ID}: lines ${LINE_START}-${LINE_END}"
    done
    exit 0
fi

# ── 主循环 ──
PIPELINE_START=$(date +%s)

for batch_num in $(seq $START_BATCH $END_BATCH); do
    BATCH_ID=$(printf '%03d' $batch_num)
    BATCH_START=$(date +%s)

    echo ""
    echo "============================================================"
    echo "  BATCH ${BATCH_ID} / $(printf '%03d' $END_BATCH)  $(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================================"

    # ── 1. 提取 URL ──
    URL_BATCH_FILE="${DATA_ROOT}/urls/urls_batch${BATCH_ID}.txt"
    LINE_START=$(( (batch_num - 1) * BATCH_SIZE + 1 ))
    LINE_END=$(( LINE_START + BATCH_SIZE - 1 ))

    if [[ ! -f "$URL_BATCH_FILE" ]]; then
        echo "[${BATCH_ID}] Step 1: 提取 URL (lines ${LINE_START}-${LINE_END})"
        sed -n "${LINE_START},${LINE_END}p" "$URL_FILE" > "$URL_BATCH_FILE"
        ACTUAL=$(wc -l < "$URL_BATCH_FILE")
        echo "[${BATCH_ID}]   提取 ${ACTUAL} 条 URL"
        if [[ "$ACTUAL" -eq 0 ]]; then
            echo "[${BATCH_ID}] 无更多 URL, 退出"
            rm -f "$URL_BATCH_FILE"
            break
        fi
    else
        ACTUAL=$(wc -l < "$URL_BATCH_FILE")
        echo "[${BATCH_ID}] Step 1: URL 文件已存在 (${ACTUAL} 条)"
    fi

    # ── 2. 下载 + 特征提取 + 品类标签 ──
    DOWNLOAD_DIR="${DATA_ROOT}/downloads/batch_${BATCH_ID}"
    BACKUP_DIR="${DATA_ROOT}/backups/batch_${BATCH_ID}"
    FEATURE_DIR="${DATA_ROOT}/features/batch_${BATCH_ID}"

    if [[ ! -d "$BACKUP_DIR" ]] || [[ ! -f "$BACKUP_DIR/metadata.jsonl" ]]; then
        echo "[${BATCH_ID}] Step 2: 下载 + CLIP 特征提取 + 品类标签"
        python3 /workspace/scripts/batch_import_clothing.py \
            --count "$BATCH_SIZE" \
            --url-file "$URL_BATCH_FILE" \
            --download-dir "$DOWNLOAD_DIR" \
            --model-path "$MODEL_PATH" \
            --skip-kafka \
            --backup-dir "$BACKUP_DIR" \
            2>&1 | tee "${DATA_ROOT}/task_logs/batch_${BATCH_ID}_import.log"

        IMPORT_EXIT=$?
        if [[ $IMPORT_EXIT -ne 0 ]]; then
            echo "[${BATCH_ID}] ❌ batch_import 失败 (exit=$IMPORT_EXIT)"
            save_state $((batch_num - 1)) "failed_import"
            exit 1
        fi
    else
        echo "[${BATCH_ID}] Step 2: backup 已存在, 跳过导入"
    fi

    # ── 3. 衍生 ×10 ──
    echo "[${BATCH_ID}] Step 3: 衍生 ×10 → Milvus + PG"
    # enriched-dir 传空目录 (本批无 batch_*.jsonl), 源数据从 backup metadata.jsonl 加载
    mkdir -p "${DATA_ROOT}/features/batch_${BATCH_ID}"
    python3 /workspace/scripts/derive_records.py \
        --target "$DERIVE_TARGET" \
        --enriched-dir "${DATA_ROOT}/features/batch_${BATCH_ID}" \
        --extra-dir "$BACKUP_DIR" \
        2>&1 | tee "${DATA_ROOT}/task_logs/batch_${BATCH_ID}_derive.log"

    DERIVE_EXIT=$?
    if [[ $DERIVE_EXIT -ne 0 ]]; then
        echo "[${BATCH_ID}] ❌ derive 失败 (exit=$DERIVE_EXIT)"
        save_state $((batch_num - 1)) "failed_derive"
        exit 1
    fi

    # ── 4. 清理临时图片 ──
    if [[ -d "$DOWNLOAD_DIR" ]]; then
        IMG_SIZE=$(du -sh "$DOWNLOAD_DIR" 2>/dev/null | cut -f1)
        echo "[${BATCH_ID}] Step 4: 清理临时图片 (${IMG_SIZE})"
        rm -rf "$DOWNLOAD_DIR"
    fi

    # ── 批次完成 ──
    BATCH_END=$(date +%s)
    BATCH_ELAPSED=$(( BATCH_END - BATCH_START ))
    BATCH_MIN=$(( BATCH_ELAPSED / 60 ))

    save_state "$batch_num" "completed"

    # 当前 Milvus/PG 总量
    STATS=$(python3 -c "
from pymilvus import connections, Collection
import psycopg2
connections.connect('default', host='localhost', port=19530)
coll = Collection('global_images_hot')
coll.flush()
mv = coll.num_entities
conn = psycopg2.connect('postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search')
cur = conn.cursor()
cur.execute('SELECT count(*) FROM uri_dedup')
pg = cur.fetchone()[0]
cur.close(); conn.close()
print(f'Milvus={mv:,} PG={pg:,}')
" 2>/dev/null || echo "stats unavailable")

    PIPELINE_ELAPSED=$(( BATCH_END - PIPELINE_START ))
    DONE_BATCHES=$(( batch_num - START_BATCH + 1 ))
    REMAINING=$(( END_BATCH - batch_num ))
    ETA_SEC=$(( PIPELINE_ELAPSED / DONE_BATCHES * REMAINING ))
    ETA_HOURS=$(( ETA_SEC / 3600 ))
    ETA_MIN=$(( (ETA_SEC % 3600) / 60 ))

    echo ""
    echo "[${BATCH_ID}] ✅ 完成 (${BATCH_MIN}min) | ${STATS}"
    echo "[${BATCH_ID}]    进度: ${DONE_BATCHES}/${TOTAL_BATCHES} | 剩余: ${REMAINING} 批 | ETA: ${ETA_HOURS}h${ETA_MIN}m"
    echo ""
done

echo "============================================================"
echo "  全部批次完成!  $(date '+%Y-%m-%d %H:%M:%S')"
TOTAL_ELAPSED=$(( $(date +%s) - PIPELINE_START ))
echo "  总耗时: $(( TOTAL_ELAPSED / 3600 ))h$(( (TOTAL_ELAPSED % 3600) / 60 ))m"
echo "============================================================"
