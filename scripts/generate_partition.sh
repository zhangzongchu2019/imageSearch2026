#!/bin/bash
# 通用分区数据生成流程
# Usage: bash scripts/generate_partition.sh <partition_name> <url_offset> <url_count> <derive_target>
# Example: bash scripts/generate_partition.sh p_202604_svip 400000 500000 20000000

set -euo pipefail

PARTITION_NAME="${1:?Usage: $0 <partition_name> <url_offset> <url_count> <derive_target>}"
URL_OFFSET="${2:?}"
URL_COUNT="${3:?}"
DERIVE_TARGET="${4:?}"

TS=$(date +%Y%m%d_%H%M%S)
LOG_DIR="/data/imgsrch/task_logs"
BACKUP_DIR="/data/imgsrch/backup_${PARTITION_NAME}"
URL_FILE="/data/imgsrch/urls/urls_cos.txt"
MODEL_PATH="/data/imgsrch/models/ViT-L-14.pt"
DOWNLOAD_DIR="/tmp/clothing_images_${PARTITION_NAME}"
ENRICHED_DIR="data/features_enriched_${PARTITION_NAME}"

# 提取 ts_month 从分区名 (p_202604_svip → 202604)
TS_MONTH=$(echo "$PARTITION_NAME" | grep -oP '\d{6}')
if [ -z "$TS_MONTH" ]; then
    TS_MONTH=202604
fi

echo "=== $(date '+%Y-%m-%d %H:%M:%S') === 分区生成: $PARTITION_NAME ==="
echo "  URL: $URL_FILE offset=$URL_OFFSET count=$URL_COUNT"
echo "  衍生目标: $DERIVE_TARGET"
echo "  ts_month: $TS_MONTH"
echo "  备份: $BACKUP_DIR"

mkdir -p "$LOG_DIR" "$BACKUP_DIR" "$DOWNLOAD_DIR"

# Step 1: 提取 URL 子集
echo "=== Step 1: 提取 URL 子集 ==="
URL_SUBSET="/tmp/urls_${PARTITION_NAME}.txt"
sed -n "$((URL_OFFSET+1)),$((URL_OFFSET+URL_COUNT))p" "$URL_FILE" > "$URL_SUBSET"
ACTUAL_COUNT=$(wc -l < "$URL_SUBSET")
echo "  提取 $ACTUAL_COUNT 条 URL"

# Step 2: 修改 batch_import 参数并运行
echo "=== Step 2: 下载 + GPU 推理 ==="
python3 scripts/batch_import_clothing.py \
    --count "$URL_COUNT" \
    --url-file "$URL_SUBSET" \
    --skip-kafka \
    --device cuda \
    --model-path "$MODEL_PATH" \
    --download-dir "$DOWNLOAD_DIR" \
    2>&1 | tee "$LOG_DIR/import_${PARTITION_NAME}_${TS}.log"

echo "=== Step 3: 导出 Milvus 数据到 enriched JSONL ==="
# batch_import 写入了 p_202604 (硬编码), 需要从 Milvus 导出后写入正确分区
# 或者直接修改脚本支持 --partition 参数
# 暂时用 enriched 方式

echo "=== $(date '+%Y-%m-%d %H:%M:%S') === 分区 $PARTITION_NAME 生成完成 ==="
