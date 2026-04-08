#!/bin/bash
# 索引巡检脚本: 监控 Milvus 状态, 自动重启 + 补建缺失索引
# 2026-04-05

LOG="/data/imgsrch/task_logs/patrol_log.jsonl"
TARGET_INDEXES=11
CHECK_INTERVAL=120  # 2 minutes

log() {
    echo "{\"time\": \"$(date '+%Y-%m-%d %H:%M')\", $1}" >> "$LOG"
    echo "[$(date '+%H:%M:%S')] $1"
}

check_and_submit_indexes() {
    python3 -c "
from pymilvus import connections, Collection
import time, json, sys

try:
    connections.connect('default', host='localhost', port=19530, timeout=15)
except:
    print('CONNECT_FAIL')
    sys.exit(1)

coll = Collection('global_images_hot')
existing = {idx.field_name for idx in coll.indexes}
count = len(existing)
print(f'INDEX_COUNT={count}')

if count >= 11:
    print('ALL_DONE')
    sys.exit(0)

scalar_fields = ['category_l1','category_l2','category_l3','color_code',
                 'material_code','style_code','season_code','is_evergreen','tags','ts_month']
submitted = 0
for field in scalar_fields:
    if field not in existing:
        try:
            coll.create_index(field, {'index_type': 'INVERTED'}, _async=True)
            submitted += 1
            print(f'SUBMITTED: {field}')
        except Exception as e:
            print(f'FAILED: {field} ({e})')
if submitted > 0:
    print(f'SUBMITTED_COUNT={submitted}')
" 2>&1
}

log "\"event\": \"patrol_started\", \"target\": $TARGET_INDEXES"

while true; do
    STATUS=$(docker inspect imgsrch-milvus --format '{{.State.Status}}' 2>/dev/null)

    if [ "$STATUS" != "running" ]; then
        log "\"event\": \"milvus_down\", \"status\": \"$STATUS\""
        docker restart imgsrch-milvus
        log "\"event\": \"milvus_restarted\""
        sleep 60  # wait for Milvus to initialize
    fi

    RESULT=$(check_and_submit_indexes 2>&1)
    echo "$RESULT"

    if echo "$RESULT" | grep -q "ALL_DONE"; then
        COUNT=$(echo "$RESULT" | grep INDEX_COUNT | cut -d= -f2)
        log "\"event\": \"all_indexes_done\", \"count\": $COUNT"
        echo "=== ALL 11 INDEXES BUILT SUCCESSFULLY ==="
        exit 0
    fi

    if echo "$RESULT" | grep -q "CONNECT_FAIL"; then
        log "\"event\": \"connect_fail\", \"action\": \"restart\""
        docker restart imgsrch-milvus
        sleep 60
        continue
    fi

    COUNT=$(echo "$RESULT" | grep INDEX_COUNT | cut -d= -f2)
    SUBMITTED=$(echo "$RESULT" | grep SUBMITTED_COUNT | cut -d= -f2)
    log "\"event\": \"check\", \"indexes\": \"${COUNT:-?}/11\", \"submitted\": \"${SUBMITTED:-0}\""

    sleep $CHECK_INTERVAL
done
