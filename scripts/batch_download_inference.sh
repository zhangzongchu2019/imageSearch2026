#!/bin/bash
# 批量下载+推理: 常青 + 冷区VIP + 冷区SVIP
# GPU 串行执行，不影响 Milvus 索引构建
# 2026-04-05

set -e
LOG_DIR="/data/imgsrch/task_logs"
TS=$(date +%Y%m%d_%H%M%S)

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_DIR/batch_dl_inf_${TS}.log"
}

update_task() {
    python3 -c "
import json
with open('/data/imgsrch/task_logs/task_list.json', 'r') as f:
    tasks = json.load(f)
for t in tasks:
    if t['id'] == $1:
        t['status'] = '$2'
        t['eta'] = '$3'
with open('/data/imgsrch/task_logs/task_list.json', 'w') as f:
    json.dump(tasks, f, ensure_ascii=False, indent=2)
" 2>/dev/null
}

log "========== 批量下载+推理 开始 =========="

# ── 1. 常青分区: 250K → img_999999 ──
log "=== [1/3] 常青 img_999999: 250K 图 ==="
update_task 6 "running" "GPU推理 250K图 (1/3)"

docker exec gpu_worker python3 /workspace/scripts/batch_import_clothing.py \
    --count 250000 \
    --url-file /data/imgsrch/urls/urls_evergreen.txt \
    --collection img_999999 \
    --partition _default \
    --ts-month 999999 \
    --model-path /data/imgsrch/models/ViT-L-14.pt \
    --download-dir /tmp/evergreen_images \
    --skip-kafka \
    --device cuda \
    --evergreen \
    2>&1 | tee -a "$LOG_DIR/batch_dl_inf_${TS}.log"

log "[1/3] 常青完成"
update_task 6 "done" "✅ 250K图推理完成"

# ── 2. 冷区VIP: 400K → img_202603_vip ──
log "=== [2/3] 冷区VIP img_202603_vip: 400K 图 ==="
update_task 7 "running" "GPU推理 400K图 (2/3)"

docker exec gpu_worker python3 /workspace/scripts/batch_import_clothing.py \
    --count 400000 \
    --url-file /data/imgsrch/urls/urls_cold_vip.txt \
    --collection img_202603_vip \
    --partition _default \
    --ts-month 202603 \
    --model-path /data/imgsrch/models/ViT-L-14.pt \
    --download-dir /tmp/cold_vip_images \
    --skip-kafka \
    --device cuda \
    2>&1 | tee -a "$LOG_DIR/batch_dl_inf_${TS}.log"

log "[2/3] 冷区VIP完成"
update_task 7 "running" "VIP✅ 400K, SVIP推理中..."

# ── 3. 冷区SVIP: 500K → img_202603_svip ──
log "=== [3/3] 冷区SVIP img_202603_svip: 500K 图 ==="

docker exec gpu_worker python3 /workspace/scripts/batch_import_clothing.py \
    --count 500000 \
    --url-file /data/imgsrch/urls/urls_cold_svip.txt \
    --collection img_202603_svip \
    --partition _default \
    --ts-month 202603 \
    --model-path /data/imgsrch/models/ViT-L-14.pt \
    --download-dir /tmp/cold_svip_images \
    --skip-kafka \
    --device cuda \
    2>&1 | tee -a "$LOG_DIR/batch_dl_inf_${TS}.log"

log "[3/3] 冷区SVIP完成"
update_task 7 "done" "✅ VIP 400K + SVIP 500K 推理完成"

# ── 最终统计 ──
log "=== 最终统计 ==="
python3 -c "
from pymilvus import connections, Collection, utility
connections.connect('default', host='localhost', port=19530, timeout=15)
for name in ['img_999999', 'img_202603_vip', 'img_202603_svip']:
    if utility.has_collection(name):
        c = Collection(name)
        print(f'{name}: {c.num_entities:,}')
" 2>&1 | tee -a "$LOG_DIR/batch_dl_inf_${TS}.log"

log "========== 批量下载+推理 全部完成 =========="
