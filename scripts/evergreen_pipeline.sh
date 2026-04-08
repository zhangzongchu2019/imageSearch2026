#!/bin/bash
# Task 5: 常青分区 p_999999 完整 pipeline
# 25万真实图 → GPU推理 → 衍生到1000万 → 写入 Milvus + PG → 重建索引
# 2026-04-05

set -e
LOG_DIR="/data/imgsrch/task_logs"
TS=$(date +%Y%m%d_%H%M%S)
LOG="$LOG_DIR/evergreen_pipeline_${TS}.log"

PARTITION="p_999999"
TS_MONTH=999999
URL_FILE="/data/imgsrch/urls/urls_evergreen.txt"
COUNT=250000
TARGET=10000000
ENRICHED_DIR="data/features_enriched_evergreen"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG"
}

update_task() {
    python3 -c "
import json
with open('/data/imgsrch/task_logs/task_list.json', 'r') as f:
    tasks = json.load(f)
for t in tasks:
    if t['id'] == 5:
        t['status'] = '$1'
        t['eta'] = '$2'
with open('/data/imgsrch/task_logs/task_list.json', 'w') as f:
    json.dump(tasks, f, ensure_ascii=False, indent=2)
" 2>/dev/null
}

log "========== 常青分区 $PARTITION 开始 =========="
log "URL: $URL_FILE ($COUNT 条), Target: $TARGET, ts_month: $TS_MONTH"
update_task "running" "Step 1: GPU推理 25万图"

# ── Step 1: 在 gpu_worker 中下载+推理 25万图 ──
log "=== Step 1: 下载 + GPU 推理 $COUNT 张图 ==="

docker exec gpu_worker python3 /workspace/scripts/batch_import_clothing.py \
    --count $COUNT \
    --url-file "$URL_FILE" \
    --partition "$PARTITION" \
    --ts-month $TS_MONTH \
    --model-path /data/imgsrch/models/ViT-L-14.pt \
    --download-dir /tmp/evergreen_images \
    --skip-kafka \
    --device cuda \
    --evergreen \
    2>&1 | tee -a "$LOG"

log "Step 1 完成: 真实数据写入 $PARTITION"
update_task "running" "Step 1✅, Step 2: 导出特征"

# ── Step 2: 导出 enriched 特征 (供 derive 使用) ──
log "=== Step 2: 从 Milvus 导出已写入的 $PARTITION 数据 ==="

mkdir -p /workspace/$ENRICHED_DIR

python3 -c "
from pymilvus import connections, Collection
import json, os

connections.connect('default', host='localhost', port=19530, timeout=30)
coll = Collection('global_images_hot')

# Query all records from p_999999
coll.load(partition_names=['$PARTITION'])
import time; time.sleep(5)

# Get count
count = coll.query('image_pk != \"\"', partition_names=['$PARTITION'], output_fields=['count(*)'])
total = count[0]['count(*)']
print(f'Total records in $PARTITION: {total:,}')

# Export in batches using query with offset
batch_size = 20000
exported = 0
file_idx = 0
outdir = '/workspace/$ENRICHED_DIR'

while exported < total:
    results = coll.query(
        'image_pk != \"\"',
        partition_names=['$PARTITION'],
        output_fields=['image_pk', 'global_vec', 'category_l1', 'category_l2', 'category_l3', 'tags', 'product_id'],
        offset=exported,
        limit=batch_size,
    )
    if not results:
        break

    outfile = f'{outdir}/batch_{file_idx:06d}.jsonl'
    with open(outfile, 'w') as f:
        for r in results:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')

    exported += len(results)
    file_idx += 1
    print(f'Exported {exported:,}/{total:,}')

coll.release()
print(f'Export done: {exported:,} records, {file_idx} files')
" 2>&1 | tee -a "$LOG"

log "Step 2 完成: 导出 enriched 特征到 $ENRICHED_DIR"
update_task "running" "Step 2✅, Step 3: 衍生到 ${TARGET}"

# ── Step 3: 衍生到 1000万 ──
log "=== Step 3: 衍生到 $TARGET ==="

python3 scripts/derive_records.py \
    --target $TARGET \
    --enriched-dir "$ENRICHED_DIR" \
    --partition "$PARTITION" \
    --ts-month $TS_MONTH \
    --evergreen \
    2>&1 | tee -a "$LOG"

log "Step 3 完成: 衍生到 $TARGET"
update_task "running" "Step 3✅, Step 4: 重建索引"

# ── Step 4: 重建 11 个索引 ──
log "=== Step 4: 重建索引 ==="

python3 -c "
from pymilvus import connections, Collection
import time

connections.connect('default', host='localhost', port=19530, timeout=30)
coll = Collection('global_images_hot')

total = coll.num_entities
print(f'Total entities: {total:,}')

# HNSW
print('Building HNSW...')
coll.create_index('global_vec', {
    'index_type': 'HNSW', 'metric_type': 'COSINE',
    'params': {'M': 24, 'efConstruction': 200}
})
print('HNSW done')

# 标量索引
scalar_fields = ['category_l1','category_l2','category_l3','color_code',
                 'material_code','style_code','season_code','is_evergreen','tags','ts_month']
for field in scalar_fields:
    coll.create_index(field, {'index_type': 'INVERTED'}, _async=True)
    print(f'Submitted: {field}')
    time.sleep(1)

# 等待确认
time.sleep(10)
print(f'Indexes: {len(coll.indexes)}/11')
for idx in coll.indexes:
    print(f'  {idx.field_name}')
" 2>&1 | tee -a "$LOG"

log "Step 4 完成: 索引重建"
update_task "done" "✅ p_999999 1000万 + 索引11/11"

# ── 最终统计 ──
log "=== 最终统计 ==="
python3 -c "
from pymilvus import connections, Collection
connections.connect('default', host='localhost', port=19530, timeout=15)
coll = Collection('global_images_hot')
for p in coll.partitions:
    print(f'{p.name}: {p.num_entities:,}')
print(f'Total: {coll.num_entities:,}')
print(f'Indexes: {len(coll.indexes)}/11')
" 2>&1 | tee -a "$LOG"

log "========== 常青分区 $PARTITION 完成 =========="
