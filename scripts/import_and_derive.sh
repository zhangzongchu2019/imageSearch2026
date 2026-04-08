#!/bin/bash
# 完整导入流程: 真实数据 → 衍生 → 建索引
# 2026-04-03

set -e
LOG_DIR="/data/imgsrch/task_logs"
BACKUP_DIR="/data/imgsrch/backup_20260403"
ENRICHED_DIR="data/features_enriched_v2"
TS=$(date +%Y%m%d_%H%M%S)

echo "=== $(date '+%Y-%m-%d %H:%M:%S') === 开始完整导入流程 ==="

# Step 1: 导入 40万真实数据到 Milvus (从 enriched JSONL)
echo "=== Step 1: 导入真实数据 ==="
python3 scripts/import_from_enriched.py \
    --enriched-dir "$ENRICHED_DIR" \
    > "$LOG_DIR/import_real_${TS}.log" 2>&1
echo "真实数据导入完成"

# Step 2: 衍生到 2000万 (每批备份)
echo "=== Step 2: 衍生到 2000万 ==="
BACKUP_DIR="$BACKUP_DIR" python3 scripts/derive_records.py \
    --target 20000000 \
    --enriched-dir "$ENRICHED_DIR" \
    > "$LOG_DIR/derive_${TS}.log" 2>&1
echo "衍生完成"

# Step 3: 建索引
echo "=== Step 3: 建索引 ==="
python3 -c "
from pymilvus import connections, Collection
import time

connections.connect('default', host='localhost', port=19530, timeout=30)
coll = Collection('global_images_hot')
print(f'Total entities: {coll.num_entities:,}')

print('Building HNSW index...')
coll.create_index('global_vec', {
    'index_type': 'HNSW', 'metric_type': 'COSINE',
    'params': {'M': 24, 'efConstruction': 200},
})
print('HNSW index building in background')

for field in ['category_l1','category_l2','category_l3','color_code',
              'material_code','style_code','season_code','is_evergreen','tags','ts_month']:
    try:
        coll.create_index(field, {'index_type': 'INVERTED'})
    except Exception as e:
        print(f'  {field}: {e}')

print('All indexes initiated')
# 等索引建完
print('Waiting for index build...')
coll.load()
print('Collection loaded and ready')
"
echo "=== $(date '+%Y-%m-%d %H:%M:%S') === 完成 ==="
