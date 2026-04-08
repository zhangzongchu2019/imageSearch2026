#!/bin/bash
# SVIP 分区全流程自动化
# 2026-04-04
# 流程: 提取URL → 下载(COS缩图) → GPU推理 → 导出enriched → drop索引 → 衍生2000万 → 重建索引

set -eo pipefail

PARTITION="p_202604_svip"
TS_MONTH=202604
URL_FILE="/data/imgsrch/urls/urls_cos.txt"
URL_OFFSET=400000   # 跳过前40万(VIP已用)
URL_COUNT=500000    # 取50万
DERIVE_TARGET=20000000
MODEL_PATH="/data/imgsrch/models/ViT-L-14.pt"
DOWNLOAD_DIR="/tmp/clothing_images_svip"
ENRICHED_DIR="data/features_enriched_svip"
BACKUP_DIR="/data/imgsrch/backup_svip"
LOG_DIR="/data/imgsrch/task_logs"
TS=$(date +%Y%m%d_%H%M%S)

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') [SVIP] $*"; }

# 辅助: 更新监控页任务状态
task_update() { python3 scripts/task_helper.py "$1" "$2" "${3:-}" 2>/dev/null || true; }

# 辅助: 检查 Milvus 是否运行
check_milvus() {
    local status=$(docker inspect imgsrch-milvus --format '{{.State.Status}}' 2>/dev/null)
    if [ "$status" != "running" ]; then
        log "WARNING: Milvus is $status, restarting..."
        docker restart imgsrch-etcd 2>/dev/null; sleep 5
        docker restart imgsrch-milvus 2>/dev/null; sleep 30
        status=$(docker inspect imgsrch-milvus --format '{{.State.Status}}' 2>/dev/null)
        if [ "$status" != "running" ]; then
            log "ERROR: Milvus failed to restart!"
            return 1
        fi
        log "Milvus restarted OK"
    fi
}

# 辅助: 检查所有服务
check_services() {
    for svc in imgsrch-milvus imgsrch-etcd server-postgres-1 server-redis-1; do
        local s=$(docker inspect "$svc" --format '{{.State.Status}}' 2>/dev/null)
        if [ "$s" != "running" ]; then
            log "WARNING: $svc is $s, restarting..."
            docker restart "$svc" 2>/dev/null
            sleep 10
        fi
    done
}

mkdir -p "$LOG_DIR" "$BACKUP_DIR" "$DOWNLOAD_DIR"

log "========== SVIP 分区生成开始 =========="
log "Partition: $PARTITION, URLs: $URL_OFFSET+$URL_COUNT, Derive: $DERIVE_TARGET"

# ── Task 1: 提取 URL + 下载 ──
task_update 1 running
log "=== Task 1: 下载 ${URL_COUNT} 张图 ==="

URL_SUBSET="/tmp/urls_svip.txt"
sed -n "$((URL_OFFSET+1)),$((URL_OFFSET+URL_COUNT))p" "$URL_FILE" > "$URL_SUBSET"
ACTUAL=$(wc -l < "$URL_SUBSET")
log "提取 $ACTUAL 条 URL (offset=$URL_OFFSET)"

# 用 batch_import 下载 + GPU 推理 + 写入 Milvus/PG (一步完成)
# 但先只下载，之后单独推理
# ── Task 1+2: 下载 + GPU 推理 (batch_import 一步完成) ──
log "=== Task 1+2: 下载 + GPU推理 + 写入 $PARTITION ==="
check_services

# 创建 Milvus 分区
python3 -c "
from pymilvus import connections, Collection
connections.connect('default', host='localhost', port=19530, timeout=30)
coll = Collection('global_images_hot')
if not coll.has_partition('$PARTITION'):
    coll.create_partition('$PARTITION')
    print('Partition $PARTITION created')
else:
    print('Partition $PARTITION already exists')
"

python3 scripts/batch_import_clothing.py \
    --count "$URL_COUNT" \
    --url-file "$URL_SUBSET" \
    --skip-kafka \
    --device cuda \
    --model-path "$MODEL_PATH" \
    --download-dir "$DOWNLOAD_DIR" \
    --partition "$PARTITION" \
    --ts-month "$TS_MONTH" \
    2>&1 | tee "$LOG_DIR/svip_import_${TS}.log"

task_update 2 done
log "GPU 推理完成"

# ── 导出 enriched JSONL (备份) ──
log "=== 导出 enriched JSONL ==="
mkdir -p "$ENRICHED_DIR"
python3 << 'PYEOF'
import json, time, os
from pymilvus import connections, Collection

connections.connect("default", host="localhost", port=19530, timeout=30)
coll = Collection("global_images_hot")
coll.load()

output_dir = os.environ.get("ENRICHED_DIR", "data/features_enriched_svip")
partition = os.environ.get("PARTITION", "p_202604_svip")
ts_month = int(os.environ.get("TS_MONTH", "202604"))

os.makedirs(output_dir, exist_ok=True)
t0 = time.time()
total = 0; file_idx = 0; out_f = None; records_in_file = 0

iterator = coll.query_iterator(
    expr=f"ts_month == {ts_month}",
    output_fields=["image_pk", "global_vec", "category_l1", "tags"],
    batch_size=5000,
    partition_names=[partition],
)
while True:
    results = iterator.next()
    if not results: break
    if out_f is None or records_in_file >= 20000:
        if out_f: out_f.close()
        out_f = open(f"{output_dir}/batch_{file_idx:06d}.jsonl", "w")
        records_in_file = 0; file_idx += 1
    for r in results:
        record = {
            "image_pk": r["image_pk"],
            "global_vec": [float(x) for x in r["global_vec"]],
            "category_l1_pred": int(r.get("category_l1", 0)),
            "tags_pred": [int(t) for t in r.get("tags", [])],
        }
        out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
        records_in_file += 1; total += 1
    if total % 100000 == 0: print(f"  Exported {total:,}...")
if out_f: out_f.close()
iterator.close()
print(f"Exported {total:,} records in {time.time()-t0:.1f}s")
PYEOF

# ── Task 3: Drop 索引 → 衍生 2000 万 ──
task_update 3 running
log "=== Task 3: Drop 索引 + 衍生 ${DERIVE_TARGET} ==="
check_milvus

# Drop 索引
python3 -c "
from pymilvus import connections, Collection
connections.connect('default', host='localhost', port=19530, timeout=30)
coll = Collection('global_images_hot')
try: coll.release()
except: pass
import time; time.sleep(5)
for idx in list(coll.indexes):
    try:
        coll.drop_index(index_name=idx.index_name)
        print(f'  Dropped: {idx.field_name}')
    except Exception as e:
        print(f'  Failed: {idx.field_name}: {e}')
print(f'Indexes remaining: {len(coll.indexes)}')
"

# 衍生
BACKUP_DIR="$BACKUP_DIR" python3 scripts/derive_records.py \
    --target "$DERIVE_TARGET" \
    --enriched-dir "$ENRICHED_DIR" \
    --partition "$PARTITION" \
    --ts-month "$TS_MONTH" \
    2>&1 | tee "$LOG_DIR/svip_derive_${TS}.log"

task_update 3 done
log "衍生完成"

# ── Task 4: 重建索引 ──
task_update 4 running
log "=== Task 4: 重建索引 ==="
check_milvus

python3 -c "
from pymilvus import connections, Collection
connections.connect('default', host='localhost', port=19530, timeout=30)
coll = Collection('global_images_hot')
print('Building HNSW...')
coll.create_index('global_vec', {
    'index_type': 'HNSW', 'metric_type': 'COSINE',
    'params': {'M': 24, 'efConstruction': 200}
}, _async=True)
for field in ['category_l1','category_l2','category_l3','color_code',
              'material_code','style_code','season_code','is_evergreen','tags','ts_month']:
    try:
        coll.create_index(field, {'index_type': 'INVERTED'}, _async=True)
        print(f'  {field}: submitted')
    except Exception as e:
        print(f'  {field}: {e}')
print('All indexes submitted (building in background)')
"

# 等索引建完 (轮询)
log "等待索引建完..."
while true; do
    check_milvus || break
    count=$(python3 -c "
from pymilvus import connections, Collection
connections.connect('default', host='localhost', port=19530, timeout=15)
coll = Collection('global_images_hot')
print(len(coll.indexes))
" 2>/dev/null)
    log "  索引: ${count}/11"
    if [ "$count" = "11" ]; then
        break
    fi
    sleep 30
done

task_update 4 done
log "索引建完"

log "========== SVIP 分区生成完成 =========="
python3 -c "
from pymilvus import connections, Collection
connections.connect('default', host='localhost', port=19530, timeout=15)
coll = Collection('global_images_hot')
print(f'Total: {coll.num_entities:,}')
for p in coll.partitions:
    if p.num_entities > 0:
        print(f'  {p.name}: {p.num_entities:,}')
"
