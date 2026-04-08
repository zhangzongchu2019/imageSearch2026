#!/usr/bin/env python3
"""
主动监控循环: 每 5 秒检查所有服务状态, 更新监控页数据, 处理异常。
"""
import json
import os
import socket
import subprocess
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

TASK_LIST_FILE = "/data/imgsrch/task_logs/task_list.json"
PATROL_LOG = "/data/imgsrch/task_logs/patrol_log.jsonl"
INTERVAL = 5
MAX_ROUNDS = 720  # 720 * 5s = 60 min max

# 上次报告的状态 (用于检测变化)
last_index_count = -1
last_bitmap_count = -1
last_milvus_status = None


def check_container(name):
    try:
        r = subprocess.run(["docker", "inspect", name, "--format", "{{.State.Status}}"],
                           capture_output=True, text=True, timeout=5)
        return r.stdout.strip()
    except:
        return "unknown"


def check_port(port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        ok = s.connect_ex(("localhost", port)) == 0
        s.close()
        return ok
    except:
        return False


def check_milvus_indexes():
    try:
        r = subprocess.run(
            ["python3", "-c", """
from pymilvus import connections, Collection
connections.connect('default', host='localhost', port=19530, timeout=10)
coll = Collection('global_images_hot')
idxs = coll.indexes
print(len(idxs))
for i in idxs:
    print(i.field_name)
"""],
            capture_output=True, text=True, timeout=20)
        lines = r.stdout.strip().split("\n")
        count = int(lines[0])
        fields = lines[1:] if len(lines) > 1 else []
        return count, fields
    except:
        return -1, []


def check_bitmap_count():
    try:
        r = subprocess.run(
            ["docker", "exec", "server-postgres-1", "psql", "-U", "imgsrch",
             "-d", "image_search", "-t", "-c", "SELECT count(*) FROM image_merchant_bitmaps;"],
            capture_output=True, text=True, timeout=15)
        return int(r.stdout.strip())
    except:
        return -1


def check_etcd_cluster():
    try:
        r = subprocess.run(
            ["docker", "exec", "etcd-node-1", "etcdctl", "endpoint", "health",
             "--endpoints=etcd-node-1:2379,etcd-node-2:2379,etcd-node-3:2379"],
            capture_output=True, text=True, timeout=10)
        healthy = r.stdout.count("is healthy")
        return healthy
    except:
        return 0


def restart_milvus():
    log.warning("⚠️  Milvus 不健康, 执行 restart...")
    subprocess.run(["docker", "restart", "imgsrch-milvus"], timeout=30)
    time.sleep(30)
    # 重新提交缺失索引
    subprocess.run(["python3", "-c", """
from pymilvus import connections, Collection
connections.connect('default', host='localhost', port=19530, timeout=30)
coll = Collection('global_images_hot')
existing = {idx.field_name for idx in coll.indexes}
for field in ['category_l1','category_l2','category_l3','color_code',
              'material_code','style_code','season_code','is_evergreen','tags','ts_month']:
    if field not in existing:
        coll.create_index(field, {'index_type': 'INVERTED'}, _async=True)
print(f'Re-submitted missing indexes. Current: {len(existing)}/11')
"""], capture_output=True, text=True, timeout=60)
    log.info("Milvus 重启 + 索引补提交完成")


def update_task_list(index_count, bitmap_count, bitmap_target):
    try:
        with open(TASK_LIST_FILE, "r") as f:
            tasks = json.load(f)
        for t in tasks:
            if t["id"] == 4:
                if index_count >= 11:
                    t["status"] = "done"
                    t["eta"] = f"✅ 全部 {index_count}/11 索引已建完"
                else:
                    t["status"] = "running"
                    t["eta"] = f"HNSW✅, 标量索引 {index_count}/11 构建中"
            elif t["id"] == 7:
                if bitmap_count >= bitmap_target - 1000:
                    t["status"] = "done"
                    t["eta"] = f"✅ bitmap {bitmap_count:,} 条"
                else:
                    t["status"] = "running"
                    pct = bitmap_count * 100 / bitmap_target if bitmap_target > 0 else 0
                    t["eta"] = f"bitmap 补建 {bitmap_count:,}/{bitmap_target:,} ({pct:.1f}%)"
        with open(TASK_LIST_FILE, "w") as f:
            json.dump(tasks, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"更新 task_list.json 失败: {e}")


def main():
    global last_index_count, last_bitmap_count, last_milvus_status

    bitmap_target = 39499401  # should match uri_dedup count
    log.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 主动监控启动, 间隔 {INTERVAL}s")

    # 每 5 秒一轮快检, 每 30 秒一轮全检 (含 bitmap count 等慢查询)
    round_num = 0
    for _ in range(MAX_ROUNDS):
        round_num += 1
        ts = time.strftime("%H:%M:%S")
        full_check = (round_num % 6 == 1)  # 每 30s 一次全检

        # === 快检: 容器 + 端口 ===
        milvus_status = check_container("imgsrch-milvus")
        milvus_port = check_port(19530)

        # Milvus 异常处理
        if milvus_status != "running" or (not milvus_port and milvus_status == "running"):
            if last_milvus_status == "running":
                log.warning(f"[{ts}] 🚨 Milvus 状态变化: {last_milvus_status} → {milvus_status}, port={milvus_port}")
                restart_milvus()
                milvus_status = check_container("imgsrch-milvus")
        last_milvus_status = milvus_status

        if not full_check:
            time.sleep(INTERVAL)
            continue

        # === 全检 ===
        # etcd 集群
        etcd_healthy = check_etcd_cluster()
        if etcd_healthy < 3:
            log.warning(f"[{ts}] ⚠️  etcd 集群: {etcd_healthy}/3 healthy")

        # Milvus 索引
        index_count, index_fields = check_milvus_indexes()
        if index_count != last_index_count and index_count >= 0:
            log.info(f"[{ts}] 📊 Milvus 索引: {index_count}/11 {index_fields}")
            last_index_count = index_count
            if index_count >= 11:
                log.info(f"[{ts}] 🎉 全部 11 个索引构建完成!")

        # bitmap count
        bitmap_count = check_bitmap_count()
        if bitmap_count >= 0:
            delta = bitmap_count - last_bitmap_count if last_bitmap_count >= 0 else 0
            if delta > 0 or last_bitmap_count < 0:
                log.info(f"[{ts}] 📊 bitmap: {bitmap_count:,}/{bitmap_target:,} "
                         f"({bitmap_count*100/bitmap_target:.1f}%) [+{delta:,} in 30s]")
            last_bitmap_count = bitmap_count

        # 其他关键容器
        for svc in ["server-postgres-1", "server-redis-1", "imgsrch-kafka"]:
            st = check_container(svc)
            if st != "running":
                log.warning(f"[{ts}] ⚠️  {svc}: {st}")

        # 更新监控页
        update_task_list(max(index_count, 1), bitmap_count, bitmap_target)

        # 检查双任务完成
        if index_count >= 11 and bitmap_count >= bitmap_target - 1000:
            log.info(f"[{ts}] ✅ Task 4 + Task 7 全部完成!")
            update_task_list(index_count, bitmap_count, bitmap_target)
            break

        time.sleep(INTERVAL)

    log.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 监控循环结束")


if __name__ == "__main__":
    main()
