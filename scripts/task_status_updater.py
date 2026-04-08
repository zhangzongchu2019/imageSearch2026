#!/usr/bin/env python3
"""
任务状态实时更新器: 每 10 秒刷新 task_list.json，
读取 Milvus 实时数据驱动任务状态显示。
"""
import json
import time
import logging
import subprocess

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

TASK_FILE = "/data/imgsrch/task_logs/task_list.json"
INTERVAL = 10
MAX_ROUNDS = 1000  # ~2.7h

# 目标数据量 (用于计算百分比)
TARGETS = {
    "img_202604_vip": 20_131_226,
    "img_202604_svip": 19_999_332,
}


def get_collection_info(name):
    """返回 (exists, num_entities, num_indexes, index_fields)"""
    try:
        from pymilvus import connections, Collection, utility
        try:
            connections.connect("default", host="localhost", port=19530, timeout=5)
        except:
            pass
        if not utility.has_collection(name):
            return False, 0, 0, []
        c = Collection(name)
        idx_fields = [i.field_name for i in c.indexes]
        return True, c.num_entities, len(c.indexes), idx_fields
    except Exception as e:
        return False, 0, 0, []


def get_migration_rate(logfile):
    """从迁移日志尾部读取最新速率"""
    try:
        r = subprocess.run(["tail", "-1", logfile], capture_output=True, text=True, timeout=3)
        line = r.stdout.strip()
        if "rec/s" in line:
            # Extract rate like [3,656 rec/s]
            import re
            m = re.search(r'\[([\d,]+) rec/s\]', line)
            if m:
                return m.group(1)
            # Extract percentage
            m = re.search(r'\(([\d.]+)%\)', line)
            if m:
                return f"{m.group(1)}%"
        return ""
    except:
        return ""


def check_process_running(keyword):
    try:
        r = subprocess.run(["pgrep", "-f", keyword], capture_output=True, text=True, timeout=3)
        return bool(r.stdout.strip())
    except:
        return False


def build_task_list(vip_info, svip_info):
    """根据实时数据构建完整 task list"""
    vip_exists, vip_count, vip_idx, vip_idx_fields = vip_info
    svip_exists, svip_count, svip_idx, svip_idx_fields = svip_info

    vip_target = TARGETS["img_202604_vip"]
    svip_target = TARGETS["img_202604_svip"]

    # 检测迁移进程是否在跑
    vip_migrating = check_process_running("migrate_to_multi.*p_202604 ")
    svip_migrating = check_process_running("migrate_to_multi.*p_202604_svip")

    # VIP 迁移日志
    vip_log = "/tmp/claude-1000/-workspace/3291135e-cd1a-4c54-ad28-fc4fdab78a1f/tasks/bfake3jzq.output"
    svip_log = "/tmp/claude-1000/-workspace/3291135e-cd1a-4c54-ad28-fc4fdab78a1f/tasks/b6hfiohys.output"

    tasks = [
        {"id": 0, "name": "脚本改造支持 --partition 参数", "status": "done", "eta": "✅"},
        {"id": 1, "name": "SVIP: 下载50万图 + GPU推理", "status": "done", "eta": "✅"},
        {"id": 2, "name": "SVIP: 衍生2000万 → p_202604_svip", "status": "done", "eta": "✅"},
        {"id": 3, "name": "PG bitmap 全量重建 (39,499,401)", "status": "done", "eta": "✅ 100%覆盖"},
        {"id": 4, "name": "etcd 3节点集群部署", "status": "done", "eta": "✅"},
    ]

    # --- 5a: VIP 迁移 ---
    if vip_migrating:
        rate = get_migration_rate(vip_log)
        tasks.append({"id": "5a", "name": "迁移 VIP → img_202604_vip (2000万)",
                       "status": "running", "eta": f"迁移中 {rate}"})
    elif vip_count > 0:
        tasks.append({"id": "5a", "name": "迁移 VIP → img_202604_vip (2000万)",
                       "status": "done", "eta": f"✅ {vip_count:,} 条"})
    else:
        tasks.append({"id": "5a", "name": "迁移 VIP → img_202604_vip (2000万)",
                       "status": "pending", "eta": "等待开始"})

    # --- 5b: SVIP 迁移 ---
    if svip_migrating:
        rate = get_migration_rate(svip_log)
        tasks.append({"id": "5b", "name": "迁移 SVIP → img_202604_svip (2000万)",
                       "status": "running", "eta": f"迁移中 {rate}"})
    elif svip_count >= svip_target * 0.9 and not svip_migrating:
        tasks.append({"id": "5b", "name": "迁移 SVIP → img_202604_svip (2000万)",
                       "status": "done", "eta": f"✅ {svip_count:,} 条"})
    elif svip_count > 0:
        tasks.append({"id": "5b", "name": "迁移 SVIP → img_202604_svip (2000万)",
                       "status": "running", "eta": f"{svip_count:,} 条 (等待重跑)"})
    else:
        tasks.append({"id": "5b", "name": "迁移 SVIP → img_202604_svip (2000万)",
                       "status": "pending", "eta": "等5a完成"})

    # --- 5c: VIP 索引 ---
    # VIP 迁移进程在建索引时仍在跑, 通过 vip_migrating + vip_idx 判断阶段
    if vip_idx >= 11:
        tasks.append({"id": "5c", "name": "img_202604_vip 独立建索引",
                       "status": "done", "eta": f"✅ {vip_idx}/11 索引"})
    elif vip_migrating and vip_idx == 1:
        # 进程在跑且只有 HNSW, 说明标量正在提交
        tasks.append({"id": "5c", "name": "img_202604_vip 独立建索引",
                       "status": "running", "eta": f"HNSW✅, 标量索引提交中..."})
    elif vip_migrating and vip_idx == 0:
        tasks.append({"id": "5c", "name": "img_202604_vip 独立建索引",
                       "status": "running", "eta": "HNSW 构建中..."})
    elif vip_idx > 0:
        built_list = ", ".join(vip_idx_fields[:3])
        more = f"+{vip_idx-3}" if vip_idx > 3 else ""
        tasks.append({"id": "5c", "name": "img_202604_vip 独立建索引",
                       "status": "running", "eta": f"{vip_idx}/11 已建: {built_list}{more}"})
    else:
        tasks.append({"id": "5c", "name": "img_202604_vip 独立建索引",
                       "status": "pending", "eta": "等5a完成"})

    # --- 5d: SVIP 索引 ---
    if svip_idx >= 11:
        tasks.append({"id": "5d", "name": "img_202604_svip 独立建索引",
                       "status": "done", "eta": f"✅ {svip_idx}/11 索引"})
    elif svip_idx > 0:
        built_list = ", ".join(svip_idx_fields[:3])
        more = f"+{svip_idx-3}" if svip_idx > 3 else ""
        tasks.append({"id": "5d", "name": "img_202604_svip 独立建索引",
                       "status": "running", "eta": f"{svip_idx}/11 已建: {built_list}{more}"})
    elif svip_migrating:
        tasks.append({"id": "5d", "name": "img_202604_svip 独立建索引",
                       "status": "pending", "eta": "等5b迁移完成"})
    else:
        tasks.append({"id": "5d", "name": "img_202604_svip 独立建索引",
                       "status": "pending", "eta": "等5b完成"})

    # --- 后续任务 ---
    tasks.extend([
        {"id": 6, "name": "常青分区 img_999999 (1000万)", "status": "pending", "eta": "独立Collection"},
        {"id": 7, "name": "冷区 img_202603_vip + img_202603_svip", "status": "pending", "eta": "独立Collection,各2000万"},
        {"id": 8, "name": "全链路验证 (多Collection并行查询)", "status": "pending", "eta": "~10min"},
        {"id": 9, "name": "全链路打通: 写入+查询接口联调", "status": "pending", "eta": "~2h"},
        {"id": 10, "name": "全链路批量写入1000万", "status": "pending", "eta": "~4h"},
        {"id": 11, "name": "全链路查询测试 50000条", "status": "pending", "eta": "~2h"},
    ])

    return tasks


def main():
    log.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 任务状态更新器启动, 间隔 {INTERVAL}s")

    last_summary = ""
    for _ in range(MAX_ROUNDS):
        try:
            vip_info = get_collection_info("img_202604_vip")
            svip_info = get_collection_info("img_202604_svip")

            tasks = build_task_list(vip_info, svip_info)

            with open(TASK_FILE, "w") as f:
                json.dump(tasks, f, ensure_ascii=False, indent=2)

            # 简要日志 (只在变化时打印)
            summary = (f"VIP: {vip_info[1]:,} idx={vip_info[2]} | "
                       f"SVIP: {svip_info[1]:,} idx={svip_info[2]}")
            if summary != last_summary:
                log.info(f"[{time.strftime('%H:%M:%S')}] {summary}")
                last_summary = summary

            # 检查是否全部完成
            if vip_info[2] >= 11 and svip_info[2] >= 11:
                log.info("✅ 所有迁移子任务完成!")
                break

        except Exception as e:
            log.error(f"Error: {e}")

        time.sleep(INTERVAL)

    log.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 更新器结束")


if __name__ == "__main__":
    main()
