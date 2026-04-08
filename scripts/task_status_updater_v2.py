#!/usr/bin/env python3
"""
任务状态实时更新器 v2: 多日志源 + 多进程并行感知
每 10 秒刷新 task_list.json

跟踪:
  - Milvus 索引 (直接查 pymilvus)
  - 常青 reimport 日志 (evergreen_reimport_*.log)
  - 冷区批量下载推理日志 (batch_dl_inf_*.log)
  - gpu_worker 容器内进程 + 磁盘文件数
"""
import json, time, logging, subprocess, re, glob, os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

TASK_FILE = "/data/imgsrch/task_logs/task_list.json"
LOG_DIR = "/data/imgsrch/task_logs"
INTERVAL = 10
MAX_ROUNDS = 5000


def get_collection_info(name):
    try:
        from pymilvus import connections, Collection, utility
        try:
            connections.connect("default", host="localhost", port=19530, timeout=5)
        except:
            pass
        if not utility.has_collection(name):
            return False, 0, 0
        c = Collection(name)
        return True, c.num_entities, len(c.indexes)
    except:
        return False, 0, 0


def read_last_progress(log_pattern):
    """从日志文件中读取最新的 ##PROGRESS## 行"""
    try:
        logs = sorted(glob.glob(log_pattern), key=os.path.getmtime)
        if not logs:
            return None
        logfile = logs[-1]
        with open(logfile, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 8192))
            content = f.read().decode("utf-8", errors="replace")

        progress = None
        for line in content.splitlines():
            if "##PROGRESS##" in line:
                m = re.search(r'##PROGRESS##({.*})', line)
                if m:
                    try:
                        progress = json.loads(m.group(1))
                    except:
                        pass
        return progress
    except:
        return None


def check_gpu_worker_process(keyword):
    """检查 gpu_worker 容器内是否有匹配的进程"""
    try:
        r = subprocess.run(
            ["docker", "exec", "gpu_worker", "ps", "aux"],
            capture_output=True, text=True, timeout=5)
        for line in r.stdout.splitlines():
            if keyword in line and "python" in line:
                return True
        return False
    except:
        return False


def count_files_in_container(path):
    """统计 gpu_worker 容器内目录的文件数"""
    try:
        r = subprocess.run(
            ["docker", "exec", "gpu_worker", "bash", "-c", f"ls {path}/ 2>/dev/null | wc -l"],
            capture_output=True, text=True, timeout=5)
        return int(r.stdout.strip())
    except:
        return 0


def detect_cold_vip_progress():
    """检测冷区 VIP 下载进度 (进程在 gpu_worker 容器内)"""
    alive = check_gpu_worker_process("cold_vip")
    prog = read_last_progress(f"{LOG_DIR}/cold_vip_reimport_*.log") or read_last_progress(f"{LOG_DIR}/batch_dl_inf_*.log")
    files_on_disk = count_files_in_container("/tmp/cold_vip_images")
    if alive or files_on_disk > 0:
        return {"alive": alive, "files": files_on_disk, "total": 400000, "log_progress": prog}
    return None


def detect_cold_svip_progress():
    """检测冷区 SVIP 下载进度"""
    alive = check_gpu_worker_process("cold_svip")
    prog = read_last_progress(f"{LOG_DIR}/cold_svip_dl_inf_*.log")
    files_on_disk = count_files_in_container("/tmp/cold_svip_images")
    if alive or files_on_disk > 0:
        return {"alive": alive, "files": files_on_disk, "total": 500000, "log_progress": prog}
    return None


def check_host_process(keyword):
    """检查宿主机上是否有匹配的进程"""
    try:
        r = subprocess.run(["pgrep", "-f", keyword], capture_output=True, text=True, timeout=5)
        return r.returncode == 0
    except:
        return False


def detect_evergreen_progress():
    """检测常青 reimport 进度"""
    # 1. 检查日志
    prog = read_last_progress(f"{LOG_DIR}/evergreen_reimport_*.log")

    # 2. 检查进程
    alive = check_gpu_worker_process("img_999999")

    if prog or alive:
        return {"alive": alive, "progress": prog}
    return None


def main():
    log.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 状态更新器 v2 启动 (多源感知)")

    last_summary = ""
    for _ in range(MAX_ROUNDS):
        try:
            # === Milvus 状态 ===
            vip_ok, vip_n, vip_idx = get_collection_info("img_202604_vip")
            svip_ok, svip_n, svip_idx = get_collection_info("img_202604_svip")
            eg_ok, eg_n, eg_idx = get_collection_info("img_999999")
            cold_vip_ok, cold_vip_n, _ = get_collection_info("img_202603_vip")
            cold_svip_ok, cold_svip_n, _ = get_collection_info("img_202603_svip")

            # === 进程感知 ===
            evergreen_info = detect_evergreen_progress()
            cold_vip_info = detect_cold_vip_progress()
            cold_svip_info = detect_cold_svip_progress()
            derive_eg_alive = check_host_process("derive_records.*img_999999")
            derive_cold_vip_alive = check_host_process("derive_records.*img_202603_vip")
            index_builder_alive = check_host_process("incremental_index_builder")

            # === 构建任务列表 ===
            tasks = [
                {"id": 0, "name": "脚本改造支持 --partition 参数", "status": "done", "eta": "✅"},
                {"id": 1, "name": "SVIP: 下载50万图 + GPU推理", "status": "done", "eta": "✅"},
                {"id": 2, "name": "SVIP: 衍生2000万", "status": "done", "eta": "✅"},
                {"id": 3, "name": "PG bitmap 全量重建", "status": "done", "eta": "✅ 39,499,401"},
                {"id": 4, "name": "etcd 3节点集群部署", "status": "done", "eta": "✅"},
                {"id": "5a", "name": "迁移 VIP → img_202604_vip", "status": "done", "eta": f"✅ {vip_n:,}条"},
                {"id": "5b", "name": "迁移 SVIP → img_202604_svip", "status": "done", "eta": f"✅ {svip_n:,}条"},
            ]

            # 5c: VIP 索引
            if vip_idx >= 11:
                tasks.append({"id": "5c", "name": "img_202604_vip 索引", "status": "done", "eta": f"✅ {vip_idx}/11"})
            else:
                tasks.append({"id": "5c", "name": "img_202604_vip 索引", "status": "running", "eta": f"{vip_idx}/11 构建中"})

            # 5d: SVIP 索引
            if svip_idx >= 11:
                tasks.append({"id": "5d", "name": "img_202604_svip 索引", "status": "done", "eta": f"✅ {svip_idx}/11"})
            else:
                tasks.append({"id": "5d", "name": "img_202604_svip 索引", "status": "running", "eta": f"{svip_idx}/11 构建中"})

            # === Task 6: 常青 img_999999 推理+写入 ===
            if eg_n > 200000 and not (evergreen_info and evergreen_info.get("alive")):
                tasks.append({"id": 6, "name": "常青 img_999999 推理+写入", "status": "done", "eta": f"✅ {eg_n:,}条"})
            elif evergreen_info and evergreen_info.get("alive"):
                prog = evergreen_info.get("progress")
                if prog:
                    stage_cn = {"download": "下载", "extract": "推理", "milvus": "写入Milvus", "collect": "收集"}.get(prog.get("stage", ""), prog.get("stage", ""))
                    completed = prog.get("completed", 0)
                    total = prog.get("total", 0)
                    pct = completed * 100 / total if total > 0 else 0
                    tasks.append({"id": 6, "name": "常青 img_999999 推理+写入",
                                  "status": "running",
                                  "eta": f"{stage_cn} {completed:,}/{total:,} ({pct:.0f}%)"})
                else:
                    tasks.append({"id": 6, "name": "常青 img_999999 推理+写入", "status": "running", "eta": "进程运行中"})
            elif eg_n == 0:
                tasks.append({"id": 6, "name": "常青 img_999999 推理+写入", "status": "failed", "eta": "❌ 写入失败, 需重试"})
            else:
                tasks.append({"id": 6, "name": "常青 img_999999 推理+写入", "status": "done", "eta": f"✅ {eg_n:,}条"})

            # === Task 7: 冷区下载推理 ===
            cold_svip_alive = cold_svip_info and cold_svip_info.get("alive")
            cold_vip_alive = cold_vip_info and cold_vip_info.get("alive")

            if cold_vip_n > 0 and cold_svip_n > 0 and not cold_svip_alive:
                tasks.append({"id": 7, "name": "冷区 img_202603 下载+推理",
                              "status": "done", "eta": f"✅ VIP:{cold_vip_n:,} SVIP:{cold_svip_n:,}"})
            elif cold_svip_alive:
                prog = cold_svip_info.get("log_progress")
                if prog and prog.get("total", 0) > 0:
                    stage_cn = {"download": "下载", "extract": "推理", "milvus": "写入"}.get(prog.get("stage", ""), prog.get("stage", ""))
                    pct = prog["completed"] * 100 / prog["total"]
                    tasks.append({"id": 7, "name": "冷区 img_202603 下载+推理",
                                  "status": "running",
                                  "eta": f"VIP✅{cold_vip_n:,}条, SVIP {stage_cn} {prog['completed']:,}/{prog['total']:,} ({pct:.0f}%)"})
                else:
                    svip_files = cold_svip_info.get("files", 0)
                    tasks.append({"id": 7, "name": "冷区 img_202603 下载+推理",
                                  "status": "running",
                                  "eta": f"VIP✅{cold_vip_n:,}条, SVIP 下载中 {svip_files:,}/500K"})
            elif cold_vip_alive:
                files = cold_vip_info.get("files", 0)
                tasks.append({"id": 7, "name": "冷区 img_202603 下载+推理",
                              "status": "running", "eta": f"VIP 下载 {files:,}/400K"})
            elif cold_vip_n > 0:
                tasks.append({"id": 7, "name": "冷区 img_202603 下载+推理",
                              "status": "running", "eta": f"VIP✅{cold_vip_n:,}条, SVIP 下载中"})
            else:
                tasks.append({"id": 7, "name": "冷区 img_202603 下载+推理", "status": "pending", "eta": "待启动"})

            # === Task 8: 衍生 ===
            # 读取 derive 日志判断是否在重建索引
            eg_derive_log = read_last_progress(f"{LOG_DIR}/derive_999999_*.log")
            eg_rebuilding_index = False
            if derive_eg_alive and eg_n >= 5_000_000:
                # 数据量已达标但进程还活着 → 在重建索引
                eg_rebuilding_index = True

            eg_derive_done = eg_n >= 5_000_000 and not derive_eg_alive
            eg_derive_status = "done" if eg_derive_done else "running" if derive_eg_alive else "pending"
            cold_vip_derive_done = cold_vip_n >= 1_000_000 and not derive_cold_vip_alive
            cold_vip_derive_status = "done" if cold_vip_derive_done else "running" if derive_cold_vip_alive else "pending"

            parts = []
            if eg_derive_status == "done":
                parts.append(f"常青✅{eg_n:,}")
            elif eg_rebuilding_index:
                parts.append(f"常青 HNSW重建中({eg_n:,})")
            elif eg_derive_status == "running":
                parts.append(f"常青衍生中({eg_n:,})")
            else:
                parts.append("常青⏳")

            if cold_vip_derive_status == "done":
                parts.append(f"冷VIP✅{cold_vip_n:,}")
            elif cold_vip_derive_status == "running":
                parts.append(f"冷VIP衍生中({cold_vip_n:,})")
            else:
                parts.append("冷VIP⏳")

            # 冷SVIP 衍生依赖下载推理完成
            parts.append("冷SVIP⏳")

            all_derive_done = eg_derive_done and cold_vip_derive_done and cold_svip_n >= 1_000_000
            derive_status = "done" if all_derive_done else "running" if (derive_eg_alive or derive_cold_vip_alive) else "pending"
            tasks.append({"id": 8, "name": "衍生: 常青+冷区", "status": derive_status, "eta": " | ".join(parts)})

            # === Task 9: 索引 ===
            # 索引状态: 需要检查所有 collection 的索引 + derive 进程的索引重建
            cold_vip_idx = get_collection_info("img_202603_vip")[2]
            cold_svip_idx = get_collection_info("img_202603_svip")[2]
            all_idx_done = (eg_idx >= 11 and cold_vip_idx >= 11 and cold_svip_idx >= 11
                           and not eg_rebuilding_index and not index_builder_alive)
            any_idx_running = index_builder_alive or eg_rebuilding_index

            if all_idx_done:
                tasks.append({"id": 9, "name": "各Collection独立建索引", "status": "done", "eta": "✅"})
            elif any_idx_running:
                idx_parts = []
                if eg_rebuilding_index:
                    idx_parts.append("常青HNSW重建中")
                if index_builder_alive:
                    idx_parts.append("标量索引构建中")
                if cold_svip_idx < 11:
                    idx_parts.append(f"冷SVIP {cold_svip_idx}/11")
                tasks.append({"id": 9, "name": "各Collection独立建索引", "status": "running",
                              "eta": " | ".join(idx_parts) if idx_parts else "构建中"})
            else:
                pending_parts = []
                if eg_idx < 11: pending_parts.append(f"常青{eg_idx}/11")
                if cold_vip_idx < 11: pending_parts.append(f"冷VIP{cold_vip_idx}/11")
                if cold_svip_idx < 11: pending_parts.append(f"冷SVIP{cold_svip_idx}/11")
                tasks.append({"id": 9, "name": "各Collection独立建索引", "status": "pending",
                              "eta": " | ".join(pending_parts) if pending_parts else ""})

            # === 后续任务 ===
            tasks.extend([
                {"id": 10, "name": "⭐ Milvus分布式部署 (docker)", "status": "pending", "eta": "等当前任务完成"},
                {"id": 11, "name": "⭐ 推理服务4GPU并行部署", "status": "pending", "eta": ""},
                {"id": 12, "name": "全链路验证 (多Collection查询)", "status": "pending", "eta": ""},
                {"id": 13, "name": "全链路写入+查询接口联调", "status": "pending", "eta": ""},
                {"id": 14, "name": "全链路批量写入1000万", "status": "pending", "eta": ""},
                {"id": 15, "name": "全链路查询测试 50000条", "status": "pending", "eta": ""},
                {"id": 16, "name": "📋 K8s生产部署 (方案已就绪)", "status": "pending", "eta": "生产环境执行"},
            ])

            with open(TASK_FILE, "w") as f:
                json.dump(tasks, f, ensure_ascii=False, indent=2)

            # 日志 (变化时打印)
            eg_status = f"常青:{eg_n:,}"
            if evergreen_info and evergreen_info.get("alive"):
                p = evergreen_info.get("progress", {})
                eg_status = f"常青推理:{p.get('completed',0):,}/{p.get('total',0):,}"
            cv_status = f"冷VIP:{cold_vip_n:,}"
            if cold_vip_info and cold_vip_info.get("alive"):
                cv_status = f"冷VIP下载:{cold_vip_info.get('files',0):,}/400K"
            summary = f"5c:{vip_idx}/11 5d:{svip_idx}/11 | {eg_status} | {cv_status}"
            if summary != last_summary:
                log.info(summary)
                last_summary = summary

        except Exception as e:
            log.error(f"Error: {e}")

        time.sleep(INTERVAL)

    log.info("更新器结束")


if __name__ == "__main__":
    main()
