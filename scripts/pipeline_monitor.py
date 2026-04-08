#!/usr/bin/env python3
"""
批量导入流水线 · 实时监控页
手机/PC 浏览器访问: http://<IP>:9800

功能:
  - 当前批次进度、ETA
  - Milvus / PG 实时数据量
  - 磁盘/内存/GPU 资源
  - 最近日志 (自动刷新)
  - 批次历史记录

Usage:
    python3 scripts/pipeline_monitor.py
    python3 scripts/pipeline_monitor.py --port 9800
"""
import argparse
import glob
import json
import os
import re
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

app = FastAPI(title="Pipeline Monitor")

DATA_ROOT = os.getenv("DATA_ROOT", "/data/imgsrch")
STATE_FILE = f"{DATA_ROOT}/task_logs/pipeline_state.json"
TOTAL_TARGET = 80_000_000  # 8000 万目标 (热区VIP/SVIP各2000万 + 冷区VIP/SVIP各2000万)


# ── 数据采集 ──

def get_pipeline_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"last_batch": 0, "status": "not_started", "timestamp": ""}


def get_running_batch() -> dict | None:
    """检测当前正在运行的 batch_import 进程"""
    try:
        r = subprocess.run(
            ["ps", "aux"], capture_output=True, text=True, timeout=5
        )
        for line in r.stdout.splitlines():
            if "batch_import_clothing" in line and "python" in line:
                # 提取 batch 号
                m = re.search(r"urls_batch(\d+)", line)
                batch_id = m.group(1) if m else "?"
                return {"batch_id": batch_id, "pid": line.split()[1]}
        # 也检查 derive
        for line in r.stdout.splitlines():
            if "derive_records" in line and "python" in line:
                return {"batch_id": "derive", "pid": line.split()[1], "phase": "derive"}
    except Exception:
        pass
    return None


def get_db_stats() -> dict:
    stats = {"milvus": 0, "pg_dedup": 0, "pg_bitmap": 0, "pg_merchants": 0}
    try:
        # 先用 socket 快速检测 Milvus 是否可达, 避免 pymilvus retry 卡住
        import socket
        _s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _s.settimeout(2)
        _s.connect(("localhost", 19530))
        _s.close()
    except Exception:
        return stats
    try:
        from pymilvus import connections, Collection, utility
        connections.connect("default", host="localhost", port=19530, timeout=3)
        total = 0
        for name in utility.list_collections():
            try:
                c = Collection(name)
                total += c.num_entities
            except:
                pass
        stats["milvus"] = total
    except Exception as e:
        stats["milvus_error"] = str(e)[:80]
    try:
        import psycopg2
        conn = psycopg2.connect(
            "postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search",
            connect_timeout=3
        )
        cur = conn.cursor()
        for tbl in ["uri_dedup", "image_merchant_bitmaps", "merchant_id_mapping"]:
            cur.execute(f"SELECT count(*) FROM {tbl}")
            key = f"pg_{tbl.replace('image_merchant_', '')}"
            stats[key] = cur.fetchone()[0]
        cur.close()
        conn.close()
    except Exception as e:
        stats["pg_error"] = str(e)[:80]
    return stats


def get_system_stats() -> dict:
    stats = {}
    try:
        import psutil
        stats["cpu_pct"] = psutil.cpu_percent(interval=0.5)
        mem = psutil.virtual_memory()
        stats["mem_total_gb"] = round(mem.total / (1024**3), 1)
        stats["mem_used_gb"] = round(mem.used / (1024**3), 1)
        stats["mem_pct"] = mem.percent
        disk = psutil.disk_usage("/data")
        stats["disk_total_gb"] = round(disk.total / (1024**3), 0)
        stats["disk_used_gb"] = round(disk.used / (1024**3), 0)
        stats["disk_free_gb"] = round(disk.free / (1024**3), 0)
        stats["disk_pct"] = disk.percent
        # 网络流量
        net = psutil.net_io_counters()
        stats["net_sent_gb"] = round(net.bytes_sent / (1024**3), 2)
        stats["net_recv_gb"] = round(net.bytes_recv / (1024**3), 2)
        stats["net_sent_bytes"] = net.bytes_sent
        stats["net_recv_bytes"] = net.bytes_recv
    except ImportError:
        pass
    # GPU
    try:
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used,memory.total",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5
        )
        gpus = []
        for line in r.stdout.strip().splitlines():
            parts = [x.strip() for x in line.split(",")]
            gpus.append({
                "util": int(parts[0]),
                "mem_used": int(parts[1]),
                "mem_total": int(parts[2]),
            })
        stats["gpus"] = gpus
    except Exception:
        pass
    return stats


def get_batch_history() -> list:
    """扫描 task_logs 获取已完成批次"""
    logs = sorted(glob.glob(f"{DATA_ROOT}/task_logs/batch_*_import.log"))
    history = []
    for log_path in logs[-20:]:  # 最近 20 批
        name = os.path.basename(log_path)
        m = re.match(r"batch_(\d+)_import\.log", name)
        if not m:
            continue
        batch_id = m.group(1)
        size = os.path.getsize(log_path)
        mtime = datetime.fromtimestamp(os.path.getmtime(log_path))

        # 从日志提取关键信息
        status = "running"
        images = 0
        try:
            with open(log_path, "r") as f:
                content = f.read()
            if "Import complete:" in content:
                status = "done"
                m2 = re.search(r"Import complete: (\d+) images", content)
                if m2:
                    images = int(m2.group(1))
            elif "aborting" in content.lower():
                status = "failed"
            # 提取下载进度
            dl_matches = re.findall(r"Download progress: (\d+)/(\d+)", content)
            if dl_matches and status == "running":
                done, total = int(dl_matches[-1][0]), int(dl_matches[-1][1])
                status = f"downloading {done}/{total}"
        except Exception:
            pass

        history.append({
            "batch_id": batch_id,
            "status": status,
            "images": images,
            "time": mtime.strftime("%m-%d %H:%M"),
            "size_kb": size // 1024,
        })
    return history


def get_services_status() -> list:
    """检测各微服务 + 基础设施的运行状态"""
    import httpx
    import socket

    services = []

    # ── 业务微服务 ──
    http_checks = [
        ("inference-service",  "http://localhost:8090/healthz",  8090, "Python/CUDA", "CLIP 向量提取 + 零样本分类"),
        ("search-service",     "http://localhost:8080/healthz",  8080, "Python",      "主检索服务"),
        ("write-service",      "http://localhost:8081/healthz",  8081, "Python",      "图片写入 Milvus+PG"),
        ("cron-scheduler",     "http://localhost:8082/healthz",  8082, "Python",      "定时任务 (分区轮转/bitmap)"),
    ]
    for name, url, port, lang, desc in http_checks:
        entry = {"name": name, "port": port, "lang": lang, "desc": desc, "status": "down", "detail": ""}
        try:
            r = httpx.get(url, timeout=3)
            if r.status_code == 200:
                entry["status"] = "up"
                entry["detail"] = r.text[:120]
            else:
                entry["status"] = "error"
                entry["detail"] = f"HTTP {r.status_code}"
        except Exception:
            # 尝试备用端口 (search-service 可能在 8079)
            if port == 8080:
                try:
                    r2 = httpx.get("http://localhost:8079/api/v1/healthz", timeout=3)
                    if r2.status_code == 200:
                        entry["status"] = "up"
                        entry["port"] = 8079
                        entry["detail"] = r2.text[:120]
                except Exception:
                    pass
        services.append(entry)

    # bitmap-filter-service (gRPC)
    entry = {"name": "bitmap-filter-service", "port": 50051, "lang": "Java/gRPC", "desc": "Bitmap 过滤 (RocksDB+Kafka)", "status": "down", "detail": ""}
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        if s.connect_ex(("localhost", 50051)) == 0:
            entry["status"] = "up"
            entry["detail"] = "gRPC listening"
        s.close()
    except Exception:
        pass
    services.append(entry)

    # ── 基础设施 ──
    infra_checks = [
        ("PostgreSQL",   5432,  "tcp",  "数据存储 (uri_dedup/bitmap)"),
        ("Redis",        6379,  "tcp",  "缓存"),
        ("Milvus",       19530, "tcp",  "向量数据库"),
        ("Kafka",        29092, "tcp",  "消息队列"),
        ("etcd",         2379,  "docker", "Milvus 元数据 (3节点集群)"),
        ("ClickHouse",   8123,  "tcp",  "分析型存储"),
        ("Prometheus",   9099,  "tcp",  "监控采集"),
        ("Grafana",      3002,  "tcp",  "监控面板"),
    ]
    for name, port, proto, desc in infra_checks:
        entry = {"name": name, "port": port, "lang": "infra", "desc": desc, "status": "down", "detail": ""}
        try:
            if proto == "docker":
                # etcd 3节点集群: 通过 docker 检查健康状态
                import subprocess
                r = subprocess.run(
                    ["docker", "exec", "etcd-node-1", "etcdctl", "endpoint", "health",
                     "--endpoints=etcd-node-1:2379,etcd-node-2:2379,etcd-node-3:2379"],
                    capture_output=True, text=True, timeout=5)
                healthy = r.stdout.count("is healthy")
                if healthy > 0:
                    entry["status"] = "up"
                    entry["detail"] = f"{healthy}/3 nodes healthy"
            else:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2)
                if s.connect_ex(("localhost", port)) == 0:
                    entry["status"] = "up"
                s.close()
        except Exception:
            pass
        services.append(entry)

    # ── 数据量补充 (Milvus 多 Collection) ──
    try:
        import socket
        _s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _s.settimeout(2)
        _s.connect(("localhost", 19530))
        _s.close()
        from pymilvus import connections, Collection, utility
        connections.connect("default", host="localhost", port=19530, timeout=3)
        coll_info = []
        for name in utility.list_collections():
            try:
                c = Collection(name)
                n = c.num_entities
                if n > 0:
                    coll_info.append(f"{name}={n:,}")
            except:
                pass
        for svc in services:
            if svc["name"] == "Milvus":
                svc["detail"] = " | ".join(coll_info) if coll_info else "connected"
    except Exception:
        pass

    try:
        import psycopg2
        conn = psycopg2.connect("postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search", connect_timeout=3)
        cur = conn.cursor()
        pg_details = []
        for tbl in ["uri_dedup", "image_merchant_bitmaps", "merchant_id_mapping"]:
            cur.execute(f"SELECT count(*) FROM {tbl}")
            cnt = cur.fetchone()[0]
            pg_details.append(f"{tbl}: {cnt:,}")
        cur.close()
        conn.close()
        for svc in services:
            if svc["name"] == "PostgreSQL":
                svc["detail"] = " | ".join(pg_details)
    except Exception:
        pass

    return services


def get_recent_log(batch_id: str = None, lines: int = 30) -> str:
    """获取最近的日志内容"""
    if batch_id:
        log_path = f"{DATA_ROOT}/task_logs/batch_{batch_id}_import.log"
    else:
        # 找最新的日志
        logs = sorted(glob.glob(f"{DATA_ROOT}/task_logs/batch_*_import.log"),
                       key=os.path.getmtime)
        log_path = logs[-1] if logs else None

    if not log_path or not os.path.exists(log_path):
        return "(no log)"

    try:
        with open(log_path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            read_size = min(size, 8192)
            f.seek(max(0, size - read_size))
            content = f.read().decode("utf-8", errors="replace")
        # 取最后 N 行
        return "\n".join(content.splitlines()[-lines:])
    except Exception as e:
        return f"(error: {e})"


def get_task_progress() -> dict:
    """从最新日志解析进度 (batch_import 或 derive)"""
    # 找最新的日志文件 (batch_import, derive, 或 svip pipeline)
    all_logs = glob.glob(f"{DATA_ROOT}/task_logs/batch_import_*.log") + \
               glob.glob(f"{DATA_ROOT}/task_logs/derive_*.log") + \
               glob.glob(f"{DATA_ROOT}/task_logs/svip_*.log")
    if not all_logs:
        return {}
    logs = sorted(all_logs, key=os.path.getmtime)
    log_path = logs[-1]
    try:
        with open(log_path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            read_size = min(size, 32768)
            f.seek(max(0, size - read_size))
            content = f.read().decode("utf-8", errors="replace")
        # 找最后一条 PROGRESS 行
        progress = {}
        for line in content.splitlines():
            if "##PROGRESS##" in line:
                try:
                    payload = line.split("##PROGRESS##", 1)[1]
                    progress = json.loads(payload)
                except Exception:
                    pass
        # 如果没有 PROGRESS 行，尝试解析 derive 日志格式
        if not progress and "derive" in log_path:
            for line in content.splitlines():
                if "Generated variants for" in line:
                    m = re.search(r"(\d+)/(\d+) records \((\d+) variants", line)
                    if m:
                        done_src = int(m.group(1))
                        total_src = int(m.group(2))
                        n_variants = int(m.group(3))
                        progress = {
                            "stage": "derive",
                            "completed": n_variants,
                            "total": 19600044,
                            "message": f"衍生中: {done_src:,}/{total_src:,} 源记录, {n_variants:,} 变体",
                        }
                elif "Milvus upsert" in line:
                    m = re.search(r"(\d+)/(\d+)", line)
                    if m:
                        progress = {
                            "stage": "derive_milvus",
                            "completed": int(m.group(1)),
                            "total": int(m.group(2)),
                            "message": f"衍生写入 Milvus: {m.group(1)}/{m.group(2)}",
                        }
                elif "uri_dedup" in line and "/" in line:
                    m = re.search(r"(\d+)/(\d+)", line)
                    if m:
                        progress = {
                            "stage": "derive_pg",
                            "completed": int(m.group(1)),
                            "total": int(m.group(2)),
                            "message": f"衍生写入 PG: {m.group(1)}/{m.group(2)}",
                        }
                elif "Batch" in line and "done" in line.lower():
                    m = re.search(r"Total written: ([\d,]+)/([\d,]+)", line)
                    if m:
                        progress = {
                            "stage": "derive",
                            "completed": int(m.group(1).replace(",", "")),
                            "total": int(m.group(2).replace(",", "")),
                            "message": line.split("] ", 1)[-1] if "] " in line else line,
                        }

        # 补充下载文件数 (磁盘实际计数)
        dl_dir = "/tmp/clothing_images"
        if os.path.isdir(dl_dir):
            try:
                progress["files_on_disk"] = len(os.listdir(dl_dir))
            except Exception:
                pass
        progress["log_file"] = os.path.basename(log_path)
        return progress
    except Exception:
        return {}


TASK_LIST_FILE = f"{DATA_ROOT}/task_logs/task_list.json"


def get_task_list() -> list:
    """读取任务清单"""
    if os.path.exists(TASK_LIST_FILE):
        try:
            with open(TASK_LIST_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return []


# ── API ──

@app.get("/api/status")
async def api_status():
    import asyncio
    import concurrent.futures
    loop = asyncio.get_event_loop()
    _pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    async def _safe(fn, default=None):
        try:
            return await asyncio.wait_for(loop.run_in_executor(_pool, fn), timeout=8)
        except Exception:
            return default if default is not None else {}

    state = get_pipeline_state()
    running = get_running_batch()
    db = await _safe(get_db_stats, {"milvus": 0, "pg_dedup": 0, "pg_bitmap": 0})
    sys_stats = await _safe(get_system_stats, {})
    history = await _safe(get_batch_history, [])
    services = await _safe(get_services_status, [])
    task_progress = get_task_progress()

    progress_pct = db.get("milvus", 0) / TOTAL_TARGET * 100 if TOTAL_TARGET > 0 else 0

    task_list = get_task_list()

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline": state,
        "running": running,
        "db": db,
        "system": sys_stats,
        "history": history,
        "services": services,
        "task_progress": task_progress,
        "task_list": task_list,
        "progress_pct": round(progress_pct, 2),
        "total_target": TOTAL_TARGET,
    }


@app.get("/api/log")
async def api_log(batch: str = None, lines: int = 50):
    return {"log": get_recent_log(batch, lines)}


# ── HTML 页面 ──

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML_PAGE


HTML_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<title>Pipeline Monitor</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, 'SF Pro', 'Helvetica Neue', sans-serif;
       background: #0d1117; color: #e6edf3; padding: 12px; max-width: 800px; margin: 0 auto; }
h1 { font-size: 18px; color: #58a6ff; margin-bottom: 12px; }
.card { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
        padding: 14px; margin-bottom: 12px; }
.card h2 { font-size: 13px; color: #8b949e; text-transform: uppercase;
            letter-spacing: 1px; margin-bottom: 10px; }
.stat-row { display: flex; justify-content: space-between; padding: 4px 0;
            border-bottom: 1px solid #21262d; font-size: 14px; }
.stat-row:last-child { border: none; }
.stat-label { color: #8b949e; }
.stat-value { color: #e6edf3; font-weight: 600; font-variant-numeric: tabular-nums; }
.stat-value.green { color: #3fb950; }
.stat-value.yellow { color: #d29922; }
.stat-value.red { color: #f85149; }
.stat-value.blue { color: #58a6ff; }

.progress-bar { width: 100%; height: 24px; background: #21262d; border-radius: 12px;
                overflow: hidden; margin: 8px 0; position: relative; }
.progress-fill { height: 100%; background: linear-gradient(90deg, #238636, #3fb950);
                 border-radius: 12px; transition: width 0.5s; min-width: 2px; }
.progress-text { position: absolute; top: 50%; left: 50%; transform: translate(-50%,-50%);
                 font-size: 12px; font-weight: 600; color: #fff; text-shadow: 0 1px 2px rgba(0,0,0,.5); }

.gpu-bar { display: flex; gap: 6px; margin-top: 6px; }
.gpu-chip { flex: 1; background: #21262d; border-radius: 6px; padding: 6px; text-align: center; font-size: 11px; }
.gpu-chip .pct { font-size: 16px; font-weight: 700; }

.log-box { background: #0d1117; border: 1px solid #30363d; border-radius: 6px;
           padding: 10px; font-family: 'SF Mono', 'Menlo', monospace; font-size: 11px;
           line-height: 1.5; max-height: 300px; overflow-y: auto; white-space: pre-wrap;
           word-break: break-all; color: #8b949e; }

.badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }
.badge.done { background: #238636; color: #fff; }
.badge.running { background: #1f6feb; color: #fff; animation: pulse 1.5s infinite; }
.badge.failed { background: #da3633; color: #fff; }
.badge.idle { background: #30363d; color: #8b949e; }

@keyframes pulse { 0%,100% { opacity:1; } 50% { opacity:.6; } }

.svc-table { width: 100%; font-size: 12px; border-collapse: collapse; }
.svc-table td { padding: 5px 6px; border-bottom: 1px solid #21262d; vertical-align: top; }
.svc-table .svc-name { font-weight: 600; white-space: nowrap; }
.svc-table .svc-port { color: #8b949e; font-family: 'SF Mono','Menlo',monospace; font-size: 11px; }
.svc-table .svc-detail { color: #8b949e; font-size: 11px; word-break: break-all; }
.svc-divider td { border-bottom: 2px solid #30363d; padding: 6px 6px 2px; color: #58a6ff;
                  font-size: 11px; text-transform: uppercase; letter-spacing: 1px; }

.history-table { width: 100%; font-size: 12px; }
.history-table td { padding: 4px 6px; border-bottom: 1px solid #21262d; }

.task-list { font-size: 12px; }
.task-item { display: flex; align-items: center; padding: 6px 8px; border-bottom: 1px solid #21262d;
             border-radius: 4px; margin-bottom: 2px; transition: background 0.3s; }
.task-item:last-child { border: none; }
.task-item.task-running { background: rgba(158, 217, 63, 0.12); border-left: 3px solid #9ed93f;
                          animation: runGlow 2s ease-in-out infinite; }
@keyframes runGlow { 0%,100% { background: rgba(158, 217, 63, 0.10); } 50% { background: rgba(158, 217, 63, 0.20); } }
.task-num { width: 24px; color: #484f58; font-size: 11px; flex-shrink: 0; }
.task-name { flex: 1; }
.task-status { flex-shrink: 0; margin-left: 8px; }
.task-eta { color: #484f58; font-size: 10px; margin-left: 6px; flex-shrink: 0; }
.task-item.task-running .task-num { color: #9ed93f; font-weight: 600; }
.task-item.task-running .task-eta { color: #9ed93f; }

.refresh-info { text-align: center; font-size: 11px; color: #484f58; margin-top: 8px; }
.timestamp { font-size: 11px; color: #484f58; float: right; }
</style>
</head>
<body>

<h1>📊 Pipeline Monitor <span class="timestamp" id="ts"></span></h1>

<!-- 总进度 -->
<div class="card" id="card-progress">
  <h2>总进度</h2>
  <div class="progress-bar">
    <div class="progress-fill" id="pbar" style="width:0%"></div>
    <div class="progress-text" id="ptext">-</div>
  </div>
  <div class="stat-row">
    <span class="stat-label">Milvus</span>
    <span class="stat-value blue" id="v-milvus">-</span>
  </div>
  <div class="stat-row">
    <span class="stat-label">PG uri_dedup</span>
    <span class="stat-value" id="v-pg-dedup">-</span>
  </div>
  <div class="stat-row">
    <span class="stat-label">PG bitmap</span>
    <span class="stat-value" id="v-pg-bitmap">-</span>
  </div>
  <div class="stat-row">
    <span class="stat-label">目标</span>
    <span class="stat-value" id="v-target">80,000,000</span>
  </div>
</div>

<!-- 任务清单 -->
<div class="card">
  <h2>任务清单</h2>
  <div class="task-list" id="task-list">加载中...</div>
</div>

<!-- 当前状态 -->
<div class="card">
  <h2>当前状态</h2>
  <div class="stat-row">
    <span class="stat-label">状态</span>
    <span id="v-status"><span class="badge idle">IDLE</span></span>
  </div>
  <div class="stat-row">
    <span class="stat-label">已完成批次</span>
    <span class="stat-value" id="v-last-batch">-</span>
  </div>
  <div class="stat-row">
    <span class="stat-label">运行中进程</span>
    <span class="stat-value" id="v-running">-</span>
  </div>
</div>

<!-- 任务进度 -->
<div class="card" id="card-task">
  <h2>任务进度</h2>
  <div class="stat-row">
    <span class="stat-label">阶段</span>
    <span class="stat-value blue" id="v-task-stage">-</span>
  </div>
  <div class="progress-bar">
    <div class="progress-fill" id="task-pbar" style="width:0%;background:linear-gradient(90deg,#1f6feb,#58a6ff)"></div>
    <div class="progress-text" id="task-ptext">-</div>
  </div>
  <div class="stat-row">
    <span class="stat-label">已完成 / 总计</span>
    <span class="stat-value" id="v-task-count">-</span>
  </div>
  <div class="stat-row">
    <span class="stat-label">磁盘文件数</span>
    <span class="stat-value" id="v-task-files">-</span>
  </div>
  <div class="stat-row">
    <span class="stat-label">速率</span>
    <span class="stat-value green" id="v-task-rate">-</span>
  </div>
  <div class="stat-row">
    <span class="stat-label">日志</span>
    <span class="stat-value" id="v-task-log" style="font-size:11px;color:#8b949e">-</span>
  </div>
</div>

<!-- 系统资源 -->
<div class="card">
  <h2>系统资源</h2>
  <div class="stat-row">
    <span class="stat-label">CPU</span>
    <span class="stat-value" id="v-cpu">-</span>
  </div>
  <div class="stat-row">
    <span class="stat-label">内存</span>
    <span class="stat-value" id="v-mem">-</span>
  </div>
  <div class="stat-row">
    <span class="stat-label">磁盘 /data</span>
    <span class="stat-value" id="v-disk">-</span>
  </div>
  <div class="stat-row">
    <span class="stat-label">网络 ↑/↓</span>
    <span class="stat-value" id="v-net">-</span>
  </div>
  <div class="stat-row">
    <span class="stat-label">网络速率</span>
    <span class="stat-value green" id="v-net-rate">-</span>
  </div>
  <div class="gpu-bar" id="gpu-bar"></div>
</div>

<!-- 服务状态 -->
<div class="card">
  <h2>服务状态</h2>
  <table class="svc-table" id="svc-table">
    <tr><td colspan="3" style="color:#484f58">加载中...</td></tr>
  </table>
</div>

<!-- 批次历史 -->
<div class="card">
  <h2>批次历史</h2>
  <table class="history-table" id="history-table">
    <tr><td colspan="4" style="color:#484f58">加载中...</td></tr>
  </table>
</div>

<!-- 实时日志 -->
<div class="card">
  <h2>实时日志</h2>
  <div class="log-box" id="log-box">加载中...</div>
</div>

<div class="refresh-info">每 5 秒自动刷新</div>

<script>
function fmt(n) { return n ? n.toLocaleString() : '-'; }

async function refresh() {
  try {
    const base = window.location.pathname.replace(/\/$/, '');
    const [statusRes, logRes] = await Promise.all([
      fetch(base + '/api/status'),
      fetch(base + '/api/log?lines=40')
    ]);
    const data = await statusRes.json();
    const logData = await logRes.json();

    // timestamp
    document.getElementById('ts').textContent = data.timestamp;

    // progress
    const pct = data.progress_pct || 0;
    document.getElementById('pbar').style.width = Math.min(pct, 100) + '%';
    document.getElementById('ptext').textContent = pct.toFixed(2) + '%';

    // DB stats
    const db = data.db || {};
    document.getElementById('v-milvus').textContent = fmt(db.milvus);
    document.getElementById('v-pg-dedup').textContent = fmt(db.pg_dedup || db.pg_uri_dedup);
    document.getElementById('v-pg-bitmap').textContent = fmt(db.pg_bitmap || db.pg_bitmaps);

    // pipeline state
    const pl = data.pipeline || {};
    const running = data.running;
    document.getElementById('v-last-batch').textContent = 'batch ' + String(pl.last_batch || 0).padStart(3, '0');

    if (running) {
      const batchLabel = running.phase === 'derive' ? '衍生中' : 'batch ' + running.batch_id;
      document.getElementById('v-status').innerHTML =
        '<span class="badge running">RUNNING</span> ' + batchLabel;
      document.getElementById('v-running').textContent = 'PID ' + running.pid;
    } else if (pl.status === 'completed' || pl.status === 'not_started') {
      document.getElementById('v-status').innerHTML = '<span class="badge idle">IDLE</span>';
      document.getElementById('v-running').textContent = '-';
    } else {
      document.getElementById('v-status').innerHTML =
        '<span class="badge failed">' + (pl.status || 'unknown').toUpperCase() + '</span>';
      document.getElementById('v-running').textContent = '-';
    }

    // task list
    const tl = data.task_list || [];
    if (tl.length) {
      document.getElementById('task-list').innerHTML = tl.map(t => {
        const isRunning = t.status === 'running';
        const icon = t.status === 'done' ? '<span style="color:#3fb950">✓</span>'
          : isRunning ? '<span style="color:#9ed93f;font-weight:700">▶</span>'
          : t.status === 'failed' ? '<span style="color:#f85149">✗</span>'
          : '<span style="color:#484f58">○</span>';
        const nameStyle = t.status === 'done' ? 'color:#8b949e;text-decoration:line-through'
          : isRunning ? 'color:#9ed93f;font-weight:600' : '';
        const rowClass = isRunning ? 'task-item task-running' : 'task-item';
        return '<div class="' + rowClass + '">' +
          '<span class="task-num">' + t.id + '</span>' +
          icon + ' ' +
          '<span class="task-name" style="' + nameStyle + '">' + t.name + '</span>' +
          '<span class="task-eta">' + (t.eta || '') + '</span>' +
          '</div>';
      }).join('');
    }

    // task progress
    const tp = data.task_progress || {};
    const stageNames = {download: '下载图片', inference: 'GPU推理', milvus: '写入Milvus', pg: '写入PG', collect: '收集URL', done: '完成'};
    document.getElementById('v-task-stage').textContent = stageNames[tp.stage] || tp.stage || '-';
    if (tp.total > 0) {
      const tpPct = (tp.completed / tp.total * 100);
      document.getElementById('task-pbar').style.width = Math.min(tpPct, 100) + '%';
      document.getElementById('task-ptext').textContent = tpPct.toFixed(1) + '%';
      document.getElementById('v-task-count').textContent = fmt(tp.completed) + ' / ' + fmt(tp.total);
    } else {
      document.getElementById('task-pbar').style.width = '0%';
      document.getElementById('task-ptext').textContent = '-';
      document.getElementById('v-task-count').textContent = '-';
    }
    document.getElementById('v-task-files').textContent = tp.files_on_disk ? fmt(tp.files_on_disk) : '-';
    // 任务速率
    if (window._lastTask && tp.completed > 0) {
      const dt = 5;
      const rate = (tp.completed - window._lastTask.completed) / dt;
      if (rate > 0) {
        const eta = (tp.total - tp.completed) / rate;
        const etaMin = Math.floor(eta / 60);
        const etaH = Math.floor(etaMin / 60);
        const etaStr = etaH > 0 ? etaH + 'h' + (etaMin%60) + 'm' : etaMin + 'min';
        document.getElementById('v-task-rate').textContent = rate.toFixed(1) + ' 张/s  ETA ' + etaStr;
      }
    }
    window._lastTask = { completed: tp.completed || 0 };
    document.getElementById('v-task-log').textContent = tp.log_file || '-';

    // system
    const sys = data.system || {};
    document.getElementById('v-cpu').textContent = (sys.cpu_pct || 0) + '%';
    document.getElementById('v-mem').textContent =
      (sys.mem_used_gb || 0) + ' / ' + (sys.mem_total_gb || 0) + ' GB (' + (sys.mem_pct || 0) + '%)';
    document.getElementById('v-disk').textContent =
      (sys.disk_used_gb || 0) + ' / ' + (sys.disk_total_gb || 0) + ' GB (空余 ' + (sys.disk_free_gb || 0) + 'GB)';
    document.getElementById('v-net').textContent =
      '↑ ' + (sys.net_sent_gb || 0) + ' GB  ↓ ' + (sys.net_recv_gb || 0) + ' GB';

    // 网络速率计算
    if (window._lastNet) {
      const dt = 5; // 5 秒刷新
      const sentRate = ((sys.net_sent_bytes || 0) - window._lastNet.sent) / dt;
      const recvRate = ((sys.net_recv_bytes || 0) - window._lastNet.recv) / dt;
      const fmtRate = (b) => b > 1048576 ? (b/1048576).toFixed(1) + ' MB/s' : (b/1024).toFixed(0) + ' KB/s';
      document.getElementById('v-net-rate').textContent = '↑ ' + fmtRate(sentRate) + '  ↓ ' + fmtRate(recvRate);
    }
    window._lastNet = { sent: sys.net_sent_bytes || 0, recv: sys.net_recv_bytes || 0 };

    // GPU
    const gpuBar = document.getElementById('gpu-bar');
    if (sys.gpus && sys.gpus.length) {
      gpuBar.innerHTML = sys.gpus.map((g, i) =>
        '<div class="gpu-chip"><div>GPU ' + i + '</div>' +
        '<div class="pct" style="color:' + (g.util > 80 ? '#3fb950' : g.util > 20 ? '#d29922' : '#8b949e') + '">' +
        g.util + '%</div>' +
        '<div>' + g.mem_used + '/' + g.mem_total + 'MB</div></div>'
      ).join('');
    }

    // services
    const svcs = data.services || [];
    if (svcs.length) {
      let svcHtml = '';
      let lastLang = '';
      svcs.forEach(s => {
        // section divider
        const isInfra = s.lang === 'infra';
        const section = isInfra ? 'infra' : 'app';
        if (section !== lastLang) {
          const label = isInfra ? '基础设施' : '业务微服务';
          svcHtml += '<tr class="svc-divider"><td colspan="3">' + label + '</td></tr>';
          lastLang = section;
        }
        const dot = s.status === 'up'
          ? '<span style="color:#3fb950">●</span>'
          : s.status === 'error'
            ? '<span style="color:#d29922">●</span>'
            : '<span style="color:#f85149">●</span>';
        const badge = s.status === 'up'
          ? '<span class="badge done">UP</span>'
          : s.status === 'error'
            ? '<span class="badge" style="background:#d29922;color:#fff">ERR</span>'
            : '<span class="badge failed">DOWN</span>';
        const detail = s.detail
          ? '<div class="svc-detail">' + s.detail + '</div>'
          : '';
        svcHtml += '<tr>' +
          '<td>' + dot + ' <span class="svc-name">' + s.name + '</span>' +
            (!isInfra ? '<br><span style="color:#484f58;font-size:10px">' + s.lang + '</span>' : '') +
          '</td>' +
          '<td><span class="svc-port">:' + s.port + '</span> ' + badge + '</td>' +
          '<td>' + s.desc + detail + '</td>' +
          '</tr>';
      });
      document.getElementById('svc-table').innerHTML = svcHtml;
    }

    // history
    const hist = data.history || [];
    if (hist.length) {
      document.getElementById('history-table').innerHTML = hist.reverse().map(h => {
        const badge = h.status === 'done'
          ? '<span class="badge done">DONE</span>'
          : h.status.startsWith('download')
            ? '<span class="badge running">' + h.status + '</span>'
            : '<span class="badge idle">' + h.status + '</span>';
        return '<tr><td>batch ' + h.batch_id + '</td><td>' + badge + '</td>' +
               '<td>' + fmt(h.images) + ' img</td><td>' + h.time + '</td></tr>';
      }).join('');
    }

    // log
    const logBox = document.getElementById('log-box');
    logBox.textContent = logData.log || '(empty)';
    logBox.scrollTop = logBox.scrollHeight;

  } catch (e) {
    console.error('refresh error', e);
  }
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>"""


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=9800)
    parser.add_argument("--host", default="0.0.0.0")
    args = parser.parse_args()

    print(f"Pipeline Monitor: http://0.0.0.0:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
