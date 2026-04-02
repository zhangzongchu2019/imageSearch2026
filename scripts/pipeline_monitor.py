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
TOTAL_TARGET = 100_000_000  # 1 亿目标


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
        from pymilvus import connections, Collection
        connections.connect("default", host="localhost", port=19530, timeout=3)
        coll = Collection("global_images_hot")
        stats["milvus"] = coll.num_entities
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
        ("etcd",         2379,  "tcp",  "Milvus 元数据"),
        ("ClickHouse",   8123,  "tcp",  "分析型存储"),
        ("Prometheus",   9099,  "tcp",  "监控采集"),
        ("Grafana",      3002,  "tcp",  "监控面板"),
    ]
    for name, port, proto, desc in infra_checks:
        entry = {"name": name, "port": port, "lang": "infra", "desc": desc, "status": "down", "detail": ""}
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            if s.connect_ex(("localhost", port)) == 0:
                entry["status"] = "up"
            s.close()
        except Exception:
            pass
        services.append(entry)

    # ── 数据量补充 (Milvus 分区 / PG 表) ──
    try:
        from pymilvus import connections, Collection
        connections.connect("default", host="localhost", port=19530, timeout=3)
        coll = Collection("global_images_hot")
        partitions_info = []
        for p in coll.partitions:
            if p.num_entities > 0:
                partitions_info.append(f"{p.name}={p.num_entities:,}")
        for svc in services:
            if svc["name"] == "Milvus":
                svc["detail"] = f"global_images_hot: {coll.num_entities:,} | " + ", ".join(partitions_info)
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


# ── API ──

@app.get("/api/status")
async def api_status():
    state = get_pipeline_state()
    running = get_running_batch()
    db = get_db_stats()
    sys_stats = get_system_stats()
    history = get_batch_history()
    services = get_services_status()

    progress_pct = db.get("milvus", 0) / TOTAL_TARGET * 100 if TOTAL_TARGET > 0 else 0

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline": state,
        "running": running,
        "db": db,
        "system": sys_stats,
        "history": history,
        "services": services,
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
    <span class="stat-value" id="v-target">100,000,000</span>
  </div>
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

<div class="refresh-info">每 10 秒自动刷新</div>

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

    // system
    const sys = data.system || {};
    document.getElementById('v-cpu').textContent = (sys.cpu_pct || 0) + '%';
    document.getElementById('v-mem').textContent =
      (sys.mem_used_gb || 0) + ' / ' + (sys.mem_total_gb || 0) + ' GB (' + (sys.mem_pct || 0) + '%)';
    document.getElementById('v-disk').textContent =
      (sys.disk_used_gb || 0) + ' / ' + (sys.disk_total_gb || 0) + ' GB (空余 ' + (sys.disk_free_gb || 0) + 'GB)';

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
setInterval(refresh, 10000);
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
