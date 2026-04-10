#!/usr/bin/env python3
"""
H4 任务执行监控页面 (替换 9800 端口)
- 5 秒自动刷新
- 移动端友好
- 显示: 任务进度 + Milvus 状态 + 系统资源 + 当前运行的脚本
"""
import os
import time
import json
import glob
import subprocess
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn


app = FastAPI()


def get_milvus_status():
    """获取 Milvus collections 状态"""
    try:
        from pymilvus import connections, utility, Collection
        connections.connect("status", host="localhost", port="19530", timeout=3)
        collections = []
        total = 0
        for c in sorted(utility.list_collections()):
            try:
                col = Collection(c, using="status")
                n = col.num_entities
                total += n
                collections.append({"name": c, "count": n})
            except Exception:
                collections.append({"name": c, "count": -1})
        connections.disconnect("status")
        return {"collections": collections, "total": total}
    except Exception as e:
        return {"error": str(e)[:100]}


def get_system_resources():
    """RAM, 磁盘, GPU"""
    info = {}
    # RAM
    try:
        with open("/proc/meminfo") as f:
            mem = {}
            for line in f:
                k, v = line.split(":")
                mem[k] = int(v.split()[0])
        total = mem["MemTotal"] / 1024 / 1024
        avail = mem["MemAvailable"] / 1024 / 1024
        info["ram"] = {
            "total_gb": total,
            "used_gb": total - avail,
            "percent": (1 - avail / total) * 100
        }
    except Exception:
        info["ram"] = {}

    # 磁盘
    try:
        result = subprocess.run(["df", "-BG", "/data"], capture_output=True, text=True, timeout=2)
        lines = result.stdout.strip().split("\n")
        if len(lines) > 1:
            parts = lines[1].split()
            info["disk"] = {
                "total": parts[1],
                "used": parts[2],
                "avail": parts[3],
                "percent": parts[4],
            }
    except Exception:
        info["disk"] = {}

    # GPU
    try:
        result = subprocess.run(
            ["docker", "exec", "gpu_worker", "nvidia-smi",
             "--query-gpu=index,utilization.gpu,memory.used,memory.total",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=3
        )
        gpus = []
        for line in result.stdout.strip().split("\n"):
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 4:
                gpus.append({
                    "idx": int(parts[0]),
                    "util": int(parts[1]),
                    "mem_used": int(parts[2]),
                    "mem_total": int(parts[3]),
                })
        info["gpus"] = gpus
    except Exception:
        info["gpus"] = []

    return info


def get_h4_tasks():
    """读 Claude task list 状态 (从最近会话快照)"""
    # 静态任务清单 (从 H4 设计)
    return [
        {"id": "H4-1", "subject": "备份旧元数据", "depends": []},
        {"id": "H4-2", "subject": "删除旧 Collections", "depends": ["H4-1"]},
        {"id": "H4-3", "subject": "创建 H4 双向量 schema", "depends": []},
        {"id": "H4-4", "subject": "写双模型推理脚本", "depends": []},
        {"id": "H4-5", "subject": "100万双模型推理 PoC", "depends": ["H4-3", "H4-4"]},
        {"id": "H4-6", "subject": "导入到 H4 Collection", "depends": ["H4-5"]},
        {"id": "H4-7", "subject": "双向量评测 (6场景+语义)", "depends": ["H4-6"]},
        {"id": "H4-8", "subject": "多模式搜索服务", "depends": ["H4-6"]},
    ]


def get_running_processes():
    """检测当前在跑的关键脚本"""
    result = []
    try:
        # 主机进程
        ps = subprocess.run(["ps", "-eo", "pid,etime,cmd"], capture_output=True, text=True, timeout=2)
        for line in ps.stdout.strip().split("\n"):
            line_lower = line.lower()
            for keyword in ["infer_dinov2", "infer_clip", "infer_dual", "import_h4",
                            "run_g6_g10", "run_g2_g10", "download_test", "backup_meta",
                            "h4_search", "eval_h"]:
                if keyword in line_lower and "grep" not in line_lower:
                    parts = line.strip().split(None, 2)
                    if len(parts) >= 3:
                        result.append({
                            "pid": parts[0],
                            "elapsed": parts[1],
                            "cmd": parts[2][:120],
                            "where": "host",
                        })
                    break

        # gpu_worker 容器内进程
        ps2 = subprocess.run(["docker", "exec", "gpu_worker", "ps", "-eo", "pid,etime,cmd"],
                             capture_output=True, text=True, timeout=2)
        for line in ps2.stdout.strip().split("\n"):
            line_lower = line.lower()
            for keyword in ["infer_dinov2", "infer_clip", "infer_dual", "infer_gpu",
                            "h4_search", "h3_search", "eval_"]:
                if keyword in line_lower and "grep" not in line_lower:
                    parts = line.strip().split(None, 2)
                    if len(parts) >= 3:
                        result.append({
                            "pid": parts[0],
                            "elapsed": parts[1],
                            "cmd": parts[2][:120],
                            "where": "gpu_worker",
                        })
                    break
    except Exception:
        pass
    return result


def get_recent_logs():
    """读最近的任务日志"""
    log_files = sorted(
        glob.glob("/data/imgsrch/task_logs/*.log"),
        key=os.path.getmtime,
        reverse=True
    )[:5]

    logs = []
    for f in log_files:
        try:
            mtime = os.path.getmtime(f)
            age_sec = time.time() - mtime
            # 只看最近 2 小时活跃的
            if age_sec > 7200:
                continue
            with open(f, "rb") as fh:
                fh.seek(0, 2)
                size = fh.tell()
                fh.seek(max(0, size - 4096))
                content = fh.read().decode("utf-8", errors="replace")
                lines = content.strip().split("\n")[-3:]
            logs.append({
                "name": os.path.basename(f),
                "age_min": int(age_sec / 60),
                "tail": "\n".join(lines),
            })
        except Exception:
            pass
    return logs


def get_test_dinov2_progress():
    """如果在跑 DINOv2 推理, 看进度"""
    progress = {}
    for outdir in ["/data/imgsrch/test_dinov2", "/data/imgsrch/test_h4"]:
        if os.path.isdir(outdir):
            total = 0
            for f in glob.glob(os.path.join(outdir, "*.jsonl")):
                try:
                    # 用 wc -l 估计 (快速)
                    result = subprocess.run(["wc", "-l", f], capture_output=True, text=True, timeout=2)
                    cnt = int(result.stdout.split()[0])
                    total += cnt
                except Exception:
                    pass
            if total > 0:
                progress[os.path.basename(outdir)] = total
    return progress


@app.get("/")
async def root():
    milvus = get_milvus_status()
    system = get_system_resources()
    procs = get_running_processes()
    logs = get_recent_logs()
    inference = get_test_dinov2_progress()
    tasks = get_h4_tasks()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 构造 HTML
    html = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="refresh" content="5">
<title>H4 任务监控</title>
<style>
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", monospace;
    background: #1a1a2e;
    color: #eee;
    margin: 0;
    padding: 8px;
    font-size: 13px;
  }}
  .header {{
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 12px;
    border-radius: 6px;
    margin-bottom: 10px;
    text-align: center;
  }}
  .header h1 {{ margin: 0; font-size: 18px; color: white; }}
  .header .ts {{ font-size: 11px; opacity: 0.85; margin-top: 4px; color: white; }}
  .card {{
    background: #16213e;
    border-radius: 6px;
    padding: 10px;
    margin-bottom: 8px;
    border-left: 3px solid #667eea;
  }}
  .card h2 {{
    margin: 0 0 8px 0;
    font-size: 14px;
    color: #f0a500;
  }}
  table {{
    width: 100%;
    font-size: 12px;
    border-collapse: collapse;
  }}
  th, td {{
    text-align: left;
    padding: 4px 6px;
    border-bottom: 1px solid #2a3e5a;
  }}
  th {{ color: #88c8ff; }}
  .num {{ text-align: right; font-family: monospace; }}
  .green {{ color: #4ade80; }}
  .yellow {{ color: #fbbf24; }}
  .red {{ color: #f87171; }}
  .gray {{ color: #94a3b8; }}
  .blue {{ color: #60a5fa; }}
  .progress {{
    background: #0f1d36;
    height: 6px;
    border-radius: 3px;
    overflow: hidden;
    margin-top: 4px;
  }}
  .progress-fill {{
    background: linear-gradient(90deg, #4ade80, #22c55e);
    height: 100%;
  }}
  pre {{
    background: #0f1d36;
    padding: 6px;
    border-radius: 4px;
    font-size: 10px;
    overflow-x: auto;
    margin: 4px 0;
    color: #d1d5db;
  }}
  .small {{ font-size: 10px; opacity: 0.7; }}
</style>
</head>
<body>
<div class="header">
  <h1>🚀 H4 双向量任务监控</h1>
  <div class="ts">{now} · 5秒自动刷新</div>
</div>
"""

    # === Milvus 状态 ===
    html += '<div class="card"><h2>📦 Milvus Collections</h2>'
    if "error" in milvus:
        html += f'<span class="red">连接失败: {milvus["error"]}</span>'
    else:
        html += '<table><tr><th>Collection</th><th class="num">Entities</th></tr>'
        for c in milvus["collections"]:
            cls = "blue" if "h4" in c["name"] else "gray"
            html += f'<tr><td class="{cls}">{c["name"]}</td><td class="num">{c["count"]:,}</td></tr>'
        html += f'<tr><td><b>TOTAL</b></td><td class="num green"><b>{milvus["total"]:,}</b></td></tr>'
        html += '</table>'
    html += '</div>'

    # === H4 任务清单 ===
    html += '<div class="card"><h2>📋 H4 任务清单 (8 个)</h2>'
    html += '<table><tr><th>ID</th><th>任务</th><th>依赖</th></tr>'
    for t in tasks:
        deps = ",".join(t["depends"]) if t["depends"] else "-"
        html += f'<tr><td class="blue">{t["id"]}</td><td>{t["subject"]}</td><td class="small gray">{deps}</td></tr>'
    html += '</table>'
    html += '<div class="small gray">详细状态请用 Claude TaskList 查看</div>'
    html += '</div>'

    # === 当前运行进程 ===
    html += '<div class="card"><h2>⚙️ 运行中进程</h2>'
    if not procs:
        html += '<span class="gray">(空闲)</span>'
    else:
        html += '<table><tr><th>位置</th><th>PID</th><th>耗时</th><th>命令</th></tr>'
        for p in procs:
            where_cls = "yellow" if p["where"] == "gpu_worker" else "blue"
            html += f'<tr><td class="{where_cls}">{p["where"]}</td><td>{p["pid"]}</td><td>{p["elapsed"]}</td><td class="small">{p["cmd"]}</td></tr>'
        html += '</table>'
    html += '</div>'

    # === 推理输出进度 ===
    if inference:
        html += '<div class="card"><h2>🧠 推理输出进度</h2><table>'
        html += '<tr><th>目录</th><th class="num">条数</th></tr>'
        for k, v in inference.items():
            html += f'<tr><td class="blue">{k}</td><td class="num green">{v:,}</td></tr>'
        html += '</table></div>'

    # === 系统资源 ===
    html += '<div class="card"><h2>💻 系统资源</h2>'

    if system.get("ram"):
        ram = system["ram"]
        ram_color = "green" if ram["percent"] < 50 else ("yellow" if ram["percent"] < 80 else "red")
        html += f'<div>RAM: <span class="{ram_color}">{ram["used_gb"]:.0f} / {ram["total_gb"]:.0f} GB ({ram["percent"]:.0f}%)</span></div>'
        html += f'<div class="progress"><div class="progress-fill" style="width:{ram["percent"]:.0f}%"></div></div>'

    if system.get("disk"):
        disk = system["disk"]
        html += f'<div style="margin-top:6px">Disk: <span class="green">{disk.get("used","")} / {disk.get("total","")} ({disk.get("percent","")})</span></div>'

    if system.get("gpus"):
        html += '<div style="margin-top:8px"><b>GPU:</b></div>'
        html += '<table><tr><th>#</th><th>Util</th><th>VRAM</th></tr>'
        for g in system["gpus"]:
            util_color = "green" if g["util"] > 30 else ("yellow" if g["util"] > 5 else "gray")
            mem_pct = g["mem_used"] / g["mem_total"] * 100
            html += f'<tr><td>{g["idx"]}</td><td class="{util_color}">{g["util"]}%</td><td>{g["mem_used"]:,} / {g["mem_total"]:,} MB ({mem_pct:.0f}%)</td></tr>'
        html += '</table>'

    html += '</div>'

    # === 最近日志 ===
    if logs:
        html += '<div class="card"><h2>📜 最近日志 (2小时内活跃)</h2>'
        for log in logs:
            html += f'<div><b class="blue">{log["name"]}</b> <span class="small gray">({log["age_min"]}分钟前)</span></div>'
            html += f'<pre>{log["tail"]}</pre>'
        html += '</div>'

    # 备份/帮助信息
    html += '''
<div class="card" style="border-left-color: #f0a500">
  <h2>💡 提示</h2>
  <div class="small">
    • 5秒自动刷新, 移动端可正常查看<br>
    • 任务详细状态请通过 Claude 终端查看 (TaskList)<br>
    • 故障排查: tail -f /data/imgsrch/task_logs/<log_name>.log<br>
    • 当前会话: H4 双向量重建 (DINOv2 + CLIP)
  </div>
</div>
'''

    html += "</body></html>"
    return HTMLResponse(content=html)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "h4-status", "time": datetime.now().isoformat()}


@app.get("/api/status")
async def api_status():
    """JSON API for programmatic access"""
    return {
        "milvus": get_milvus_status(),
        "system": get_system_resources(),
        "processes": get_running_processes(),
        "inference": get_test_dinov2_progress(),
        "tasks": get_h4_tasks(),
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9800, log_level="warning")
