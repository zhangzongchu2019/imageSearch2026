#!/usr/bin/env python3
"""轻量监控页 — 实时任务进度 + 系统状态"""
import json, os, time, subprocess, glob, re
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn

app = FastAPI()

def get_l2_progress():
    """读 L2 更新日志的最新进度"""
    # 检查所有相关日志: update_l2, inherit_l2, 取最新的在跑的
    all_logs = (glob.glob("/data/imgsrch/task_logs/update_l2*.log") +
                glob.glob("/data/imgsrch/task_logs/inherit_l2*.log"))
    if not all_logs: return {}
    logs = sorted(all_logs, key=os.path.getmtime)
    # 从最新的日志开始找进度
    for logfile in reversed(logs):
        try:
            with open(logfile, "rb") as f:
                f.seek(0, 2)
                f.seek(max(0, f.tell() - 8192))
                lines = f.read().decode("utf-8", errors="replace").splitlines()
            for line in reversed(lines):
                # 格式1: [done/total] rate/s ok=X l2=X fail=X ETA=Xmin
                m = re.search(r'\[(\d[\d,]+)/(\d[\d,]+)\]\s+([\d.]+)/s\s+ok=([\d,]+)\s+l2=([\d,]+)\s+fail=([\d,]+)\s+ETA=(\d+)', line)
                if m:
                    return {
                        "done": int(m.group(1).replace(",","")),
                        "total": int(m.group(2).replace(",","")),
                        "rate": float(m.group(3)),
                        "ok": int(m.group(4).replace(",","")),
                        "l2": int(m.group(5).replace(",","")),
                        "fail": int(m.group(6).replace(",","")),
                        "eta_min": int(m.group(7)),
                        "log": os.path.basename(logfile),
                    }
                # 格式2: [done/total] rate/s updated=X fail=X ETA=Xmin (inherit_l2)
                m2 = re.search(r'\[(\d[\d,]+)/(\d[\d,]+)\]\s+(\d+)/s\s+updated=([\d,]+)\s+fail=([\d,]+)\s+ETA=(\d+)', line)
                if m2:
                    return {
                        "done": int(m2.group(1).replace(",","")),
                        "total": int(m2.group(2).replace(",","")),
                        "rate": float(m2.group(3)),
                        "ok": int(m2.group(4).replace(",","")),
                        "l2": int(m2.group(4).replace(",","")),
                        "fail": int(m2.group(5).replace(",","")),
                        "eta_min": int(m2.group(6)),
                        "log": os.path.basename(logfile),
                    }
                # 完成行
                if "=== 完成" in line or "=== Done" in line:
                    return {"done": 1, "total": 1, "rate": 0, "ok": 0, "l2": 0, "fail": 0, "eta_min": 0, "log": os.path.basename(logfile) + " (完成)"}
        except: pass
    return {}

def get_tasks():
    try:
        with open("/data/imgsrch/task_logs/task_list.json") as f:
            return json.load(f)
    except: return []

def get_system():
    stats = {}
    try:
        import psutil
        stats["cpu"] = psutil.cpu_percent(interval=0.5)
        m = psutil.virtual_memory()
        stats["mem_used_gb"] = round(m.used / 1024**3)
        stats["mem_total_gb"] = round(m.total / 1024**3)
    except: pass
    try:
        r = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True, timeout=3)
        mvs = [n for n in r.stdout.strip().split("\n") if "imgsrch-mvs" in n]
        stats["milvus_nodes"] = len(mvs)
    except: pass
    return stats

@app.get("/api/status")
async def api_status():
    return {
        "time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "l2": get_l2_progress(),
        "tasks": get_tasks(),
        "system": get_system(),
    }

@app.get("/", response_class=HTMLResponse)
async def index():
    return """<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>任务监控</title><style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,sans-serif;background:#0d1117;color:#e6edf3;padding:12px;max-width:800px;margin:0 auto}
h1{font-size:18px;color:#58a6ff;margin-bottom:12px;text-align:center}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px;margin-bottom:10px}
.card h2{font-size:12px;color:#8b949e;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px}
.bar{width:100%;height:28px;background:#21262d;border-radius:14px;overflow:hidden;position:relative;margin:6px 0}
.fill{height:100%;background:linear-gradient(90deg,#238636,#3fb950);border-radius:14px;transition:width 1s;min-width:2px}
.bar-text{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);font-size:12px;font-weight:600;color:#fff;text-shadow:0 1px 2px rgba(0,0,0,.5)}
.stat{display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #21262d;font-size:13px}
.stat:last-child{border:none}
.label{color:#8b949e}
.val{font-weight:600;font-variant-numeric:tabular-nums}
.green{color:#3fb950}.blue{color:#58a6ff}.yellow{color:#d29922}.red{color:#f85149}
.task{display:flex;align-items:center;padding:5px 0;border-bottom:1px solid #21262d;font-size:12px}
.task:last-child{border:none}
.task-done{color:#8b949e;text-decoration:line-through}
.task-run{color:#9ed93f;font-weight:600;background:rgba(158,217,63,.08);padding:4px 6px;border-radius:4px;border-left:3px solid #9ed93f}
.ts{font-size:11px;color:#484f58;text-align:center;margin-top:6px}
</style></head><body>
<h1>&#x1F4CA; 任务监控</h1>
<div class="card" id="l2card">
<h2>Task 17: L2 品类补全 (3 GPU)</h2>
<div class="bar"><div class="fill" id="l2bar"></div><div class="bar-text" id="l2pct">-</div></div>
<div class="stat"><span class="label">进度</span><span class="val blue" id="l2prog">-</span></div>
<div class="stat"><span class="label">速度</span><span class="val green" id="l2rate">-</span></div>
<div class="stat"><span class="label">L2 命中</span><span class="val" id="l2hit">-</span></div>
<div class="stat"><span class="label">失败</span><span class="val red" id="l2fail">-</span></div>
<div class="stat"><span class="label">预计完成</span><span class="val yellow" id="l2eta">-</span></div>
</div>
<div class="card">
<h2>系统</h2>
<div class="stat"><span class="label">CPU</span><span class="val" id="cpu">-</span></div>
<div class="stat"><span class="label">内存</span><span class="val" id="mem">-</span></div>
<div class="stat"><span class="label">Milvus 分布式</span><span class="val" id="mvs">-</span></div>
</div>
<div class="card">
<h2>全部任务</h2>
<div id="tasks">加载中...</div>
</div>
<div class="ts" id="ts">-</div>
<script>
function fmt(n){return n?n.toLocaleString():'-'}
async function refresh(){
try{
const r=await fetch(window.location.pathname.replace(/\/$/,'') + '/api/status');const d=await r.json();
document.getElementById('ts').textContent='更新: '+d.time+' (每5秒)';
// L2
const l=d.l2||{};
if(l.total){
const pct=(l.done/l.total*100).toFixed(1);
document.getElementById('l2bar').style.width=pct+'%';
document.getElementById('l2pct').textContent=pct+'%';
document.getElementById('l2prog').textContent=fmt(l.done)+' / '+fmt(l.total);
document.getElementById('l2rate').textContent=l.rate+'/s';
document.getElementById('l2hit').textContent=fmt(l.l2)+' ('+((l.l2/(l.ok||1))*100).toFixed(0)+'%)';
document.getElementById('l2fail').textContent=fmt(l.fail)+' ('+(l.fail/(l.done||1)*100).toFixed(1)+'%)';
const h=Math.floor(l.eta_min/60);const m=l.eta_min%60;
document.getElementById('l2eta').textContent=(h?h+'h':'')+m+'min';
}else{document.getElementById('l2pct').textContent='完成或未运行'}
// system
const s=d.system||{};
document.getElementById('cpu').textContent=(s.cpu||0)+'%';
document.getElementById('mem').textContent=(s.mem_used_gb||0)+'/'+(s.mem_total_gb||0)+' GB';
document.getElementById('mvs').textContent=(s.milvus_nodes||0)+' 个组件';
// tasks
const tl=d.tasks||[];
document.getElementById('tasks').innerHTML=tl.map(t=>{
const cls=t.status==='done'?'task task-done':t.status==='running'?'task task-run':'task';
const icon=t.status==='done'?'&#x2705;':t.status==='running'?'&#x25B6;':'&#x25CB;';
return '<div class="'+cls+'">'+icon+' '+t.id+'. '+t.name+' <span style="margin-left:auto;font-size:10px;color:#484f58">'+
(t.eta||'')+'</span></div>'}).join('');
}catch(e){document.getElementById('ts').textContent='Error: '+e}
}
refresh();setInterval(refresh,5000);
</script></body></html>"""

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9800, log_level="warning")
