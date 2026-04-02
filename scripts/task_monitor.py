#!/usr/bin/env python3
"""
任务执行状态监控页 — 手机友好, 端口 9800

显示两级分类架构重构的所有 Phase 执行状态,
支持实时刷新, 适配手机屏幕。

Usage:
    python3 scripts/task_monitor.py
    # 访问 http://<server-ip>:9800
"""

import json
import os
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

PORT = 9800
TASK_STATE_FILE = "/data/imgsrch/task_logs/task_state.json"
PLAN_FILE = "/home/dev/.claude/plans/crispy-watching-barto.md"

# 任务定义
PHASES = [
    {"id": "phase1", "name": "Phase 1: 品类体系定义", "description": "创建 taxonomy.py, 定义 L1/L2/L3 品类 + 文本 prompt"},
    {"id": "phase2", "name": "Phase 2: inference-service 重构", "description": "双模型加载 (ViT-L-14 + FashionSigLIP), 两级推理"},
    {"id": "phase3", "name": "Phase 3: Docker/部署更新", "description": "Dockerfile, requirements.txt, docker-compose.yml"},
    {"id": "phase4", "name": "Phase 4: batch_import 更新", "description": "CLIPFeatureExtractor 升级到 ViT-L-14 + FashionSigLIP"},
    {"id": "phase5", "name": "Phase 5: vocabulary_mapping SQL", "description": "L1/L2/L3 品类 + 标签 vocabulary 条目"},
    {"id": "phase6", "name": "Phase 6: 上下游服务适配", "description": "write-service 传递 L2/L3, search-service 适配"},
    {"id": "model_download", "name": "模型下载", "description": "ViT-L-14 (~890MB) + FashionSigLIP (~1.2GB)"},
    {"id": "data_migration", "name": "数据迁移 (清空重来)", "description": "清空旧 collection, 用新模型重新导入"},
    {"id": "validation", "name": "L1 准确率验证", "description": "2000 张图片覆盖全品类, 验证 95%+ 准确率"},
]


def load_task_state() -> dict:
    """加载任务状态"""
    if os.path.exists(TASK_STATE_FILE):
        try:
            with open(TASK_STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    # 默认状态 — 根据当前代码提交情况
    return {
        "phase1": {"status": "completed", "updated_at": "2026-04-02 18:00:00", "note": "commit 8c810fc"},
        "phase2": {"status": "completed", "updated_at": "2026-04-02 18:30:00", "note": "main.py 重写完成"},
        "phase3": {"status": "completed", "updated_at": "2026-04-02 18:45:00", "note": "Dockerfile + docker-compose 更新"},
        "phase4": {"status": "completed", "updated_at": "2026-04-02 19:00:00", "note": "batch_import ViT-L-14 适配"},
        "phase5": {"status": "completed", "updated_at": "2026-04-02 19:15:00", "note": "init_db.sql 新增 ~120 条词表"},
        "phase6": {"status": "completed", "updated_at": "2026-04-02 19:30:00", "note": "write/search service 适配"},
        "model_download": {"status": "pending", "updated_at": "", "note": ""},
        "data_migration": {"status": "pending", "updated_at": "", "note": ""},
        "validation": {"status": "pending", "updated_at": "", "note": ""},
    }


def save_task_state(state: dict):
    os.makedirs(os.path.dirname(TASK_STATE_FILE), exist_ok=True)
    with open(TASK_STATE_FILE, "w") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def get_status_emoji(status: str) -> str:
    return {"completed": "&#x2705;", "in_progress": "&#x1F504;", "failed": "&#x274C;", "pending": "&#x23F3;"}.get(status, "&#x2753;")


def get_status_class(status: str) -> str:
    return {"completed": "completed", "in_progress": "in-progress", "failed": "failed", "pending": "pending"}.get(status, "pending")


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>ImgSrch - Task Monitor</title>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family: -apple-system, 'Segoe UI', Roboto, sans-serif; background:#0d1117; color:#c9d1d9; padding:12px; }}
h1 {{ font-size:1.2em; color:#58a6ff; margin-bottom:4px; }}
.subtitle {{ font-size:0.85em; color:#8b949e; margin-bottom:16px; }}
.summary {{ display:flex; gap:12px; margin-bottom:16px; flex-wrap:wrap; }}
.summary-card {{ background:#161b22; border-radius:8px; padding:10px 14px; flex:1; min-width:70px; text-align:center; }}
.summary-card .num {{ font-size:1.6em; font-weight:700; }}
.summary-card .label {{ font-size:0.75em; color:#8b949e; }}
.num.green {{ color:#3fb950; }}
.num.yellow {{ color:#d29922; }}
.num.gray {{ color:#8b949e; }}
.num.red {{ color:#f85149; }}
.task {{ background:#161b22; border-radius:8px; padding:12px; margin-bottom:8px; border-left:3px solid #30363d; }}
.task.completed {{ border-left-color:#3fb950; }}
.task.in-progress {{ border-left-color:#d29922; animation: pulse 2s infinite; }}
.task.failed {{ border-left-color:#f85149; }}
.task-header {{ display:flex; justify-content:space-between; align-items:center; }}
.task-name {{ font-weight:600; font-size:0.95em; }}
.task-status {{ font-size:0.8em; padding:2px 8px; border-radius:4px; }}
.task-desc {{ font-size:0.8em; color:#8b949e; margin-top:4px; }}
.task-meta {{ font-size:0.75em; color:#6e7681; margin-top:6px; }}
.refresh {{ text-align:center; margin-top:16px; }}
.refresh a {{ color:#58a6ff; text-decoration:none; font-size:0.85em; }}
.arch {{ background:#161b22; border-radius:8px; padding:10px; margin-bottom:16px; font-family:monospace; font-size:0.7em; color:#8b949e; white-space:pre; overflow-x:auto; line-height:1.4; }}
@keyframes pulse {{ 0%,100% {{ opacity:1; }} 50% {{ opacity:0.6; }} }}
</style>
</head>
<body>
<h1>ImgSrch Two-Stage Classification</h1>
<p class="subtitle">{timestamp}</p>

<div class="arch">图片 → ViT-L-14 (Stage 1) → 768维
  ├── 投影 768→256 → Milvus
  ├── L1 大类 (18类, 95%+)
  ├── 非时尚 → ViT-L-14 零样本 L2
  └── 时尚类 → FashionSigLIP L2/L3</div>

<div class="summary">
  <div class="summary-card"><div class="num green">{completed}</div><div class="label">完成</div></div>
  <div class="summary-card"><div class="num yellow">{in_progress}</div><div class="label">进行中</div></div>
  <div class="summary-card"><div class="num gray">{pending}</div><div class="label">待执行</div></div>
  <div class="summary-card"><div class="num red">{failed}</div><div class="label">失败</div></div>
</div>

{tasks_html}

<div class="refresh">
  <a href="/" onclick="location.reload();return false;">刷新</a> |
  <a href="/api/state">API JSON</a>
  <br><span style="color:#6e7681;font-size:0.75em;">自动刷新: 30秒</span>
</div>

<script>setTimeout(()=>location.reload(), 30000);</script>
</body>
</html>"""


class MonitorHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/state":
            state = load_task_state()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(state, ensure_ascii=False).encode())
            return

        if self.path.startswith("/api/update"):
            # POST-like via GET for simplicity: /api/update?id=phase1&status=completed&note=done
            from urllib.parse import urlparse, parse_qs
            params = parse_qs(urlparse(self.path).query)
            task_id = params.get("id", [""])[0]
            new_status = params.get("status", [""])[0]
            note = params.get("note", [""])[0]
            if task_id and new_status:
                state = load_task_state()
                state[task_id] = {
                    "status": new_status,
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "note": note,
                }
                save_task_state(state)
            self.send_response(302)
            self.send_header("Location", "/")
            self.end_headers()
            return

        # Main page
        state = load_task_state()
        counts = {"completed": 0, "in_progress": 0, "pending": 0, "failed": 0}
        for p in PHASES:
            s = state.get(p["id"], {}).get("status", "pending")
            counts[s] = counts.get(s, 0) + 1

        tasks_html = ""
        for p in PHASES:
            s = state.get(p["id"], {})
            status = s.get("status", "pending")
            tasks_html += f"""<div class="task {get_status_class(status)}">
  <div class="task-header">
    <span class="task-name">{get_status_emoji(status)} {p['name']}</span>
  </div>
  <div class="task-desc">{p['description']}</div>
  <div class="task-meta">{s.get('updated_at', '')} {s.get('note', '')}</div>
</div>\n"""

        html = HTML_TEMPLATE.format(
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            completed=counts["completed"],
            in_progress=counts["in_progress"],
            pending=counts["pending"],
            failed=counts["failed"],
            tasks_html=tasks_html,
        )
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html.encode())

    def log_message(self, format, *args):
        pass  # suppress access logs


def main():
    # 初始化状态文件
    state = load_task_state()
    save_task_state(state)

    server = HTTPServer(("0.0.0.0", PORT), MonitorHandler)
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Task Monitor running on http://0.0.0.0:{PORT}")
    print(f"  State file: {TASK_STATE_FILE}")
    print(f"  Update API: http://<host>:{PORT}/api/update?id=<phase_id>&status=<status>&note=<note>")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
