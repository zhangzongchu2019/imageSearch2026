#!/usr/bin/env python3
"""任务清单更新工具，供其他脚本调用"""
import json
import os

TASK_FILE = "/data/imgsrch/task_logs/task_list.json"


def update_task(task_id: int, status: str = None, eta: str = None, name: str = None):
    """更新任务状态"""
    if not os.path.exists(TASK_FILE):
        return
    with open(TASK_FILE) as f:
        tasks = json.load(f)
    for t in tasks:
        if t["id"] == task_id:
            if status:
                t["status"] = status
            if eta:
                t["eta"] = eta
            if name:
                t["name"] = name
            break
    with open(TASK_FILE, "w") as f:
        json.dump(tasks, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 3:
        tid = int(sys.argv[1])
        status = sys.argv[2]
        eta = sys.argv[3] if len(sys.argv) > 3 else None
        update_task(tid, status, eta)
        print(f"Task {tid} → {status}")
    else:
        print("Usage: python3 task_helper.py <task_id> <status> [eta]")
