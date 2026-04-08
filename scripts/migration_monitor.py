#!/usr/bin/env python3
"""
监控迁移进度，自动更新 task_list.json 中 5a/5b/5c/5d 状态。
每 30 秒检查一次。
"""
import json
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

TASK_FILE = "/data/imgsrch/task_logs/task_list.json"
INTERVAL = 30
MAX_ROUNDS = 400  # ~3.3h


def get_collection_info(name):
    """返回 (exists, num_entities, num_indexes)"""
    try:
        from pymilvus import connections, Collection, utility
        try:
            connections.connect("default", host="localhost", port=19530, timeout=10)
        except:
            pass
        if not utility.has_collection(name):
            return False, 0, 0
        c = Collection(name)
        return True, c.num_entities, len(c.indexes)
    except:
        return False, 0, 0


def update_tasks(tasks, task_id, status, eta):
    for t in tasks:
        if str(t["id"]) == str(task_id):
            t["status"] = status
            t["eta"] = eta
            return


def main():
    log.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 迁移监控启动")

    for _ in range(MAX_ROUNDS):
        try:
            with open(TASK_FILE, "r") as f:
                tasks = json.load(f)

            # Check VIP collection
            vip_exists, vip_count, vip_idx = get_collection_info("img_202604_vip")
            svip_exists, svip_count, svip_idx = get_collection_info("img_202604_svip")

            VIP_TARGET = 20_131_226
            SVIP_TARGET = 19_999_332

            # 5a: VIP 迁移
            if vip_count >= VIP_TARGET - 1000:
                update_tasks(tasks, "5a", "done", f"✅ {vip_count:,} 条")
            elif vip_count > 0:
                pct = vip_count * 100 / VIP_TARGET
                update_tasks(tasks, "5a", "running", f"{vip_count:,}/{VIP_TARGET:,} ({pct:.1f}%)")
            else:
                if vip_exists:
                    update_tasks(tasks, "5a", "running", "写入中...")

            # 5b: SVIP 迁移
            if svip_count >= SVIP_TARGET - 1000:
                update_tasks(tasks, "5b", "done", f"✅ {svip_count:,} 条")
            elif svip_count > 0:
                pct = svip_count * 100 / SVIP_TARGET
                update_tasks(tasks, "5b", "running", f"{svip_count:,}/{SVIP_TARGET:,} ({pct:.1f}%)")
            elif vip_count >= VIP_TARGET - 1000:
                update_tasks(tasks, "5b", "running", "等待开始...")

            # 5c: VIP 索引
            if vip_idx >= 11:
                update_tasks(tasks, "5c", "done", f"✅ {vip_idx}/11 索引")
            elif vip_idx > 0:
                update_tasks(tasks, "5c", "running", f"{vip_idx}/11 索引构建中")
            elif vip_count >= VIP_TARGET - 1000:
                update_tasks(tasks, "5c", "running", "索引构建中...")

            # 5d: SVIP 索引
            if svip_idx >= 11:
                update_tasks(tasks, "5d", "done", f"✅ {svip_idx}/11 索引")
            elif svip_idx > 0:
                update_tasks(tasks, "5d", "running", f"{svip_idx}/11 索引构建中")
            elif svip_count >= SVIP_TARGET - 1000:
                update_tasks(tasks, "5d", "running", "索引构建中...")

            with open(TASK_FILE, "w") as f:
                json.dump(tasks, f, ensure_ascii=False, indent=2)

            # Check if all 4 sub-tasks done
            all_done = (vip_count >= VIP_TARGET - 1000 and svip_count >= SVIP_TARGET - 1000
                        and vip_idx >= 11 and svip_idx >= 11)
            if all_done:
                log.info(f"[{time.strftime('%H:%M:%S')}] ✅ 全部迁移子任务完成!")
                break

            log.info(f"[{time.strftime('%H:%M:%S')}] VIP: {vip_count:,} idx={vip_idx} | "
                     f"SVIP: {svip_count:,} idx={svip_idx}")

        except Exception as e:
            log.error(f"Monitor error: {e}")

        time.sleep(INTERVAL)

    log.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 迁移监控结束")


if __name__ == "__main__":
    main()
