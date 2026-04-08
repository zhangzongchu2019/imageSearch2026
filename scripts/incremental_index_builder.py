#!/usr/bin/env python3
"""
增量索引构建器: 每次只提交 1 个标量索引，等完成后再提交下一个。
避免 standalone 模式下 indexing task 风暴导致 etcd 超时崩溃。

Usage:
    python3 scripts/incremental_index_builder.py
    python3 scripts/incremental_index_builder.py --collections img_202604_vip img_202604_svip
"""
import argparse
import logging
import subprocess
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

SCALAR_FIELDS = [
    'category_l1', 'category_l2', 'category_l3', 'color_code',
    'material_code', 'style_code', 'season_code', 'is_evergreen', 'tags', 'ts_month',
]

CHECK_INTERVAL = 30  # 每 30 秒检查一次索引是否建完
INDEX_TIMEOUT = 3600  # 每个索引最多等 60 分钟 (22M 行 × 几十个 segment, standalone 1 slot)
MILVUS_RESTART_WAIT = 45


def ensure_milvus_running():
    """确保 Milvus 在运行，崩了就重启"""
    for attempt in range(5):
        try:
            r = subprocess.run(
                ["docker", "inspect", "imgsrch-milvus", "--format", "{{.State.Status}}"],
                capture_output=True, text=True, timeout=10)
            status = r.stdout.strip()
            if status == "running":
                return True
            log.warning(f"Milvus status: {status}, restarting... (attempt {attempt+1})")
            subprocess.run(["docker", "restart", "imgsrch-milvus"], timeout=30)
            time.sleep(MILVUS_RESTART_WAIT)
        except Exception as e:
            log.error(f"Error checking Milvus: {e}")
            time.sleep(10)
    return False


def get_indexes(collection_name):
    """获取 collection 当前索引列表"""
    try:
        from pymilvus import connections, Collection
        try:
            connections.connect("default", host="localhost", port=19530, timeout=15)
        except:
            pass
        coll = Collection(collection_name)
        return {idx.field_name for idx in coll.indexes}
    except Exception as e:
        log.error(f"Failed to get indexes for {collection_name}: {e}")
        return None


def submit_and_wait_index(collection_name, field_name):
    """同步提交 1 个标量索引，阻塞直到所有 segment 建完。
    create_index 同步模式会等 Milvus 完成全部 segment 的索引构建。
    """
    from pymilvus import connections, Collection
    try:
        connections.connect("default", host="localhost", port=19530, timeout=15)
    except:
        pass

    coll = Collection(collection_name)

    # 先检查是否已有
    existing = {idx.field_name for idx in coll.indexes}
    if field_name in existing:
        log.info(f"[{collection_name}] ✅ 索引已存在: {field_name} ({len(existing)}/11)")
        return True

    log.info(f"[{collection_name}] 提交索引 (同步): {field_name}")
    t0 = time.time()
    try:
        # 同步模式: 阻塞直到所有 segment 构建完成
        coll.create_index(field_name, {"index_type": "INVERTED"}, timeout=INDEX_TIMEOUT)
        elapsed = time.time() - t0
        log.info(f"[{collection_name}] ✅ 索引完成: {field_name} ({elapsed:.0f}s)")
        return True
    except Exception as e:
        elapsed = time.time() - t0
        err_str = str(e)
        if "already exist" in err_str.lower() or "index already" in err_str.lower():
            log.info(f"[{collection_name}] ✅ 索引已存在: {field_name}")
            return True
        log.error(f"[{collection_name}] 索引 {field_name} 失败 ({elapsed:.0f}s): {e}")
        return False


def build_all_indexes(collection_name):
    """逐个构建所有缺失的索引"""
    log.info(f"=== [{collection_name}] 开始增量索引构建 ===")

    if not ensure_milvus_running():
        log.error("Milvus 无法启动")
        return

    existing = get_indexes(collection_name)
    if existing is None:
        log.error("无法获取索引列表")
        return

    log.info(f"[{collection_name}] 当前索引: {len(existing)}/11 — {existing}")

    # 先确保 HNSW
    if "global_vec" not in existing:
        log.info(f"[{collection_name}] 缺少 HNSW, 同步构建...")
        from pymilvus import connections, Collection
        try:
            connections.connect("default", host="localhost", port=19530, timeout=30)
        except:
            pass
        coll = Collection(collection_name)
        t0 = time.time()
        coll.create_index("global_vec", {
            "index_type": "HNSW", "metric_type": "COSINE",
            "params": {"M": 24, "efConstruction": 200}
        }, timeout=INDEX_TIMEOUT)
        log.info(f"[{collection_name}] ✅ HNSW 构建完成 ({time.time()-t0:.0f}s)")
        existing.add("global_vec")

    # 逐个构建标量索引 (同步模式, 每次只占 1 个 slot)
    for field in SCALAR_FIELDS:
        if field in existing:
            log.info(f"[{collection_name}] 跳过已有: {field}")
            continue

        # 确保 Milvus 健康
        if not ensure_milvus_running():
            log.error("Milvus 无法恢复, 中断")
            return

        # 同步提交并等待完成
        success = submit_and_wait_index(collection_name, field)
        if not success:
            # 失败: 重启 Milvus 后重试一次
            log.warning(f"[{collection_name}] {field} 失败, 重启 Milvus 重试")
            time.sleep(30)
            ensure_milvus_running()
            time.sleep(10)
            success = submit_and_wait_index(collection_name, field)
            if not success:
                log.error(f"[{collection_name}] {field} 重试仍失败, 跳过")
                continue

        # 建完后短暂休息, 让 etcd 喘口气
        time.sleep(5)

    # 最终确认
    final = get_indexes(collection_name)
    log.info(f"=== [{collection_name}] 索引构建完成: {len(final) if final else '?'}/11 ===")


def main():
    parser = argparse.ArgumentParser(description="增量索引构建器")
    parser.add_argument("--collections", nargs="+",
                        default=["img_202604_vip", "img_202604_svip"])
    args = parser.parse_args()

    log.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 增量索引构建器启动")
    log.info(f"目标 Collections: {args.collections}")

    for cname in args.collections:
        build_all_indexes(cname)

    log.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 全部完成")


if __name__ == "__main__":
    main()
