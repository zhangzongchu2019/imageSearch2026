#!/usr/bin/env python3
"""
多 Collection 架构 — 全链路查询验证
2026-04-05

验证项:
  1. 连接测试: 连接所有 Collection，确认可 load
  2. 单 Collection 向量搜索: img_202604_vip 随机取向量, top-10
  3. 跨 Collection 搜索: 同一向量搜索所有 Collection, 合并排序
  4. 标量过滤搜索: 向量搜索 + category_l1 过滤
  5. 混合搜索: 向量搜索 + is_evergreen=True (仅 img_999999)
  6. 性能基准: 单次延迟 P50/P99, 连续 100 次搜索
"""
import logging
import sys
import time
from datetime import datetime

from pymilvus import Collection, connections, utility

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("e2e_validate")

# suppress noisy pymilvus RPC error logs during polling
logging.getLogger("handler").setLevel(logging.CRITICAL)

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
MILVUS_HOST = "localhost"
MILVUS_PORT = 19530

COLLECTIONS = [
    "img_999999",
    "img_202604_vip",
    "img_202604_svip",
    "img_202603_vip",
]

# 旧 Collection，已迁移完成，需要 release 以腾出内存
OLD_COLLECTIONS_TO_RELEASE = ["global_images_hot"]

SEARCH_PARAMS = {"metric_type": "COSINE", "params": {"ef": 64}}
TOP_K = 10
PERF_ROUNDS = 100
LOAD_TIMEOUT = 300  # 最长等待 load 完成 5 分钟


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------
def ts():
    return datetime.now().strftime("[%Y-%m-%d %H:%M]")


def fmt_result(passed: bool, name: str, elapsed_ms: float, summary: str) -> str:
    tag = "PASS" if passed else "FAIL"
    return f"[{tag}] {name} — {elapsed_ms:.0f}ms — {summary}"


_connected = False


def connect():
    global _connected
    if not _connected:
        connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT, timeout=30)
        _connected = True


def release_old_collections():
    """Release old collections to free memory for new ones."""
    connect()
    for name in OLD_COLLECTIONS_TO_RELEASE:
        if utility.has_collection(name):
            try:
                progress = utility.loading_progress(name)
                pct = progress.get("loading_progress", "0%")
                if pct != "0%":
                    log.info(f"  释放旧 Collection {name} (loaded={pct}) 以腾出内存...")
                    coll = Collection(name)
                    coll.release()
                    time.sleep(5)  # 等待释放完成
                    log.info(f"  {name} 已释放")
            except Exception as e:
                log.info(f"  {name}: release skipped ({e})")


def ensure_loaded(coll_name: str, timeout: int = LOAD_TIMEOUT) -> Collection:
    """Load collection, polling progress until 100% or timeout."""
    connect()
    coll = Collection(coll_name)

    # 检查是否已加载
    try:
        progress = utility.loading_progress(coll_name)
        pct = progress.get("loading_progress", "0%")
        if pct == "100%":
            return coll
        log.info(f"  {coll_name}: 当前加载进度 {pct}, 触发 load ...")
    except Exception:
        log.info(f"  {coll_name}: 未加载, 触发 load ...")

    # 异步 load
    coll.load(_async=True)

    # 轮询等待
    t0 = time.time()
    last_pct = ""
    while time.time() - t0 < timeout:
        time.sleep(10)
        try:
            progress = utility.loading_progress(coll_name)
            pct = progress.get("loading_progress", "0%")
            if pct == "100%":
                log.info(f"  {coll_name}: 加载完成 100%")
                return coll
            elapsed = int(time.time() - t0)
            if pct != last_pct:
                log.info(f"  {coll_name}: {pct} ({elapsed}s elapsed)")
                last_pct = pct
        except Exception as e:
            elapsed = int(time.time() - t0)
            # OOM 或其他错误 - 报告但继续等待
            err_short = str(e)[:120]
            log.warning(f"  {coll_name}: {err_short} ({elapsed}s elapsed)")

    raise TimeoutError(f"{coll_name} load timeout after {timeout}s")


# ---------------------------------------------------------------------------
# 测试 1: 连接测试
# ---------------------------------------------------------------------------
def test_connection():
    name = "1. 连接测试 (load all Collections)"
    t0 = time.time()
    connect()

    # 先释放旧 Collection 腾出内存
    release_old_collections()

    loaded = []
    errors = []
    for cname in COLLECTIONS:
        try:
            if not utility.has_collection(cname):
                errors.append(f"{cname}: not found")
                continue
            coll = ensure_loaded(cname)
            n = coll.num_entities
            loaded.append(f"{cname}({n:,})")
        except Exception as e:
            errors.append(f"{cname}: {e}")

    elapsed = (time.time() - t0) * 1000
    passed = len(errors) == 0 and len(loaded) == len(COLLECTIONS)
    summary = f"loaded {len(loaded)}/{len(COLLECTIONS)}: {', '.join(loaded)}"
    if errors:
        summary += f" | errors: {errors}"
    msg = fmt_result(passed, name, elapsed, summary)
    log.info(f"{ts()} {msg}")
    return passed


# ---------------------------------------------------------------------------
# 测试 2: 单 Collection 向量搜索
# ---------------------------------------------------------------------------
def test_single_collection_search():
    name = "2. 单 Collection 向量搜索 (img_202604_vip top-10)"
    t0 = time.time()
    connect()
    coll = Collection("img_202604_vip")

    # 随机取一条记录
    sample = coll.query(
        expr="ts_month >= 0",
        output_fields=["image_pk", "global_vec", "category_l1"],
        limit=1,
    )
    if not sample:
        elapsed = (time.time() - t0) * 1000
        msg = fmt_result(False, name, elapsed, "no data to sample")
        log.info(f"{ts()} {msg}")
        return False

    query_vec = sample[0]["global_vec"]
    query_pk = sample[0]["image_pk"]

    t_search = time.time()
    results = coll.search(
        data=[query_vec],
        anns_field="global_vec",
        param=SEARCH_PARAMS,
        limit=TOP_K,
        output_fields=["image_pk", "category_l1"],
    )
    search_ms = (time.time() - t_search) * 1000

    hits = results[0] if results else []
    n_hits = len(hits)
    top1_pk = hits[0].entity.get("image_pk") if n_hits > 0 else None
    top1_score = hits[0].distance if n_hits > 0 else None
    self_hit = (top1_pk == query_pk)

    elapsed = (time.time() - t0) * 1000
    passed = n_hits == TOP_K and self_hit
    summary = (
        f"{n_hits} hits, search {search_ms:.0f}ms, "
        f"top1_pk={'self' if self_hit else top1_pk}, score={top1_score:.4f}"
    )
    msg = fmt_result(passed, name, elapsed, summary)
    log.info(f"{ts()} {msg}")

    # 打印 top-10 详情
    for i, hit in enumerate(hits):
        pk = hit.entity.get("image_pk")
        cat = hit.entity.get("category_l1")
        log.info(f"    #{i+1} score={hit.distance:.4f} pk={pk} cat_l1={cat}")

    return passed, query_vec, query_pk


# ---------------------------------------------------------------------------
# 测试 3: 跨 Collection 搜索
# ---------------------------------------------------------------------------
def test_cross_collection_search(query_vec, query_pk):
    name = "3. 跨 Collection 搜索 (all Collections, merge by score)"
    t0 = time.time()
    connect()

    all_hits = []
    coll_stats = []
    for cname in COLLECTIONS:
        try:
            coll = Collection(cname)
            t_s = time.time()
            results = coll.search(
                data=[query_vec],
                anns_field="global_vec",
                param=SEARCH_PARAMS,
                limit=TOP_K,
                output_fields=["image_pk", "category_l1"],
            )
            s_ms = (time.time() - t_s) * 1000

            hits = results[0] if results else []
            for hit in hits:
                all_hits.append({
                    "collection": cname,
                    "pk": hit.entity.get("image_pk"),
                    "category_l1": hit.entity.get("category_l1"),
                    "score": hit.distance,
                })
            coll_stats.append(f"{cname}:{len(hits)}hits/{s_ms:.0f}ms")
        except Exception as e:
            coll_stats.append(f"{cname}:ERROR({e})")

    # 合并按 score 降序 (COSINE: 越大越相似)
    all_hits.sort(key=lambda x: x["score"], reverse=True)
    merged_top10 = all_hits[:TOP_K]

    elapsed = (time.time() - t0) * 1000
    passed = len(merged_top10) == TOP_K
    colls_in_top10 = set(h["collection"] for h in merged_top10)
    summary = (
        f"merged {len(all_hits)} hits -> top-10 from {colls_in_top10} | "
        f"per-coll: {', '.join(coll_stats)}"
    )
    msg = fmt_result(passed, name, elapsed, summary)
    log.info(f"{ts()} {msg}")

    for i, h in enumerate(merged_top10):
        log.info(
            f"    #{i+1} score={h['score']:.4f} coll={h['collection']} "
            f"pk={h['pk']} cat_l1={h['category_l1']}"
        )

    return passed


# ---------------------------------------------------------------------------
# 测试 4: 标量过滤搜索
# ---------------------------------------------------------------------------
def test_filtered_search(query_vec):
    name = "4. 标量过滤搜索 (向量 + category_l1 过滤)"
    t0 = time.time()
    connect()
    coll = Collection("img_202604_vip")

    # 先查出一个有效的 category_l1 值
    sample = coll.query(
        expr="ts_month >= 0",
        output_fields=["category_l1"],
        limit=1,
    )
    cat_l1 = sample[0]["category_l1"] if sample else ""

    t_search = time.time()
    results = coll.search(
        data=[query_vec],
        anns_field="global_vec",
        param=SEARCH_PARAMS,
        limit=TOP_K,
        expr=f'category_l1 == "{cat_l1}"',
        output_fields=["image_pk", "category_l1"],
    )
    search_ms = (time.time() - t_search) * 1000

    hits = results[0] if results else []
    n_hits = len(hits)

    # 验证所有返回结果的 category_l1 一致
    all_match = all(h.entity.get("category_l1") == cat_l1 for h in hits)

    elapsed = (time.time() - t0) * 1000
    passed = n_hits > 0 and all_match
    summary = (
        f'filter=category_l1=="{cat_l1}", {n_hits} hits, '
        f"all_match={all_match}, search {search_ms:.0f}ms"
    )
    msg = fmt_result(passed, name, elapsed, summary)
    log.info(f"{ts()} {msg}")

    for i, hit in enumerate(hits[:5]):
        log.info(
            f"    #{i+1} score={hit.distance:.4f} "
            f"pk={hit.entity.get('image_pk')} cat={hit.entity.get('category_l1')}"
        )

    return passed


# ---------------------------------------------------------------------------
# 测试 5: 混合搜索 (向量 + is_evergreen, 仅 img_999999)
# ---------------------------------------------------------------------------
def test_hybrid_search(query_vec):
    name = "5. 混合搜索 (向量 + is_evergreen=True, img_999999)"
    t0 = time.time()
    connect()
    coll = Collection("img_999999")

    t_search = time.time()
    results = coll.search(
        data=[query_vec],
        anns_field="global_vec",
        param=SEARCH_PARAMS,
        limit=TOP_K,
        expr="is_evergreen == true",
        output_fields=["image_pk", "category_l1", "is_evergreen"],
    )
    search_ms = (time.time() - t_search) * 1000

    hits = results[0] if results else []
    n_hits = len(hits)

    all_evergreen = all(h.entity.get("is_evergreen") is True for h in hits)

    elapsed = (time.time() - t0) * 1000
    passed = n_hits > 0 and all_evergreen
    summary = (
        f"{n_hits} hits, all_evergreen={all_evergreen}, search {search_ms:.0f}ms"
    )
    msg = fmt_result(passed, name, elapsed, summary)
    log.info(f"{ts()} {msg}")

    for i, hit in enumerate(hits[:5]):
        log.info(
            f"    #{i+1} score={hit.distance:.4f} "
            f"pk={hit.entity.get('image_pk')} evergreen={hit.entity.get('is_evergreen')}"
        )

    return passed


# ---------------------------------------------------------------------------
# 测试 6: 性能基准
# ---------------------------------------------------------------------------
def test_performance_benchmark(query_vec):
    name = f"6. 性能基准 (连续 {PERF_ROUNDS} 次搜索)"
    t0 = time.time()
    connect()
    coll = Collection("img_202604_vip")

    latencies = []
    for _ in range(PERF_ROUNDS):
        t_s = time.time()
        coll.search(
            data=[query_vec],
            anns_field="global_vec",
            param=SEARCH_PARAMS,
            limit=TOP_K,
        )
        latencies.append((time.time() - t_s) * 1000)

    latencies.sort()
    p50 = latencies[len(latencies) // 2]
    p99 = latencies[int(len(latencies) * 0.99)]
    avg = sum(latencies) / len(latencies)
    min_l = latencies[0]
    max_l = latencies[-1]

    elapsed = (time.time() - t0) * 1000
    passed = p99 < 500  # P99 < 500ms 为通过
    summary = (
        f"P50={p50:.1f}ms, P99={p99:.1f}ms, avg={avg:.1f}ms, "
        f"min={min_l:.1f}ms, max={max_l:.1f}ms"
    )
    msg = fmt_result(passed, name, elapsed, summary)
    log.info(f"{ts()} {msg}")

    # 跨 Collection 性能
    log.info(f"  --- 跨 Collection 搜索性能 (每个 Collection {PERF_ROUNDS} 次) ---")
    for cname in COLLECTIONS:
        try:
            c = Collection(cname)
            lats = []
            for _ in range(PERF_ROUNDS):
                t_s = time.time()
                c.search(
                    data=[query_vec],
                    anns_field="global_vec",
                    param=SEARCH_PARAMS,
                    limit=TOP_K,
                )
                lats.append((time.time() - t_s) * 1000)
            lats.sort()
            cp50 = lats[len(lats) // 2]
            cp99 = lats[int(len(lats) * 0.99)]
            cavg = sum(lats) / len(lats)
            log.info(
                f"  {cname}: P50={cp50:.1f}ms P99={cp99:.1f}ms avg={cavg:.1f}ms"
            )
        except Exception as e:
            log.info(f"  {cname}: ERROR {e}")

    return passed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log.info(f"{ts()} ========== 多 Collection 查询验证开始 ==========")
    t_total = time.time()

    results = {}

    # Test 1: 连接 & load all (先释放旧 Collection)
    results["connection"] = test_connection()
    if not results["connection"]:
        log.error(f"{ts()} 连接测试失败，部分 Collection 无法加载")
        # 继续执行能跑的测试

    # Test 2: 单 Collection 搜索 (返回查询向量供后续测试复用)
    ret = test_single_collection_search()
    if isinstance(ret, tuple):
        results["single_search"] = ret[0]
        query_vec = ret[1]
        query_pk = ret[2]
    else:
        results["single_search"] = ret
        log.error(f"{ts()} 无法获取查询向量，跳过后续测试")
        _print_summary(results, time.time() - t_total)
        return

    # Test 3: 跨 Collection 搜索
    results["cross_collection"] = test_cross_collection_search(query_vec, query_pk)

    # Test 4: 标量过滤
    results["filtered_search"] = test_filtered_search(query_vec)

    # Test 5: 混合搜索
    results["hybrid_search"] = test_hybrid_search(query_vec)

    # Test 6: 性能基准
    results["performance"] = test_performance_benchmark(query_vec)

    _print_summary(results, time.time() - t_total)


def _print_summary(results: dict, total_s: float):
    log.info("")
    log.info(f"{ts()} ========== 验证汇总 ==========")
    all_pass = all(results.values())
    for k, v in results.items():
        tag = "PASS" if v else "FAIL"
        log.info(f"  [{tag}] {k}")
    status = "ALL PASS" if all_pass else "HAS FAILURES"
    log.info(f"{ts()} 总耗时 {total_s:.1f}s — {status}")
    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
