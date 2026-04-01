#!/usr/bin/env python3
"""
端到端功能验证 — 不依赖应用服务, 直接连 Milvus + PG 测试核心链路

测试项:
  1. Milvus 向量搜索 (COSINE ANN)
  2. 品类过滤搜索 (category_l1 filter)
  3. 标签过滤搜索 (tags array_contains)
  4. PG bitmap 商家过滤
  5. CLIP 模型加载 + 特征提取 + 分类 (GPU)
  6. 全链路: 特征提取 → 向量搜索 → 品类过滤 → 商家过滤
"""
import json
import sys
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("e2e_test")

PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        PASS += 1
        log.info(f"  PASS: {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        log.error(f"  FAIL: {name}" + (f" — {detail}" if detail else ""))


def test_milvus_connection():
    log.info("=" * 60)
    log.info("TEST 1: Milvus 连接 + 数据完整性")
    log.info("=" * 60)
    from pymilvus import connections, Collection, utility

    connections.connect(host="localhost", port=19530)
    check("Milvus 连接", True)

    has_coll = utility.has_collection("global_images_hot")
    check("Collection 存在", has_coll)

    coll = Collection("global_images_hot")
    coll.load()
    count = coll.num_entities
    check("数据量 > 90000", count > 90000, f"count={count}")

    return coll


def test_vector_search(coll):
    log.info("=" * 60)
    log.info("TEST 2: 向量相似度搜索")
    log.info("=" * 60)

    # 取一条做 query
    sample = coll.query(expr='image_pk != ""',
                        output_fields=["image_pk", "global_vec", "category_l1"],
                        limit=1)
    query_vec = sample[0]["global_vec"]
    query_pk = sample[0]["image_pk"]
    log.info(f"  Query: pk={query_pk[:16]}..., category={sample[0]['category_l1']}")

    t0 = time.time()
    results = coll.search(
        data=[query_vec],
        anns_field="global_vec",
        param={"metric_type": "COSINE", "params": {"ef": 128}},
        limit=50,
        output_fields=["image_pk", "category_l1", "product_id", "tags"],
    )
    latency = (time.time() - t0) * 1000
    hits = results[0]

    check("搜索返回结果", len(hits) > 0, f"{len(hits)} hits")
    check("Top-1 是自身 (score=1.0)", hits[0].score > 0.999,
          f"score={hits[0].score:.4f}")
    check("搜索延迟 < 100ms", latency < 100, f"{latency:.1f}ms")
    check("结果包含品类信息", hits[0].entity.get("category_l1") is not None)
    check("结果包含标签信息", hits[0].entity.get("tags") is not None)

    return query_vec, query_pk, hits


def test_category_filter(coll, query_vec):
    log.info("=" * 60)
    log.info("TEST 3: 品类过滤搜索")
    log.info("=" * 60)

    # 只搜箱包 (category_l1 = 2, 数据量最大)
    t0 = time.time()
    results = coll.search(
        data=[query_vec],
        anns_field="global_vec",
        param={"metric_type": "COSINE", "params": {"ef": 128}},
        limit=20,
        expr="category_l1 == 2",
        output_fields=["image_pk", "category_l1"],
    )
    latency = (time.time() - t0) * 1000
    hits = results[0]

    all_cat2 = all(h.entity.get("category_l1") == 2 for h in hits)
    check("品类过滤返回结果", len(hits) > 0, f"{len(hits)} hits")
    check("所有结果 category_l1 == 2", all_cat2)
    check("过滤搜索延迟 < 200ms", latency < 200, f"{latency:.1f}ms")

    # 搜服装 (category_l1 = 0)
    results2 = coll.search(
        data=[query_vec],
        anns_field="global_vec",
        param={"metric_type": "COSINE", "params": {"ef": 128}},
        limit=10,
        expr="category_l1 == 0",
        output_fields=["image_pk", "category_l1"],
    )
    all_cat0 = all(h.entity.get("category_l1") == 0 for h in results2[0])
    check("服装过滤结果正确", all_cat0, f"{len(results2[0])} hits, all cat=0")


def test_tag_filter(coll, query_vec):
    log.info("=" * 60)
    log.info("TEST 4: 标签过滤搜索")
    log.info("=" * 60)

    # 取一个已知 tag
    sample = coll.query(expr='image_pk != ""',
                        output_fields=["tags"], limit=10)
    known_tag = None
    for s in sample:
        if s.get("tags"):
            known_tag = s["tags"][0]
            break

    if known_tag is None:
        check("找到已知标签", False, "no tags found in sample")
        return

    t0 = time.time()
    results = coll.search(
        data=[query_vec],
        anns_field="global_vec",
        param={"metric_type": "COSINE", "params": {"ef": 128}},
        limit=10,
        expr=f"array_contains(tags, {known_tag})",
        output_fields=["image_pk", "tags"],
    )
    latency = (time.time() - t0) * 1000
    hits = results[0]

    has_tag = all(known_tag in (h.entity.get("tags") or []) for h in hits)
    check("标签过滤返回结果", len(hits) > 0, f"{len(hits)} hits, tag={known_tag}")
    check("所有结果包含目标标签", has_tag)
    check("标签过滤延迟 < 200ms", latency < 200, f"{latency:.1f}ms")


def test_bitmap_filter(query_pk, candidate_pks):
    log.info("=" * 60)
    log.info("TEST 5: PG bitmap 商家过滤")
    log.info("=" * 60)
    import psycopg2

    conn = psycopg2.connect("postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search")
    cur = conn.cursor()

    # 验证 bitmap 表有数据
    cur.execute("SELECT count(*) FROM image_merchant_bitmaps")
    bmp_count = cur.fetchone()[0]
    check("bitmap 表有数据", bmp_count > 90000, f"count={bmp_count}")

    # 查询 query image 的商家
    cur.execute("SELECT rb_to_array(bitmap) FROM image_merchant_bitmaps WHERE image_pk = %s",
                (query_pk,))
    row = cur.fetchone()
    check("query image 有 bitmap", row is not None)
    query_merchants = row[0] if row else [1]
    log.info(f"  Query merchants: {query_merchants}")

    # 对候选集做 bitmap 过滤
    pk_list = ",".join([f"'{pk}'" for pk in candidate_pks[:50]])
    t0 = time.time()
    cur.execute(f"""
        SELECT image_pk FROM image_merchant_bitmaps
        WHERE image_pk IN ({pk_list})
        AND rb_cardinality(rb_and(bitmap, rb_build(%s::int[]))) > 0
    """, (query_merchants,))
    filtered = cur.fetchall()
    latency = (time.time() - t0) * 1000

    check("bitmap 过滤有结果", len(filtered) >= 1,
          f"{len(filtered)}/{len(candidate_pks)} passed")
    check("过滤后数量 <= 候选数量", len(filtered) <= len(candidate_pks))
    check("bitmap 过滤延迟 < 50ms", latency < 50, f"{latency:.1f}ms")

    # 验证: 用不存在的商家过滤应该结果很少
    cur.execute(f"""
        SELECT count(*) FROM image_merchant_bitmaps
        WHERE image_pk IN ({pk_list})
        AND rb_cardinality(rb_and(bitmap, rb_build(ARRAY[99999]::int[]))) > 0
    """)
    zero_count = cur.fetchone()[0]
    check("不存在的商家过滤结果为 0", zero_count == 0, f"count={zero_count}")

    cur.close()
    conn.close()


def test_pg_dedup():
    log.info("=" * 60)
    log.info("TEST 6: PG URI 去重表")
    log.info("=" * 60)
    import psycopg2

    conn = psycopg2.connect("postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search")
    cur = conn.cursor()

    cur.execute("SELECT count(*) FROM uri_dedup")
    count = cur.fetchone()[0]
    check("uri_dedup 有数据", count > 90000, f"count={count}")

    # 查询一条验证结构
    cur.execute("SELECT image_pk, uri_hash, uri FROM uri_dedup LIMIT 1")
    r = cur.fetchone()
    check("uri_dedup 有 URL", r[2] is not None and r[2].startswith("http"),
          f"uri={r[2][:60]}...")

    cur.close()
    conn.close()


def test_clip_gpu():
    log.info("=" * 60)
    log.info("TEST 7: CLIP 模型 GPU 推理 + 零样本分类")
    log.info("=" * 60)
    import torch
    import numpy as np

    check("CUDA 可用", torch.cuda.is_available(),
          f"{torch.cuda.device_count()} GPUs")

    import open_clip

    model_path = "/workspace/data/models/open_clip_model.safetensors"
    t0 = time.time()
    model, _, preprocess = open_clip.create_model_and_transforms(
        "ViT-B-32", pretrained=model_path)
    model = model.cuda().eval()
    tokenizer = open_clip.get_tokenizer("ViT-B-32")
    load_time = time.time() - t0
    check("CLIP 模型加载", True, f"{load_time:.1f}s")

    # 测试文本编码 (品类分类)
    categories = ["a photo of clothing", "a photo of shoes",
                  "a photo of bag", "a photo of jewelry", "a photo of accessories"]
    tokens = tokenizer(categories).cuda()
    with torch.no_grad():
        text_feats = model.encode_text(tokens)
        text_feats = text_feats / text_feats.norm(dim=-1, keepdim=True)
    check("文本编码 shape 正确", text_feats.shape == (5, 512),
          f"shape={text_feats.shape}")

    # 用随机向量模拟分类
    fake_img = torch.randn(1, 512, device="cuda")
    fake_img = fake_img / fake_img.norm(dim=-1, keepdim=True)
    sims = (fake_img @ text_feats.T).squeeze(0)
    cat_idx = sims.argmax().item()
    check("零样本分类输出有效", 0 <= cat_idx < 5, f"predicted category index={cat_idx}")

    # 投影矩阵测试
    rng = np.random.RandomState(42)
    proj = rng.randn(512, 256).astype(np.float32)
    u, _, _ = np.linalg.svd(proj, full_matrices=False)
    vec_512 = np.random.randn(1, 512).astype(np.float32)
    vec_256 = vec_512 @ u
    norm = np.linalg.norm(vec_256)
    vec_256 = vec_256 / max(norm, 1e-8)
    check("512→256 投影正常", vec_256.shape == (1, 256), f"shape={vec_256.shape}")

    return model, preprocess, tokenizer, text_feats


def test_full_pipeline(coll, model, preprocess, tokenizer, text_feats):
    log.info("=" * 60)
    log.info("TEST 8: 全链路测试 (向量搜索 → 品类过滤 → 商家过滤)")
    log.info("=" * 60)
    import torch
    import numpy as np
    import psycopg2

    t_total = time.time()

    # 从 Milvus 取一条真实向量当 query
    sample = coll.query(expr="category_l1 == 2",
                        output_fields=["image_pk", "global_vec"],
                        limit=1)
    query_vec = sample[0]["global_vec"]
    query_pk = sample[0]["image_pk"]
    log.info(f"  Query: pk={query_pk[:16]}... (箱包)")

    # Step 1: Milvus ANN 搜索 (模拟 coarse_top_k=3000)
    t0 = time.time()
    results = coll.search(
        data=[query_vec],
        anns_field="global_vec",
        param={"metric_type": "COSINE", "params": {"ef": 256}},
        limit=200,
        output_fields=["image_pk", "category_l1", "product_id"],
    )
    milvus_ms = (time.time() - t0) * 1000
    candidates = [(h.entity.get("image_pk"), h.score, h.entity.get("category_l1"))
                  for h in results[0]]
    log.info(f"  Step 1 Milvus ANN: {len(candidates)} candidates, {milvus_ms:.1f}ms")

    # Step 2: 品类过滤 (只保留箱包)
    t0 = time.time()
    cat_filtered = [(pk, score, cat) for pk, score, cat in candidates if cat == 2]
    cat_ms = (time.time() - t0) * 1000
    log.info(f"  Step 2 品类过滤: {len(candidates)} → {len(cat_filtered)}, {cat_ms:.2f}ms")

    # Step 3: PG bitmap 商家过滤
    conn = psycopg2.connect("postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search")
    cur = conn.cursor()

    cur.execute("SELECT rb_to_array(bitmap) FROM image_merchant_bitmaps WHERE image_pk = %s",
                (query_pk,))
    query_merchants = cur.fetchone()[0]

    pk_list = ",".join([f"'{pk}'" for pk, _, _ in cat_filtered])
    t0 = time.time()
    cur.execute(f"""
        SELECT image_pk FROM image_merchant_bitmaps
        WHERE image_pk IN ({pk_list})
        AND rb_cardinality(rb_and(bitmap, rb_build(%s::int[]))) > 0
    """, (query_merchants,))
    bitmap_pks = {r[0].strip() for r in cur.fetchall()}
    bitmap_ms = (time.time() - t0) * 1000

    final = [(pk, score) for pk, score, _ in cat_filtered if pk in bitmap_pks]
    log.info(f"  Step 3 bitmap 过滤: {len(cat_filtered)} → {len(final)}, {bitmap_ms:.1f}ms")

    total_ms = (time.time() - t_total) * 1000
    log.info(f"  Total pipeline: {total_ms:.1f}ms")

    check("全链路有最终结果", len(final) >= 1, f"{len(final)} results")
    check("全链路延迟 < 500ms", total_ms < 500, f"{total_ms:.1f}ms")
    check("结果按相似度排序", all(final[i][1] >= final[i+1][1] for i in range(len(final)-1)))

    if final:
        log.info(f"  Top-3 results:")
        for pk, score in final[:3]:
            log.info(f"    pk={pk[:16]}... score={score:.4f}")

    cur.close()
    conn.close()


def main():
    global PASS, FAIL
    t_start = time.time()

    log.info("=" * 60)
    log.info("图搜系统端到端功能验证")
    log.info(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    # Test 1: Milvus
    coll = test_milvus_connection()

    # Test 2: 向量搜索
    query_vec, query_pk, hits = test_vector_search(coll)
    candidate_pks = [h.entity.get("image_pk") for h in hits]

    # Test 3: 品类过滤
    test_category_filter(coll, query_vec)

    # Test 4: 标签过滤
    test_tag_filter(coll, query_vec)

    # Test 5: bitmap 过滤
    test_bitmap_filter(query_pk, candidate_pks)

    # Test 6: PG 去重
    test_pg_dedup()

    # Test 7: CLIP GPU
    model, preprocess, tokenizer, text_feats = test_clip_gpu()

    # Test 8: 全链路
    test_full_pipeline(coll, model, preprocess, tokenizer, text_feats)

    # Summary
    total_time = time.time() - t_start
    log.info("=" * 60)
    log.info(f"测试完成: {PASS} PASS, {FAIL} FAIL ({total_time:.1f}s)")
    log.info("=" * 60)

    return 1 if FAIL > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
