#!/usr/bin/env python3
"""
Task 7: 为缺失 bitmap 的 image_pk 补建 image_merchant_bitmaps 记录。
约 2000 万条 SVIP 记录缺失 bitmap。

策略: 从 uri_dedup LEFT JOIN image_merchant_bitmaps 找出缺失记录，
     为每条分配随机 merchant (与 derive_records.py 一致)，
     分批插入。

Usage:
    python3 scripts/rebuild_bitmaps.py
    python3 scripts/rebuild_bitmaps.py --batch-size 50000 --dry-run
"""
import argparse
import logging
import os
import random
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

PG_DSN = os.getenv("PG_DSN", "postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search")
MERCHANT_RANGE = (10001, 15999)
BATCH_SIZE = 50000  # 每批插入行数


def get_missing_count(cur):
    cur.execute("""
        SELECT count(*) FROM uri_dedup u
        LEFT JOIN image_merchant_bitmaps b ON u.image_pk = b.image_pk
        WHERE b.image_pk IS NULL
    """)
    return cur.fetchone()[0]


def rebuild_bitmaps(batch_size: int, dry_run: bool = False):
    import psycopg2
    from psycopg2.extras import execute_values

    log.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 连接 PostgreSQL: {PG_DSN}")
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = False
    cur = conn.cursor()

    # 读取 merchant_id_mapping
    cur.execute("SELECT merchant_str, bitmap_index FROM merchant_id_mapping")
    merchant_map = {r[0]: r[1] for r in cur.fetchall()}
    log.info(f"  merchant_id_mapping: {len(merchant_map)} 条")

    # 所有 bitmap_index 列表, 用于随机分配
    all_bitmap_indices = list(merchant_map.values())
    if not all_bitmap_indices:
        log.error("merchant_id_mapping 为空, 无法分配 bitmap")
        return

    # 统计缺失数
    missing = get_missing_count(cur)
    log.info(f"  缺失 bitmap 记录: {missing:,} 条")

    if missing == 0:
        log.info("无缺失记录, 无需重建")
        return

    if dry_run:
        log.info("[DRY RUN] 不执行实际写入")
        return

    # 分批获取缺失的 image_pk 并插入 bitmap
    # 策略: 先一次性读出所有缺失 pk (约20M, ~640MB内存), 然后分批插入
    # 不用 server-side cursor (commit 会关闭它)
    rng = random.Random(42)  # 固定种子，可复现
    done = 0
    t0 = time.time()
    failed = 0

    log.info("  读取缺失的 image_pk ...")
    t_read = time.time()
    cur.execute("SELECT u.image_pk FROM uri_dedup u "
                "LEFT JOIN image_merchant_bitmaps b ON u.image_pk = b.image_pk "
                "WHERE b.image_pk IS NULL")
    missing_pks = [r[0] for r in cur.fetchall()]
    log.info(f"  读取完成: {len(missing_pks):,} 条, 耗时 {time.time()-t_read:.1f}s")
    conn.commit()  # 释放查询锁

    for i in range(0, len(missing_pks), batch_size):
        batch_pks = missing_pks[i:i + batch_size]
        values = []
        for image_pk in batch_pks:
            bmp_idx = rng.choice(all_bitmap_indices)
            values.append((image_pk, [bmp_idx]))

        try:
            execute_values(
                cur,
                "INSERT INTO image_merchant_bitmaps (image_pk, bitmap) VALUES %s "
                "ON CONFLICT (image_pk) DO UPDATE SET bitmap = EXCLUDED.bitmap, updated_at = now()",
                values,
                template="(%s, rb_build(%s::int[]))",
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            failed += len(values)
            log.error(f"  批次写入失败: {e}")
            continue

        done += len(batch_pks)
        elapsed = time.time() - t0
        rate = done / elapsed if elapsed > 0 else 0
        if done % 500000 < batch_size or i + batch_size >= len(missing_pks):
            log.info(f"  bitmap 补建: {done:,}/{missing:,} ({done*100/missing:.1f}%) "
                     f"[{rate:,.0f} rec/s, elapsed {elapsed:.0f}s]")

    elapsed = time.time() - t0
    log.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 补建完成: {done:,} 条, "
             f"失败 {failed:,} 条, 耗时 {elapsed:.1f}s ({done/elapsed:,.0f} rec/s)")

    # 最终统计
    cur.execute("SELECT count(*) FROM image_merchant_bitmaps")
    total_bitmap = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM uri_dedup")
    total_dedup = cur.fetchone()[0]
    log.info(f"  最终: image_merchant_bitmaps={total_bitmap:,}, uri_dedup={total_dedup:,}")
    log.info(f"  覆盖率: {total_bitmap*100/total_dedup:.1f}%")

    cur.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="补建缺失的 image_merchant_bitmaps")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    rebuild_bitmaps(batch_size=args.batch_size, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
