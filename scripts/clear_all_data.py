#!/usr/bin/env python3
"""
清空 Milvus + PostgreSQL + Redis 中的所有导入数据。

Usage:
    python3 scripts/clear_all_data.py
    python3 scripts/clear_all_data.py --yes          # 跳过确认
"""

import argparse
import os
import sys

MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))
PG_DSN = os.getenv("PG_DSN", "postgresql://imgsrch:imgsrch_pass@localhost:5432/image_search")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
COLLECTION_NAME = "global_images_hot"


def clear_milvus():
    from pymilvus import connections, Collection, utility

    print(f"[Milvus] Connecting to {MILVUS_HOST}:{MILVUS_PORT} ...")
    connections.connect(alias="default", host=MILVUS_HOST, port=MILVUS_PORT)

    if not utility.has_collection(COLLECTION_NAME):
        print(f"[Milvus] Collection '{COLLECTION_NAME}' does not exist, nothing to clear.")
        return

    coll = Collection(COLLECTION_NAME)
    print(f"[Milvus] Collection '{COLLECTION_NAME}' has {len(coll.partitions)} partitions, "
          f"~{coll.num_entities} entities")

    # Release collection first (required before drop partition)
    try:
        coll.release()
    except Exception:
        pass

    # Drop all user partitions (skip _default)
    for p in list(coll.partitions):
        if p.name == "_default":
            continue
        print(f"  Dropping partition '{p.name}' ...")
        coll.drop_partition(p.name)

    # For _default partition: load then delete
    print("  Deleting all entities from _default partition ...")
    try:
        coll.load()
        coll.delete(expr="image_pk != ''")
    except Exception as e:
        print(f"  Warning: delete from _default failed ({e}), may be empty")

    coll.flush()
    try:
        coll.release()
    except Exception:
        pass
    print(f"[Milvus] Done. Entity count after clear: {coll.num_entities}")


def clear_postgres():
    import psycopg2

    print(f"[PostgreSQL] Connecting ...")
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()

    cur.execute("TRUNCATE uri_dedup")
    cur.execute("TRUNCATE image_merchant_bitmaps")
    cur.execute("TRUNCATE merchant_id_mapping")
    conn.commit()

    cur.close()
    conn.close()
    print("[PostgreSQL] Truncated uri_dedup, image_merchant_bitmaps, merchant_id_mapping")


def clear_redis():
    try:
        import redis
    except ImportError:
        # Fallback: use redis-cli directly
        import subprocess
        print("[Redis] python redis not installed, using redis-cli ...")
        result = subprocess.run(
            ["docker", "exec", "imagesearch2026_redis_1", "redis-cli", "FLUSHDB"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            print(f"[Redis] FLUSHDB done ({result.stdout.strip()})")
        else:
            print(f"[Redis] Warning: {result.stderr.strip()}")
        return

    print(f"[Redis] Connecting to {REDIS_URL} ...")
    r = redis.from_url(REDIS_URL)
    r.flushdb()
    print("[Redis] FLUSHDB done")


def main():
    parser = argparse.ArgumentParser(description="Clear all imported data")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    args = parser.parse_args()

    if not args.yes:
        print("WARNING: This will DELETE ALL data from Milvus, PostgreSQL, and Redis.")
        answer = input("Type 'yes' to confirm: ")
        if answer.strip().lower() != "yes":
            print("Aborted.")
            sys.exit(0)

    clear_milvus()
    clear_postgres()
    clear_redis()
    print("\n=== All data cleared successfully ===")


if __name__ == "__main__":
    main()
