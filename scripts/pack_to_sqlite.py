#!/usr/bin/env python3
"""
将目录中的图片打包到 SQLite (单文件, 顺序IO, 解决 998K 小文件瓶颈)

用法:
  python3 pack_to_sqlite.py --image-dir /tmp/import_batch2 --output /data/imgsrch/dl_batch2.db

速度: ~20K files/s (主要是 readdir + read)
"""
import argparse, os, sqlite3, time, sys


def pack(image_dir, output_db):
    if os.path.exists(output_db):
        print(f"已存在: {output_db}, 跳过")
        return

    conn = sqlite3.connect(output_db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("CREATE TABLE IF NOT EXISTS images (pk TEXT PRIMARY KEY, data BLOB)")

    files = os.listdir(image_dir)
    jpg_files = [f for f in files if f.endswith(".jpg")]
    print(f"总文件: {len(jpg_files):,}")

    batch = []
    ok = fail = 0
    t0 = time.time()

    for i, fname in enumerate(jpg_files):
        pk = fname.replace(".jpg", "")
        fpath = os.path.join(image_dir, fname)
        try:
            with open(fpath, "rb") as f:
                data = f.read()
            if len(data) > 500:
                batch.append((pk, data))
                ok += 1
            else:
                fail += 1
        except:
            fail += 1

        if len(batch) >= 5000:
            conn.executemany("INSERT OR IGNORE INTO images VALUES (?,?)", batch)
            conn.commit()
            batch = []

        if (i + 1) % 50000 == 0:
            elapsed = time.time() - t0
            print(f"  {i+1:,}/{len(jpg_files):,} ({(i+1)/elapsed:.0f}/s) ok={ok:,} fail={fail:,}")

    if batch:
        conn.executemany("INSERT OR IGNORE INTO images VALUES (?,?)", batch)
        conn.commit()

    # 验证
    count = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
    db_size = os.path.getsize(output_db)
    conn.close()

    elapsed = time.time() - t0
    print(f"完成: {count:,} 图片, {db_size/1024/1024:.0f}MB, {elapsed:.0f}s ({count/elapsed:.0f}/s)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    pack(args.image_dir, args.output)
