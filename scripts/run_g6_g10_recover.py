#!/usr/bin/env python3
"""
g6-g10 恢复推理 (基于 run_g2_g10_fast.py)
- 跳过 g2-g5 (已完成)
- 跳过传输 (图片已在 /data/imgsrch/dl_g{6-10})
- 直接推理 + 写入
"""
import sys
sys.path.insert(0, "/workspace/scripts")

# 导入主编排函数
from run_g2_g10_fast import (
    BATCHES, infer_batch, write_batch, cleanup_batch
)
import time, logging, os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("recover")

# 只处理 g6-g10
RECOVER_BATCHES = [b for b in BATCHES if b["id"] in ("g6", "g7", "g8", "g9", "g10")]


def main():
    log.info("=" * 60)
    log.info(f"恢复 {len(RECOVER_BATCHES)} 批: " + ", ".join(b["id"] for b in RECOVER_BATCHES))
    log.info("=" * 60)

    t_global = time.time()
    total_written = 0

    for i, batch in enumerate(RECOVER_BATCHES):
        log.info(f"\n[{batch['id']}] 开始 ({i+1}/{len(RECOVER_BATCHES)})")
        t_batch = time.time()

        # 验证图片目录
        if not os.path.isdir(batch["dst"]):
            log.warning(f"  目录不存在: {batch['dst']}, 跳过")
            continue
        cnt = len(os.listdir(batch["dst"]))
        log.info(f"  图片数: {cnt:,}")

        # 推理
        infer_dir = f"/data/imgsrch/infer_{batch['id']}"
        results = infer_batch(batch, infer_dir)
        if not results:
            log.warning(f"  推理 0 条!")
            continue

        # 写入
        mv = write_batch(batch, results)
        total_written += mv

        # 清理
        cleanup_batch(batch)

        elapsed = time.time() - t_batch
        log.info(f"  本批完成: {mv:,} 条, {elapsed:.0f}s | 累计 {total_written:,}")

    log.info(f"\n{'='*60}")
    log.info(f"全部完成: {total_written:,}, {(time.time()-t_global)/60:.1f}min")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    main()
