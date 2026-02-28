package com.szwego.imagesearch.bitmap.grpc;

import com.szwego.imagesearch.bitmap.store.RocksDBStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * gRPC 服务实现: 批量 Bitmap 过滤
 *
 * <p>核心逻辑: 对每个候选 image_pk, 从 RocksDB 取出 Bitmap,
 * 与查询商家范围 Bitmap 做 AND 运算, 非空则保留
 *
 * <p>性能目标: P99 ≤ 8ms (2000 候选)
 */
@Component
public class BitmapFilterHandler {

    private static final Logger LOG = LoggerFactory.getLogger(BitmapFilterHandler.class);

    private final RocksDBStore store;
    private final MeterRegistry metrics;
    private final ExecutorService multiGetPool;

    // 并行 MultiGet 线程数
    private static final int MULTI_GET_THREADS = 4;

    public BitmapFilterHandler(RocksDBStore store, MeterRegistry metrics) {
        this.store = store;
        this.metrics = metrics;
        this.multiGetPool = Executors.newFixedThreadPool(MULTI_GET_THREADS,
            r -> {
                Thread t = new Thread(r, "rocksdb-multiget");
                t.setDaemon(true);
                return t;
            });
    }

    /**
     * 批量过滤: 输入候选 PK 列表 + 查询商家 Bitmap → 匹配的 PK 列表
     */
    public BatchFilterResult batchFilter(
            List<byte[]> candidatePks,
            byte[] queryBitmapBytes,
            int maxResults) {

        Timer.Sample sample = metrics != null ? Timer.start(metrics) : null;
        try {
            // 1. 反序列化查询 Bitmap
            RoaringBitmap queryBitmap = new RoaringBitmap();
            queryBitmap.deserialize(ByteBuffer.wrap(queryBitmapBytes));

            // 2. 并行 MultiGet 从 RocksDB 读取
            List<byte[]> storedBitmaps = parallelMultiGet(candidatePks);

            // 3. AND 过滤
            List<byte[]> filtered = new ArrayList<>();
            for (int i = 0; i < candidatePks.size(); i++) {
                byte[] bmpBytes = storedBitmaps.get(i);
                if (bmpBytes == null) continue;

                RoaringBitmap candidateBmp = new RoaringBitmap();
                candidateBmp.deserialize(ByteBuffer.wrap(bmpBytes));

                // AND: 候选图片的商家集 ∩ 查询商家范围 ≠ ∅
                if (RoaringBitmap.and(candidateBmp, queryBitmap).getCardinality() > 0) {
                    filtered.add(candidatePks.get(i));
                    if (maxResults > 0 && filtered.size() >= maxResults) break;
                }
            }

            return new BatchFilterResult(
                filtered,
                candidatePks.size(),
                filtered.size()
            );

        } catch (Exception e) {
            LOG.error("BatchFilter failed: {}", e.getMessage(), e);
            throw new RuntimeException("Filter error", e);
        } finally {
            if (sample != null && metrics != null) {
                sample.stop(metrics.timer("bitmap.filter.latency"));
            }
        }
    }

    /**
     * 并行 MultiGet: 将候选列表分为 4 份并行查 RocksDB
     */
    private List<byte[]> parallelMultiGet(List<byte[]> keys) throws Exception {
        int batchSize = (keys.size() + MULTI_GET_THREADS - 1) / MULTI_GET_THREADS;
        List<Future<List<byte[]>>> futures = new ArrayList<>();

        for (int i = 0; i < keys.size(); i += batchSize) {
            int end = Math.min(i + batchSize, keys.size());
            List<byte[]> batch = keys.subList(i, end);
            futures.add(multiGetPool.submit(() -> store.multiGet(batch)));
        }

        List<byte[]> results = new ArrayList<>(keys.size());
        for (Future<List<byte[]>> f : futures) {
            results.addAll(f.get(5, TimeUnit.MILLISECONDS));  // 5ms per-shard timeout
        }
        return results;
    }

    /**
     * 健康检查
     */
    public HealthStatus healthCheck() {
        long cdcLagMs = store.getCdcLagMs();
        long sizeBytes = store.getSizeBytes();

        HealthStatus.Status status;
        if (!store.isOpen() || cdcLagMs > 60_000) {
            status = HealthStatus.Status.NOT_SERVING;
        } else if (cdcLagMs > 10_000) {
            status = HealthStatus.Status.DEGRADED;
        } else {
            status = HealthStatus.Status.SERVING;
        }

        return new HealthStatus(status, sizeBytes, store.getLastCdcOffset(), cdcLagMs);
    }

    /**
     * FIX-V: 优雅关闭线程池, 防止服务关闭时线程泄露
     */
    public void shutdown() {
        multiGetPool.shutdown();
        try {
            if (!multiGetPool.awaitTermination(3, TimeUnit.SECONDS)) {
                multiGetPool.shutdownNow();
                LOG.warn("multiGetPool forced shutdown");
            }
        } catch (InterruptedException e) {
            multiGetPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOG.info("BitmapFilterHandler multiGetPool closed");
    }

    // ── 返回类型 ──

    public record BatchFilterResult(
        List<byte[]> filteredPks,
        int totalCandidates,
        int filteredCount
    ) {
        public float filterRatio() {
            return totalCandidates > 0 ? (float) filteredCount / totalCandidates : 0f;
        }
    }

    public record HealthStatus(Status status, long sizeBytes, long lastCdcOffset, long cdcLagMs) {
        public enum Status { SERVING, NOT_SERVING, DEGRADED }
    }
}
