package com.szwego.imagesearch.bitmap.degrade;

import com.szwego.imagesearch.bitmap.store.RocksDBStore;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 健康检查 + 节点级降级 + 自动弹出/恢复
 *
 * <p>v1.3 加固:
 * <ul>
 *   <li>FIX #4: PG-RocksDB 行数差异检测 → 自动弹出负载均衡</li>
 *   <li>CDC lag 降级 (保留)</li>
 *   <li>自动恢复机制</li>
 * </ul>
 *
 * <p>弹出条件: PG vs RocksDB 行数差 >10,000 连续 3 次采样 (90s)
 * <p>恢复条件: 差值 <5,000 连续 3 次采样
 */
public class HealthChecker {

    private static final Logger LOG = LoggerFactory.getLogger(HealthChecker.class);

    public enum Status { SERVING, NOT_SERVING, DEGRADED }

    private static final long DIFF_EJECT_THRESHOLD = 10_000;
    private static final long DIFF_RECOVER_THRESHOLD = 5_000;
    private static final int CONSECUTIVE_SAMPLES_TO_EJECT = 3;
    private static final int CONSECUTIVE_SAMPLES_TO_RECOVER = 3;
    private static final long CHECK_INTERVAL_S = 30;

    private final RocksDBStore store;
    private final DataSource pgDataSource;
    private final MeterRegistry metrics;
    private final AtomicLong lastCdcEventMs = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong lastCdcOffset = new AtomicLong(0);

    // 节点弹出状态
    private final AtomicBoolean ejected = new AtomicBoolean(false);
    private final AtomicInteger consecutiveOverThreshold = new AtomicInteger(0);
    private final AtomicInteger consecutiveUnderThreshold = new AtomicInteger(0);
    private final AtomicLong lastPgRocksDbDiff = new AtomicLong(0);

    private final ScheduledExecutorService scheduler;

    public HealthChecker(RocksDBStore store, DataSource pgDataSource, MeterRegistry metrics) {
        this.store = store;
        this.pgDataSource = pgDataSource;
        this.metrics = metrics;

        // 注册 Prometheus 指标
        Gauge.builder("bitmap.pg_rocksdb_diff", lastPgRocksDbDiff, AtomicLong::get)
                .description("PG vs RocksDB row count difference")
                .register(metrics);
        Gauge.builder("bitmap.node_ejected", ejected, a -> a.get() ? 1.0 : 0.0)
                .description("Whether this node is ejected from LB")
                .register(metrics);
        // FIX-K: CDC consumer lag 可观测指标
        Gauge.builder("bitmap.cdc.lag_ms", lastCdcEventMs,
                ts -> System.currentTimeMillis() - ts.get())
                .description("CDC consumer lag in milliseconds (time since last event)")
                .register(metrics);
        Gauge.builder("bitmap.cdc.last_offset", lastCdcOffset, AtomicLong::get)
                .description("Last consumed CDC offset")
                .register(metrics);

        // 定时检查 PG-RocksDB 一致性
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "health-checker");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(
                this::checkPgRocksDbDiff,
                CHECK_INTERVAL_S, CHECK_INTERVAL_S, TimeUnit.SECONDS
        );
    }

    public void onCdcEvent(long offset) {
        lastCdcEventMs.set(System.currentTimeMillis());
        lastCdcOffset.set(offset);
    }

    public Status check() {
        // 节点已弹出 → NOT_SERVING
        if (ejected.get()) {
            return Status.NOT_SERVING;
        }

        if (!store.isOpen()) {
            return Status.NOT_SERVING;
        }

        long lagMs = System.currentTimeMillis() - lastCdcEventMs.get();
        if (lagMs > 60_000) {
            LOG.warn("CDC lag {}ms > 60s → NOT_SERVING", lagMs);
            return Status.NOT_SERVING;
        }
        if (lagMs > 10_000) {
            LOG.warn("CDC lag {}ms > 10s → DEGRADED", lagMs);
            return Status.DEGRADED;
        }
        return Status.SERVING;
    }

    /**
     * 定时检查 PG 与 RocksDB 行数差异
     * 差异过大 → 自动弹出; 恢复正常 → 自动重新加入
     */
    void checkPgRocksDbDiff() {
        try {
            long pgCount = queryPgCount();
            long rocksDbCount = store.estimateKeyCount();
            long diff = Math.abs(pgCount - rocksDbCount);

            lastPgRocksDbDiff.set(diff);
            LOG.debug("PG-RocksDB diff: pg={}, rocksdb={}, diff={}", pgCount, rocksDbCount, diff);

            if (diff > DIFF_EJECT_THRESHOLD) {
                consecutiveUnderThreshold.set(0);
                int over = consecutiveOverThreshold.incrementAndGet();
                if (over >= CONSECUTIVE_SAMPLES_TO_EJECT && !ejected.get()) {
                    ejectFromLoadBalancer(diff);
                }
            } else if (diff < DIFF_RECOVER_THRESHOLD) {
                consecutiveOverThreshold.set(0);
                int under = consecutiveUnderThreshold.incrementAndGet();
                if (under >= CONSECUTIVE_SAMPLES_TO_RECOVER && ejected.get()) {
                    recoverToLoadBalancer(diff);
                }
            } else {
                // 在阈值之间, 重置计数器
                consecutiveOverThreshold.set(0);
                consecutiveUnderThreshold.set(0);
            }
        } catch (Exception e) {
            LOG.error("PG-RocksDB diff check failed: {}", e.getMessage(), e);
        }
    }

    private void ejectFromLoadBalancer(long diff) {
        ejected.set(true);
        LOG.error("NODE EJECTED: PG-RocksDB diff={} > {} for {} consecutive samples. " +
                        "Health check will return NOT_SERVING.",
                diff, DIFF_EJECT_THRESHOLD, CONSECUTIVE_SAMPLES_TO_EJECT);

        // 标记需要 CDC 全量重建
        try {
            store.setMetadata("rebuild_requested", "true");
            store.setMetadata("rebuild_requested_at", String.valueOf(System.currentTimeMillis()));
        } catch (Exception e) {
            LOG.error("Failed to mark rebuild request", e);
        }

        metrics.counter("bitmap.node_ejected_total").increment();
    }

    private void recoverToLoadBalancer(long diff) {
        ejected.set(false);
        consecutiveOverThreshold.set(0);
        LOG.info("NODE RECOVERED: PG-RocksDB diff={} < {} for {} consecutive samples. " +
                        "Rejoining load balancer.",
                diff, DIFF_RECOVER_THRESHOLD, CONSECUTIVE_SAMPLES_TO_RECOVER);

        store.setMetadata("rebuild_requested", "false");
        metrics.counter("bitmap.node_recovered_total").increment();
    }

    private long queryPgCount() throws Exception {
        try (Connection conn = pgDataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT count(*) FROM image_merchant_bitmaps")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    // ── Getters ──

    public boolean isEjected() {
        return ejected.get();
    }

    public long getCdcLagMs() {
        return System.currentTimeMillis() - lastCdcEventMs.get();
    }

    public long getLastOffset() {
        return lastCdcOffset.get();
    }

    public long getRocksDBSizeBytes() {
        return store.estimateSize();
    }

    public long getPgRocksDbDiff() {
        return lastPgRocksDbDiff.get();
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
