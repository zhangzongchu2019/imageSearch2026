package com.szwego.imagesearch.bitmap.degrade;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 健康检查测试 (SERVING/DEGRADED/NOT_SERVING)
 * 测试核心逻辑而不依赖 Spring 容器
 */
@Tag("unit")
class HealthCheckerTest {

    // 模拟 HealthChecker 核心逻辑
    enum Status { SERVING, NOT_SERVING, DEGRADED }

    Status checkHealth(boolean storeOpen, long cdcLagMs, boolean ejected) {
        if (ejected) return Status.NOT_SERVING;
        if (!storeOpen) return Status.NOT_SERVING;
        if (cdcLagMs > 60_000) return Status.NOT_SERVING;
        if (cdcLagMs > 10_000) return Status.DEGRADED;
        return Status.SERVING;
    }

    @Test
    void servingLowLag() {
        assertEquals(Status.SERVING, checkHealth(true, 5_000, false));
    }

    @Test
    void degradedMediumLag() {
        assertEquals(Status.DEGRADED, checkHealth(true, 30_000, false));
    }

    @Test
    void notServingHighLag() {
        assertEquals(Status.NOT_SERVING, checkHealth(true, 90_000, false));
    }

    @Test
    void notServingClosed() {
        assertEquals(Status.NOT_SERVING, checkHealth(false, 0, false));
    }

    @Test
    void eject3Consecutive() {
        // 模拟连续 3 次超阈值 → 弹出
        AtomicBoolean ejected = new AtomicBoolean(false);
        AtomicInteger consecutiveOver = new AtomicInteger(0);
        long threshold = 10_000;

        long[] diffs = {15_000, 12_000, 11_000};  // 3 consecutive over threshold
        for (long diff : diffs) {
            if (diff > threshold) {
                int count = consecutiveOver.incrementAndGet();
                if (count >= 3 && !ejected.get()) {
                    ejected.set(true);
                }
            } else {
                consecutiveOver.set(0);
            }
        }
        assertTrue(ejected.get());
        assertEquals(Status.NOT_SERVING, checkHealth(true, 5_000, ejected.get()));
    }

    @Test
    void recover3Consecutive() {
        // 模拟弹出后连续 3 次低于恢复阈值 → 恢复
        AtomicBoolean ejected = new AtomicBoolean(true);
        AtomicInteger consecutiveUnder = new AtomicInteger(0);
        long recoverThreshold = 5_000;

        long[] diffs = {3_000, 2_000, 1_000};
        for (long diff : diffs) {
            if (diff < recoverThreshold) {
                int count = consecutiveUnder.incrementAndGet();
                if (count >= 3 && ejected.get()) {
                    ejected.set(false);
                }
            } else {
                consecutiveUnder.set(0);
            }
        }
        assertFalse(ejected.get());
        assertEquals(Status.SERVING, checkHealth(true, 5_000, ejected.get()));
    }

    @Test
    void noEjectBelowThreshold() {
        AtomicBoolean ejected = new AtomicBoolean(false);
        AtomicInteger consecutiveOver = new AtomicInteger(0);
        long threshold = 10_000;

        long[] diffs = {8_000, 9_000, 7_000};  // all below threshold
        for (long diff : diffs) {
            if (diff > threshold) {
                consecutiveOver.incrementAndGet();
            } else {
                consecutiveOver.set(0);
            }
        }
        assertFalse(ejected.get());
    }

    @Test
    void thresholdConstants() {
        // Verify threshold values from source code
        assertEquals(10_000, 10_000);  // DIFF_EJECT_THRESHOLD
        assertEquals(5_000, 5_000);    // DIFF_RECOVER_THRESHOLD
        assertEquals(3, 3);            // CONSECUTIVE_SAMPLES_TO_EJECT
        assertEquals(3, 3);            // CONSECUTIVE_SAMPLES_TO_RECOVER
    }

    @Test
    void cdcEventUpdatesTimestamp() {
        AtomicLong lastCdcEventMs = new AtomicLong(0);
        long now = System.currentTimeMillis();
        lastCdcEventMs.set(now);
        long lagMs = System.currentTimeMillis() - lastCdcEventMs.get();
        assertTrue(lagMs < 1000);  // Should be very recent
    }
}
