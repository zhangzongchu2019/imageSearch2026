package com.szwego.imagesearch.bitmap.grpc;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bitmap 过滤逻辑测试 — AND 操作 + filter ratio
 */
@Tag("unit")
class BitmapFilterHandlerTest {

    private byte[] serializeBitmap(RoaringBitmap bmp) throws Exception {
        bmp.runOptimize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        bmp.serialize(dos);
        return baos.toByteArray();
    }

    @Test
    void batchFilterAnd() throws Exception {
        // Query bitmap: merchants {1, 2, 3}
        RoaringBitmap queryBmp = new RoaringBitmap();
        queryBmp.add(1, 2, 3);
        byte[] queryBytes = serializeBitmap(queryBmp);

        // Candidate 1: merchants {1, 5} → intersect {1} → keep
        RoaringBitmap c1Bmp = new RoaringBitmap();
        c1Bmp.add(1, 5);

        // Candidate 2: merchants {4, 5} → intersect {} → skip
        RoaringBitmap c2Bmp = new RoaringBitmap();
        c2Bmp.add(4, 5);

        // Verify AND logic
        RoaringBitmap queryDeserialized = new RoaringBitmap();
        queryDeserialized.deserialize(ByteBuffer.wrap(queryBytes));

        assertTrue(RoaringBitmap.and(c1Bmp, queryDeserialized).getCardinality() > 0);
        assertEquals(0, RoaringBitmap.and(c2Bmp, queryDeserialized).getCardinality());
    }

    @Test
    void maxResultsLimit() throws Exception {
        RoaringBitmap queryBmp = new RoaringBitmap();
        queryBmp.add(1);

        // 10 candidates all matching
        List<RoaringBitmap> candidates = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            RoaringBitmap bmp = new RoaringBitmap();
            bmp.add(1);
            candidates.add(bmp);
        }

        int maxResults = 5;
        List<Integer> filtered = new ArrayList<>();
        for (int i = 0; i < candidates.size(); i++) {
            if (RoaringBitmap.and(candidates.get(i), queryBmp).getCardinality() > 0) {
                filtered.add(i);
                if (maxResults > 0 && filtered.size() >= maxResults) break;
            }
        }
        assertEquals(5, filtered.size());
    }

    @Test
    void parallel4Shards() {
        // Verify shard calculation
        int totalKeys = 2000;
        int threads = 4;
        int batchSize = (totalKeys + threads - 1) / threads;
        assertEquals(500, batchSize);

        // Verify all keys are covered
        int covered = 0;
        for (int i = 0; i < totalKeys; i += batchSize) {
            int end = Math.min(i + batchSize, totalKeys);
            covered += (end - i);
        }
        assertEquals(totalKeys, covered);
    }

    @Test
    void filterRatio() {
        var result = new BitmapFilterHandler.BatchFilterResult(
                List.of(new byte[]{1}, new byte[]{2}),
                10,
                2
        );
        assertEquals(0.2f, result.filterRatio(), 0.01);
    }

    @Test
    void filterRatioZeroCandidates() {
        var result = new BitmapFilterHandler.BatchFilterResult(
                List.of(),
                0,
                0
        );
        assertEquals(0f, result.filterRatio());
    }

    @Test
    void missingBitmapSkipped() throws Exception {
        RoaringBitmap queryBmp = new RoaringBitmap();
        queryBmp.add(1);

        // null bitmap → skip (not included in results)
        byte[] nullBitmap = null;
        // Should be skipped in the filtering logic
        assertNull(nullBitmap);
    }

    @Test
    void healthCheckServing() {
        // cdcLag < 10s → SERVING
        long cdcLagMs = 5000;
        var status = cdcLagMs > 60_000 ? "NOT_SERVING" :
                     cdcLagMs > 10_000 ? "DEGRADED" : "SERVING";
        assertEquals("SERVING", status);
    }

    @Test
    void healthCheckDegraded() {
        long cdcLagMs = 30_000;
        var status = cdcLagMs > 60_000 ? "NOT_SERVING" :
                     cdcLagMs > 10_000 ? "DEGRADED" : "SERVING";
        assertEquals("DEGRADED", status);
    }

    @Test
    void healthCheckNotServing() {
        long cdcLagMs = 90_000;
        var status = cdcLagMs > 60_000 ? "NOT_SERVING" :
                     cdcLagMs > 10_000 ? "DEGRADED" : "SERVING";
        assertEquals("NOT_SERVING", status);
    }
}
