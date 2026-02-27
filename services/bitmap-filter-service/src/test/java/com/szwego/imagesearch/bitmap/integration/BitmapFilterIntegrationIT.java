package com.szwego.imagesearch.bitmap.integration;

import com.szwego.imagesearch.bitmap.store.RocksDBStore;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bitmap 过滤服务集成测试 — 真实 RocksDB + 完整过滤链路
 */
@Tag("integration")
class BitmapFilterIntegrationIT {

    @TempDir
    Path tempDir;

    @Test
    void testRocksDBRollingWriteAndRead() throws Exception {
        RocksDBStore store = new RocksDBStore(tempDir.toString());
        store.init();

        RoaringBitmap bm = new RoaringBitmap();
        bm.add(1, 2, 3, 100, 200);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        bm.serialize(new DataOutputStream(baos));
        byte[] serialized = baos.toByteArray();

        store.put("rolling", "img_001", serialized);
        byte[] retrieved = store.get("rolling", "img_001");

        assertNotNull(retrieved);
        RoaringBitmap restored = new RoaringBitmap();
        restored.deserialize(new java.io.DataInputStream(
            new java.io.ByteArrayInputStream(retrieved)));
        assertEquals(bm, restored);

        store.close();
    }

    @Test
    void testRocksDBEvergreenWriteAndRead() throws Exception {
        RocksDBStore store = new RocksDBStore(tempDir.toString());
        store.init();

        RoaringBitmap bm = new RoaringBitmap();
        bm.add(500, 600, 700);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        bm.serialize(new DataOutputStream(baos));

        store.put("evergreen", "img_ever_001", baos.toByteArray());
        byte[] retrieved = store.get("evergreen", "img_ever_001");
        assertNotNull(retrieved);

        store.close();
    }

    @Test
    void testRocksDBMultiGet() throws Exception {
        RocksDBStore store = new RocksDBStore(tempDir.toString());
        store.init();

        for (int i = 0; i < 10; i++) {
            RoaringBitmap bm = new RoaringBitmap();
            bm.add(i * 10, i * 10 + 1);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            bm.serialize(new DataOutputStream(baos));
            store.put("rolling", "multi_" + i, baos.toByteArray());
        }

        // Read back multiple
        for (int i = 0; i < 10; i++) {
            byte[] data = store.get("rolling", "multi_" + i);
            assertNotNull(data, "Key multi_" + i + " should exist");
        }

        // Non-existent key
        byte[] missing = store.get("rolling", "multi_999");
        assertNull(missing);

        store.close();
    }

    @Test
    void testRocksDBDeleteKey() throws Exception {
        RocksDBStore store = new RocksDBStore(tempDir.toString());
        store.init();

        RoaringBitmap bm = new RoaringBitmap();
        bm.add(42);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        bm.serialize(new DataOutputStream(baos));

        store.put("rolling", "to_delete", baos.toByteArray());
        assertNotNull(store.get("rolling", "to_delete"));

        store.delete("rolling", "to_delete");
        assertNull(store.get("rolling", "to_delete"));

        store.close();
    }

    @Test
    void testBitmapAndFilter() throws Exception {
        // Simulate bitmap AND filtering logic
        RoaringBitmap merchantBitmap = new RoaringBitmap();
        merchantBitmap.add(1, 2, 3, 4, 5);

        RoaringBitmap candidateBitmap = new RoaringBitmap();
        candidateBitmap.add(3, 4, 5, 6, 7);

        RoaringBitmap result = RoaringBitmap.and(merchantBitmap, candidateBitmap);
        assertEquals(3, result.getCardinality());
        assertTrue(result.contains(3));
        assertTrue(result.contains(4));
        assertTrue(result.contains(5));
    }
}
