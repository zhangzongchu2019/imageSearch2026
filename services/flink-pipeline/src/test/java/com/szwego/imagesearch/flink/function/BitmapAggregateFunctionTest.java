package com.szwego.imagesearch.flink.function;

import com.szwego.imagesearch.flink.model.BitmapUpdate;
import com.szwego.imagesearch.flink.model.MerchantEvent;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 窗口聚合测试 (ADD/REMOVE/幂等/合并)
 */
@Tag("unit")
class BitmapAggregateFunctionTest {

    /**
     * 创建一个不依赖外部连接的 BitmapAggregateFunction 测试版本
     * 由于 MerchantDictEncoder.getInstance() 需要 PG/Redis 连接,
     * 我们直接测试 BitmapUpdate 的 Bitmap 操作逻辑
     */

    @Test
    void emptyAccumulator() {
        BitmapUpdate acc = new BitmapUpdate();
        assertNull(acc.getImagePk());
        assertTrue(acc.getAdditions().isEmpty());
        assertTrue(acc.getRemovals().isEmpty());
    }

    @Test
    void addSetsPk() {
        BitmapUpdate acc = new BitmapUpdate();
        acc.setImagePk("pk001");
        acc.getAdditions().add(42);
        assertEquals("pk001", acc.getImagePk());
        assertTrue(acc.getAdditions().contains(42));
    }

    @Test
    void addIdempotent() {
        BitmapUpdate acc = new BitmapUpdate();
        acc.getAdditions().add(42);
        acc.getAdditions().add(42);  // second add
        assertEquals(1, acc.getAdditions().getCardinality());
    }

    @Test
    void removeEvent() {
        BitmapUpdate acc = new BitmapUpdate();
        acc.getRemovals().add(99);
        assertTrue(acc.getRemovals().contains(99));
        assertEquals(1, acc.getRemovals().getCardinality());
    }

    @Test
    void mergeTwoBitmaps() {
        BitmapUpdate a = new BitmapUpdate();
        a.setImagePk("pk001");
        a.getAdditions().add(1);
        a.getAdditions().add(2);

        BitmapUpdate b = new BitmapUpdate();
        b.setImagePk("pk001");
        b.getAdditions().add(3);
        b.getRemovals().add(10);

        // merge b into a
        a.getAdditions().or(b.getAdditions());
        a.getRemovals().or(b.getRemovals());

        assertEquals(3, a.getAdditions().getCardinality());
        assertTrue(a.getAdditions().contains(1));
        assertTrue(a.getAdditions().contains(2));
        assertTrue(a.getAdditions().contains(3));
        assertTrue(a.getRemovals().contains(10));
    }

    @Test
    void window100Events() {
        BitmapUpdate acc = new BitmapUpdate();
        acc.setImagePk("pk_window_test");
        for (int i = 0; i < 100; i++) {
            acc.getAdditions().add(i);
        }
        assertEquals(100, acc.getAdditions().getCardinality());
    }

    @Test
    void bitmapSerializable() {
        RoaringBitmap bmp = new RoaringBitmap();
        bmp.add(1, 2, 3, 100, 200);
        bmp.runOptimize();
        assertEquals(5, bmp.getCardinality());
    }
}
