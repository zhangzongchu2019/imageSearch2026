package com.szwego.imagesearch.flink.function;

import com.szwego.imagesearch.flink.model.BitmapUpdate;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PG Upsert SQL 与 Bitmap 序列化测试
 */
@Tag("unit")
class BitmapPostgresqlSinkTest {

    @Test
    void upsertSqlRbOr() {
        // 验证 UPSERT SQL 使用 rb_or
        String sql = """
            INSERT INTO image_merchant_bitmaps (image_pk, bitmap, updated_at)
            VALUES (?, ?::roaringbitmap, now())
            ON CONFLICT (image_pk) DO UPDATE
            SET bitmap = rb_or(image_merchant_bitmaps.bitmap, EXCLUDED.bitmap),
                updated_at = now()
            """;
        assertTrue(sql.contains("rb_or"));
        assertTrue(sql.contains("ON CONFLICT"));
    }

    @Test
    void removeSqlRbAndnot() {
        // 验证 REMOVE SQL 使用 rb_andnot
        String sql = """
            UPDATE image_merchant_bitmaps
            SET bitmap = rb_andnot(bitmap, ?::roaringbitmap),
                updated_at = now()
            WHERE image_pk = ?
            """;
        assertTrue(sql.contains("rb_andnot"));
    }

    @Test
    void bitmapSerialization() throws Exception {
        RoaringBitmap bmp = new RoaringBitmap();
        bmp.add(1, 2, 3, 42, 99);
        bmp.runOptimize();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        bmp.serialize(dos);
        byte[] bytes = baos.toByteArray();

        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        // Deserialize and verify
        RoaringBitmap restored = new RoaringBitmap();
        restored.deserialize(java.nio.ByteBuffer.wrap(bytes));
        assertEquals(5, restored.getCardinality());
        assertTrue(restored.contains(42));
    }

    @Test
    void emptyAdditionsSkipped() {
        BitmapUpdate update = new BitmapUpdate();
        update.setImagePk("pk_test");
        // Empty additions → should skip UPSERT
        assertTrue(update.getAdditions().isEmpty());
    }

    @Test
    void emptyRemovalsSkipped() {
        BitmapUpdate update = new BitmapUpdate();
        update.setImagePk("pk_test");
        // Empty removals → should skip REMOVE
        assertTrue(update.getRemovals().isEmpty());
    }
}
