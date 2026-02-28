package com.szwego.imagesearch.bitmap.store;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RocksDB 存储测试 — 双列族 (cf_rolling + cf_evergreen)
 */
@Tag("unit")
class RocksDBStoreTest {

    @TempDir
    Path tempDir;

    private RocksDB openTestDb(List<ColumnFamilyHandle> handles) throws RocksDBException {
        RocksDB.loadLibrary();
        ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
                .setCompressionType(CompressionType.LZ4_COMPRESSION);
        DBOptions dbOpts = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        List<ColumnFamilyDescriptor> cfDescs = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                new ColumnFamilyDescriptor("cf_rolling".getBytes(), cfOpts),
                new ColumnFamilyDescriptor("cf_evergreen".getBytes(), cfOpts)
        );
        return RocksDB.open(dbOpts, tempDir.toString(), cfDescs, handles);
    }

    @Test
    void dualCfInit() throws RocksDBException {
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        RocksDB db = openTestDb(handles);
        assertEquals(3, handles.size());  // default + rolling + evergreen
        db.close();
        handles.forEach(ColumnFamilyHandle::close);
    }

    @Test
    void putGetRolling() throws RocksDBException {
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        RocksDB db = openTestDb(handles);
        ColumnFamilyHandle cfRolling = handles.get(1);

        byte[] key = "testkey".getBytes();
        byte[] value = "testvalue".getBytes();
        db.put(cfRolling, key, value);
        byte[] result = db.get(cfRolling, key);

        assertNotNull(result);
        assertArrayEquals(value, result);

        db.close();
        handles.forEach(ColumnFamilyHandle::close);
    }

    @Test
    void putGetEvergreen() throws RocksDBException {
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        RocksDB db = openTestDb(handles);
        ColumnFamilyHandle cfEvergreen = handles.get(2);

        byte[] key = "evgkey".getBytes();
        byte[] value = "evgvalue".getBytes();
        db.put(cfEvergreen, key, value);
        byte[] result = db.get(cfEvergreen, key);

        assertNotNull(result);
        assertArrayEquals(value, result);

        db.close();
        handles.forEach(ColumnFamilyHandle::close);
    }

    @Test
    void deleteKey() throws RocksDBException {
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        RocksDB db = openTestDb(handles);
        ColumnFamilyHandle cfRolling = handles.get(1);

        byte[] key = "delkey".getBytes();
        db.put(cfRolling, key, "val".getBytes());
        assertNotNull(db.get(cfRolling, key));

        db.delete(cfRolling, key);
        assertNull(db.get(cfRolling, key));

        db.close();
        handles.forEach(ColumnFamilyHandle::close);
    }

    @Test
    void multiGetFallback() throws RocksDBException {
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        RocksDB db = openTestDb(handles);
        ColumnFamilyHandle cfRolling = handles.get(1);
        ColumnFamilyHandle cfEvergreen = handles.get(2);

        // Put in rolling
        db.put(cfRolling, "k1".getBytes(), "v1".getBytes());
        // Put in evergreen
        db.put(cfEvergreen, "k2".getBytes(), "v2".getBytes());

        // MultiGet from rolling
        List<byte[]> keys = Arrays.asList("k1".getBytes(), "k2".getBytes());
        List<byte[]> rollingVals = db.multiGetAsList(Collections.nCopies(keys.size(), cfRolling), keys);

        assertNotNull(rollingVals.get(0));  // k1 in rolling
        assertNull(rollingVals.get(1));     // k2 not in rolling

        // Fallback to evergreen for k2
        byte[] evgVal = db.get(cfEvergreen, "k2".getBytes());
        assertNotNull(evgVal);

        db.close();
        handles.forEach(ColumnFamilyHandle::close);
    }

    @Test
    void lz4Compression() {
        // Verify LZ4 compression config
        ColumnFamilyOptions opts = new ColumnFamilyOptions()
                .setCompressionType(CompressionType.LZ4_COMPRESSION);
        assertEquals(CompressionType.LZ4_COMPRESSION, opts.compressionType());
        opts.close();
    }
}
