package com.szwego.imagesearch.bitmap.store;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RocksDB 封装 — 双 Column Family (cf_rolling + cf_evergreen)
 *
 * <p>Key: hex_decode(image_pk) → 16 bytes
 * <p>Value: 序列化的 Roaring Bitmap (平均 ~2KB)
 *
 * <p>配置 (对齐系统设计 v1.2 §3.4):
 * <ul>
 *   <li>block_cache: 8GB</li>
 *   <li>bloom_filter: 10 bits (假阳率 ~1%)</li>
 *   <li>compression: LZ4</li>
 *   <li>write_buffer: 128MB</li>
 * </ul>
 */
@Component
public class RocksDBStore {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStore.class);

    @Value("${rocksdb.data-dir:/data/rocksdb}")
    private String dataDir;

    @Value("${rocksdb.block-cache-gb:8}")
    private int blockCacheGb;

    private RocksDB db;
    private ColumnFamilyHandle cfRolling;
    private ColumnFamilyHandle cfEvergreen;
    private final AtomicLong lastCdcOffset = new AtomicLong(0);
    private final AtomicLong lastCdcEventTimeMs = new AtomicLong(System.currentTimeMillis());

    /** No-arg constructor for Spring component scanning. */
    public RocksDBStore() {
    }

    /** Standalone constructor (used by integration tests and main()). */
    public RocksDBStore(String dataDir) {
        this.dataDir = dataDir;
    }

    /** Standalone constructor with cache size (used by main()). */
    public RocksDBStore(String dataDir, long blockCacheGb) {
        this.dataDir = dataDir;
        this.blockCacheGb = (int) blockCacheGb;
    }

    @PostConstruct
    public void init() {
        try {
            RocksDB.loadLibrary();

            // Block-based table options
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                .setBlockCache(new LRUCache((long) blockCacheGb * 1024 * 1024 * 1024))
                .setFilterPolicy(new BloomFilter(10, false))
                .setBlockSize(16 * 1024);  // 16KB blocks

            // Column family options
            ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                .setTableFormatConfig(tableConfig)
                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setWriteBufferSize(128 * 1024 * 1024)    // 128MB
                .setMaxWriteBufferNumber(3)
                .setLevel0FileNumCompactionTrigger(4)
                .setTargetFileSizeBase(256 * 1024 * 1024)  // 256MB
                // FIX-S: 显式 Level Compaction + 周期性强制压缩
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setPeriodicCompactionSeconds(7 * 86400)   // 7天强制重压缩
                .setMaxBytesForLevelBase(1024L * 1024 * 1024) // 1GB L1
                .setMaxBytesForLevelMultiplier(10);         // L2=10GB, L3=100GB

            // DB options
            DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxOpenFiles(-1)                       // 无限制
                .setMaxBackgroundJobs(4)
                .setStatsDumpPeriodSec(60)
                // FIX-S: compaction rate limiter (防止 IO 突刺影响查询)
                .setRateLimiter(new RateLimiter(
                    100L * 1024 * 1024,  // 100MB/s compaction IO 上限
                    100_000,             // refill period 100ms
                    10                   // fairness
                ));

            // Column families
            List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
                new ColumnFamilyDescriptor("cf_rolling".getBytes(), cfOptions),
                new ColumnFamilyDescriptor("cf_evergreen".getBytes(), cfOptions)
            );

            List<ColumnFamilyHandle> handles = new ArrayList<>();
            db = RocksDB.open(dbOptions, Path.of(dataDir).toString(), cfDescriptors, handles);

            cfRolling = handles.get(1);
            cfEvergreen = handles.get(2);

            LOG.info("RocksDB opened at {} with {}GB block cache", dataDir, blockCacheGb);

        } catch (RocksDBException e) {
            LOG.error("Failed to initialize RocksDB: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    public void close() {
        if (cfRolling != null) cfRolling.close();
        if (cfEvergreen != null) cfEvergreen.close();
        if (db != null) db.close();
        // FIX-W: 置 null 使 isOpen() 正确返回 false
        cfRolling = null;
        cfEvergreen = null;
        db = null;
        LOG.info("RocksDB closed");
    }

    /**
     * 批量读取: 先查 cf_rolling, 若为 null 再查 cf_evergreen
     */
    public List<byte[]> multiGet(List<byte[]> keys) {
        List<byte[]> results = new ArrayList<>(keys.size());
        try {
            // 查 rolling — multiGetAsList 需要 List<ColumnFamilyHandle>
            List<ColumnFamilyHandle> rollingHandles = Collections.nCopies(keys.size(), cfRolling);
            List<byte[]> rollingVals = db.multiGetAsList(rollingHandles, keys);
            // 查 evergreen (针对 rolling 中没有的)
            List<byte[]> evergreenKeys = new ArrayList<>();
            List<Integer> evergreenIdx = new ArrayList<>();
            for (int i = 0; i < keys.size(); i++) {
                if (rollingVals.get(i) == null) {
                    evergreenKeys.add(keys.get(i));
                    evergreenIdx.add(i);
                }
            }

            List<ColumnFamilyHandle> evergreenHandles = Collections.nCopies(evergreenKeys.size(), cfEvergreen);
            List<byte[]> evergreenVals = evergreenKeys.isEmpty()
                ? List.of()
                : db.multiGetAsList(evergreenHandles, evergreenKeys);

            // 合并结果
            for (int i = 0; i < keys.size(); i++) {
                results.add(rollingVals.get(i));
            }
            for (int j = 0; j < evergreenIdx.size(); j++) {
                if (j < evergreenVals.size() && evergreenVals.get(j) != null) {
                    results.set(evergreenIdx.get(j), evergreenVals.get(j));
                }
            }

        } catch (RocksDBException e) {
            LOG.error("MultiGet failed: {}", e.getMessage());
            throw new RuntimeException(e);
        }
        return results;
    }

    /**
     * 写入 (CDC 消费端调用)
     */
    public void put(String cf, byte[] key, byte[] value) throws RocksDBException {
        ColumnFamilyHandle handle = "cf_evergreen".equals(cf) ? cfEvergreen : cfRolling;
        db.put(handle, key, value);
        lastCdcEventTimeMs.set(System.currentTimeMillis());
    }

    /**
     * 删除
     */
    public void delete(String cf, byte[] key) throws RocksDBException {
        ColumnFamilyHandle handle = "cf_evergreen".equals(cf) ? cfEvergreen : cfRolling;
        db.delete(handle, key);
    }

    public boolean isOpen() {
        return db != null;
    }

    public long getCdcLagMs() {
        return System.currentTimeMillis() - lastCdcEventTimeMs.get();
    }

    public long getLastCdcOffset() {
        return lastCdcOffset.get();
    }

    public void setLastCdcOffset(long offset) {
        lastCdcOffset.set(offset);
    }

    public long getSizeBytes() {
        try {
            return Long.parseLong(db.getProperty("rocksdb.estimate-live-data-size"));
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * 估算 Key 总数 (rolling + evergreen)
     */
    public long estimateKeyCount() {
        try {
            long rolling = Long.parseLong(
                db.getProperty(cfRolling, "rocksdb.estimate-num-keys"));
            long evergreen = Long.parseLong(
                db.getProperty(cfEvergreen, "rocksdb.estimate-num-keys"));
            return rolling + evergreen;
        } catch (Exception e) {
            LOG.warn("estimateKeyCount failed: {}", e.getMessage());
            return -1;
        }
    }

    /**
     * estimateSize 别名 (兼容 HealthChecker)
     */
    public long estimateSize() {
        return getSizeBytes();
    }

    /**
     * 写入 (String key convenience overload for integration tests)
     */
    public void put(String cf, String key, byte[] value) throws RocksDBException {
        put(cf, key.getBytes(), value);
    }

    /**
     * 删除 (String key convenience overload)
     */
    public void delete(String cf, String key) throws RocksDBException {
        delete(cf, key.getBytes());
    }

    /**
     * 单 key 读取 (String key convenience overload)
     */
    public byte[] get(String cf, String key) {
        return get(cf, key.getBytes());
    }

    /**
     * 单 key 读取
     */
    public byte[] get(String cf, byte[] key) {
        try {
            ColumnFamilyHandle handle = "cf_evergreen".equals(cf) ? cfEvergreen : cfRolling;
            return db.get(handle, key);
        } catch (RocksDBException e) {
            LOG.error("Get failed for cf={}: {}", cf, e.getMessage());
            return null;
        }
    }

    /**
     * 元数据存储 (default CF, 用于 rebuild 标记等)
     */
    public void setMetadata(String key, String value) {
        try {
            db.put(("__meta__:" + key).getBytes(), value.getBytes());
        } catch (RocksDBException e) {
            LOG.error("setMetadata failed: {}", e.getMessage());
        }
    }

    public String getMetadata(String key) {
        try {
            byte[] val = db.get(("__meta__:" + key).getBytes());
            return val != null ? new String(val) : null;
        } catch (RocksDBException e) {
            LOG.error("getMetadata failed: {}", e.getMessage());
            return null;
        }
    }
}
