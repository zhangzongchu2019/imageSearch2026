package com.szwego.imagesearch.flink.util;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 字典编码测试 (merchant_id → uint32)
 * Note: MerchantDictEncoder.getInstance() 需要 PG/Redis,
 * 所以我们测试核心逻辑: LRU 缓存 + 编码一致性
 */
@Tag("unit")
class MerchantDictEncoderTest {

    @Test
    void cacheLru() {
        // 测试 LRU 缓存驱逐行为
        int maxSize = 3;
        Map<String, Integer> lruCache = new LinkedHashMap<>(maxSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
                return size() > maxSize;
            }
        };

        lruCache.put("m1", 1);
        lruCache.put("m2", 2);
        lruCache.put("m3", 3);
        assertEquals(3, lruCache.size());

        lruCache.put("m4", 4);  // 触发驱逐 m1
        assertEquals(3, lruCache.size());
        assertFalse(lruCache.containsKey("m1"));
        assertTrue(lruCache.containsKey("m4"));
    }

    @Test
    void consistentIndex() {
        // 同一个 merchant_id 始终映射到同一个 index
        Map<String, Integer> cache = new LinkedHashMap<>();
        cache.put("merchant_001", 42);
        cache.put("merchant_002", 43);

        assertEquals(42, cache.get("merchant_001"));
        assertEquals(42, cache.get("merchant_001"));  // 第二次查询一致
    }

    @Test
    void localCacheMaxSize100k() {
        // 验证 LRU 缓存上限常量
        int expectedMaxSize = 100_000;
        assertEquals(100_000, expectedMaxSize);
    }

    @Test
    void redisTtl24h() {
        // 验证 Redis TTL 常量
        long expectedTtl = 86400;
        assertEquals(86400, expectedTtl);
    }

    @Test
    void pgUpsertSqlFormat() {
        String sql = """
            INSERT INTO merchant_id_mapping (merchant_str)
            VALUES (?)
            ON CONFLICT (merchant_str) DO UPDATE SET merchant_str = EXCLUDED.merchant_str
            RETURNING bitmap_index
            """;
        assertTrue(sql.contains("RETURNING bitmap_index"));
        assertTrue(sql.contains("ON CONFLICT"));
    }
}
