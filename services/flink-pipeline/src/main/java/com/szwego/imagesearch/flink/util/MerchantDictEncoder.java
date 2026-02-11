package com.szwego.imagesearch.flink.util;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 商家 ID 字典编码: string → uint32
 *
 * <p>三级缓存: 本地 LRU (100K) → Redis (24h TTL) → PG (SERIAL 自增)
 * <p>编码上限: uint32 max = 4,294,967,295, 远大于 4000 万商家
 */
public class MerchantDictEncoder {

    private static final Logger LOG = LoggerFactory.getLogger(MerchantDictEncoder.class);
    private static volatile MerchantDictEncoder instance;

    private final Map<String, Integer> localCache;
    private RedisCommands<String, String> redis;
    private Connection pgConnection;

    private static final int LOCAL_CACHE_SIZE = 100_000;
    private static final long REDIS_TTL_SECONDS = 86400;
    private static final String REDIS_PREFIX = "dict:merchant:";

    private static final String PG_UPSERT_SQL = """
        INSERT INTO merchant_id_mapping (merchant_str)
        VALUES (?)
        ON CONFLICT (merchant_str) DO UPDATE SET merchant_str = EXCLUDED.merchant_str
        RETURNING bitmap_index
        """;

    private MerchantDictEncoder() {
        // LRU Cache
        this.localCache = new LinkedHashMap<>(LOCAL_CACHE_SIZE, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
                return size() > LOCAL_CACHE_SIZE;
            }
        };
        initConnections();
    }

    public static MerchantDictEncoder getInstance() {
        if (instance == null) {
            synchronized (MerchantDictEncoder.class) {
                if (instance == null) {
                    instance = new MerchantDictEncoder();
                }
            }
        }
        return instance;
    }

    /**
     * 编码: merchant_id (string) → bitmap_index (int)
     * 线程安全: localCache 使用 synchronized LinkedHashMap
     */
    public int encode(String merchantId) {
        // Level 1: Local LRU
        synchronized (localCache) {
            Integer cached = localCache.get(merchantId);
            if (cached != null) return cached;
        }

        // Level 2: Redis
        try {
            if (redis != null) {
                String val = redis.get(REDIS_PREFIX + merchantId);
                if (val != null) {
                    int index = Integer.parseInt(val);
                    synchronized (localCache) {
                        localCache.put(merchantId, index);
                    }
                    return index;
                }
            }
        } catch (Exception e) {
            LOG.warn("Redis lookup failed for {}: {}", merchantId, e.getMessage());
        }

        // Level 3: PG (INSERT ... ON CONFLICT RETURNING)
        try {
            int index = pgAllocate(merchantId);
            // 回写缓存
            synchronized (localCache) {
                localCache.put(merchantId, index);
            }
            if (redis != null) {
                try {
                    redis.setex(REDIS_PREFIX + merchantId, REDIS_TTL_SECONDS, String.valueOf(index));
                } catch (Exception e) {
                    // Redis 写入失败不阻塞
                }
            }
            return index;
        } catch (Exception e) {
            LOG.error("PG dict encode failed for {}: {}", merchantId, e.getMessage());
            throw new RuntimeException("Dict encode failed", e);
        }
    }

    private int pgAllocate(String merchantId) throws Exception {
        ensurePgConnection();
        try (PreparedStatement ps = pgConnection.prepareStatement(PG_UPSERT_SQL)) {
            ps.setString(1, merchantId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("bitmap_index");
                }
            }
        }
        throw new RuntimeException("Failed to allocate bitmap_index for " + merchantId);
    }

    private void initConnections() {
        // Redis
        try {
            String redisUrl = System.getenv().getOrDefault("REDIS_URL", "redis://localhost:6379");
            RedisClient client = RedisClient.create(redisUrl);
            redis = client.connect().sync();
            LOG.info("Redis connected for dict encoder");
        } catch (Exception e) {
            LOG.warn("Redis not available for dict encoder: {}", e.getMessage());
        }

        // PG
        ensurePgConnection();
    }

    private void ensurePgConnection() {
        try {
            if (pgConnection == null || pgConnection.isClosed()) {
                String url = System.getenv().getOrDefault(
                    "PG_JDBC_URL", "jdbc:postgresql://localhost:5432/image_search"
                );
                String user = System.getenv().getOrDefault("PG_USER", "postgres");
                String pass = System.getenv().getOrDefault("PG_PASSWORD", "");
                pgConnection = DriverManager.getConnection(url, user, pass);
                LOG.info("PG connected for dict encoder");
            }
        } catch (Exception e) {
            LOG.error("PG connection failed: {}", e.getMessage());
        }
    }
}
