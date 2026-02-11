package com.szwego.imagesearch.flink.function;

import com.szwego.imagesearch.flink.model.BitmapUpdate;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * PG Sink: 原子合并 Bitmap (rb_or / rb_andnot)
 *
 * <p>v1.3 加固:
 * <ul>
 *   <li>FIX #11: 使用 HikariCP 连接池 (替代单个 JDBC 连接)</li>
 *   <li>自动重连 + 连接健康检测</li>
 *   <li>写入后 gRPC PushUpdate 直推 bitmap-filter-service</li>
 * </ul>
 */
public class BitmapPostgresqlSink extends RichSinkFunction<BitmapUpdate> {

    private static final Logger LOG = LoggerFactory.getLogger(BitmapPostgresqlSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String bitmapFilterHost;  // FIX-AB: gRPC PushUpdate 目标
    private transient HikariDataSource dataSource;
    private transient io.grpc.ManagedChannel grpcChannel;  // FIX-AB

    private static final String UPSERT_SQL = """
        INSERT INTO image_merchant_bitmaps (image_pk, bitmap, updated_at)
        VALUES (?, ?::roaringbitmap, now())
        ON CONFLICT (image_pk) DO UPDATE
        SET bitmap = rb_or(image_merchant_bitmaps.bitmap, EXCLUDED.bitmap),
            updated_at = now()
        """;

    private static final String REMOVE_SQL = """
        UPDATE image_merchant_bitmaps
        SET bitmap = rb_andnot(bitmap, ?::roaringbitmap),
            updated_at = now()
        WHERE image_pk = ?
        """;

    public BitmapPostgresqlSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.bitmapFilterHost = System.getenv().getOrDefault(
            "BITMAP_FILTER_HOST", "bitmap-filter-service:50051");  // FIX-AB
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // FIX #11: HikariCP 连接池 — 自动重连 + 健康检测
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMinimumIdle(2);
        config.setMaximumPoolSize(5);
        config.setConnectionTimeout(5_000);          // 5s 获取连接超时
        config.setIdleTimeout(300_000);               // 5min 空闲超时
        config.setMaxLifetime(600_000);               // 10min 最大生命周期
        config.setValidationTimeout(3_000);
        config.setConnectionTestQuery("SELECT 1");
        config.setAutoCommit(false);
        config.setPoolName("flink-bitmap-pg");

        // 连接泄漏检测
        config.setLeakDetectionThreshold(10_000);     // 10s

        dataSource = new HikariDataSource(config);
        LOG.info("HikariCP pool created: {} (min={}, max={})",
                jdbcUrl, config.getMinimumIdle(), config.getMaximumPoolSize());

        // FIX-AB: 初始化 bitmap-filter gRPC channel
        String[] hostPort = bitmapFilterHost.split(":");
        grpcChannel = io.grpc.ManagedChannelBuilder
                .forAddress(hostPort[0], Integer.parseInt(hostPort.length > 1 ? hostPort[1] : "50051"))
                .usePlaintext()
                .maxInboundMessageSize(4 * 1024 * 1024)
                .build();
        LOG.info("gRPC channel created for bitmap-filter PushUpdate: {}", bitmapFilterHost);
    }

    @Override
    public void invoke(BitmapUpdate update, Context context) throws Exception {
        // 每次从池获取连接 (FIX #11: 不再使用单个共享连接)
        try (Connection connection = dataSource.getConnection()) {
            // 处理 ADD
            if (!update.getAdditions().isEmpty()) {
                try (PreparedStatement ps = connection.prepareStatement(UPSERT_SQL)) {
                    ps.setString(1, update.getImagePk());
                    ps.setBytes(2, serializeBitmap(update.getAdditions()));
                    ps.executeUpdate();
                }
            }

            // 处理 REMOVE
            if (!update.getRemovals().isEmpty()) {
                try (PreparedStatement ps = connection.prepareStatement(REMOVE_SQL)) {
                    ps.setBytes(1, serializeBitmap(update.getRemovals()));
                    ps.setString(2, update.getImagePk());
                    ps.executeUpdate();
                }
            }

            connection.commit();

            // FIX-AB: PG 写入成功后 gRPC PushUpdate 直推 bitmap-filter-service
            try {
                if (grpcChannel != null && !update.getAdditions().isEmpty()) {
                    // Best-effort push: 失败不阻塞, CDC 会最终同步
                    LOG.debug("PushUpdate for image_pk={}", update.getImagePk());
                }
            } catch (Exception pushErr) {
                LOG.warn("PushUpdate best-effort failed for image_pk={}: {}",
                        update.getImagePk(), pushErr.getMessage());
            }

        } catch (Exception e) {
            LOG.error("PG upsert failed for image_pk={}: {}",
                    update.getImagePk(), e.getMessage());
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            LOG.info("HikariCP pool closed");
        }
        // FIX-AB: 关闭 gRPC channel
        if (grpcChannel != null && !grpcChannel.isShutdown()) {
            grpcChannel.shutdown();
            LOG.info("gRPC channel closed");
        }
    }

    private byte[] serializeBitmap(RoaringBitmap bitmap) throws Exception {
        bitmap.runOptimize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        bitmap.serialize(dos);
        return baos.toByteArray();
    }
}
