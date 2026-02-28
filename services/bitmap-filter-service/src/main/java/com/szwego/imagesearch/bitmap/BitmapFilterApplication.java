package com.szwego.imagesearch.bitmap;

import com.szwego.imagesearch.bitmap.grpc.BitmapFilterHandler;
import com.szwego.imagesearch.bitmap.store.RocksDBStore;
import com.szwego.imagesearch.bitmap.sync.CdcKafkaConsumer;
import com.szwego.imagesearch.bitmap.degrade.HealthChecker;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * bitmap-filter-service gRPC 服务主入口
 *
 * <p>职责: 商家 Bitmap 过滤 (内嵌 RocksDB)
 * <p>SLA: P99 ≤8ms
 * <p>CDC: Debezium → Kafka → RocksDB 近实时同步
 */
public class BitmapFilterApplication {

    private static final Logger LOG = LoggerFactory.getLogger(BitmapFilterApplication.class);
    private static final int PORT = Integer.parseInt(
        System.getenv().getOrDefault("GRPC_PORT", "50051")
    );

    public static void main(String[] args) throws IOException, InterruptedException {
        // 1. 初始化 RocksDB
        String dbPath = System.getenv().getOrDefault("ROCKSDB_PATH", "/data/rocksdb");
        long blockCacheGb = Long.parseLong(
            System.getenv().getOrDefault("ROCKSDB_CACHE_GB", "8")
        );
        RocksDBStore store = new RocksDBStore(dbPath, blockCacheGb);
        store.init();
        LOG.info("RocksDB initialized at {}", dbPath);

        // 2. 初始化健康检查
        HealthChecker healthChecker = new HealthChecker(store);

        // 3. 启动 CDC 消费 (Kafka → RocksDB)
        String kafkaBrokers = System.getenv().getOrDefault("KAFKA_BROKERS", "localhost:9092");
        String cdcTopic = System.getenv().getOrDefault(
            "CDC_TOPIC", "dbserver.public.image_merchant_bitmaps"
        );
        CdcKafkaConsumer cdcConsumer = new CdcKafkaConsumer(kafkaBrokers, cdcTopic, store, healthChecker);
        Thread cdcThread = new Thread(cdcConsumer::start, "cdc-consumer");
        cdcThread.setDaemon(true);
        cdcThread.start();
        LOG.info("CDC consumer started for topic {}", cdcTopic);

        // 4. 启动 gRPC 服务
        BitmapFilterHandler handler = new BitmapFilterHandler(store, null);
        Server server = ServerBuilder.forPort(PORT)
            .build()
            .start();

        LOG.info("bitmap-filter-service started on port {}", PORT);

        // 5. 优雅关闭
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down...");
            server.shutdown();
            handler.shutdown();  // FIX-V: 关闭 multiGet 线程池
            healthChecker.shutdown();
            cdcConsumer.stop();
            store.close();
        }));

        server.awaitTermination();
    }
}
