package com.szwego.imagesearch.flink.job;

import com.szwego.imagesearch.flink.function.BitmapAggregateFunction;
import com.szwego.imagesearch.flink.function.BitmapPostgresqlSink;
import com.szwego.imagesearch.flink.function.MerchantEventDeserializer;
import com.szwego.imagesearch.flink.model.BitmapUpdate;
import com.szwego.imagesearch.flink.model.MerchantEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * 商家关联 Bitmap 聚合 Flink Job
 *
 * <p>v1.2 变更:
 * <ul>
 *   <li>窗口从 5s 缩至 1s (写入可见性 3× 压缩)</li>
 *   <li>预留 REMOVE 事件支持 (v1.2+ 商家解绑)</li>
 *   <li>bitmap-filter PushUpdate 直推</li>
 * </ul>
 *
 * <p>链路: Kafka(merchant-events) → KeyBy(image_pk) → Window(1s) → Aggregate → PG Upsert + PushUpdate
 */
public class MerchantBitmapJob {

    private static final Logger LOG = LoggerFactory.getLogger(MerchantBitmapJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint 配置
        env.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60_000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setParallelism(4);

        // 从配置加载 (生产环境用 ParameterTool)
        String kafkaBrokers = System.getenv().getOrDefault(
            "KAFKA_BROKERS", "localhost:9092"
        );
        String kafkaTopic = System.getenv().getOrDefault(
            "KAFKA_TOPIC", "image-search.merchant-events"
        );
        String pgJdbcUrl = System.getenv().getOrDefault(
            "PG_JDBC_URL", "jdbc:postgresql://localhost:5432/image_search"
        );
        String pgUser = System.getenv().getOrDefault("PG_USER", "postgres");
        String pgPassword = System.getenv().getOrDefault("PG_PASSWORD", "");
        int windowSeconds = Integer.parseInt(
            System.getenv().getOrDefault("WINDOW_SECONDS", "1")
        );

        // Kafka Source
        KafkaSource<MerchantEvent> kafkaSource = KafkaSource.<MerchantEvent>builder()
            .setBootstrapServers(kafkaBrokers)
            .setTopics(kafkaTopic)
            .setGroupId("flink-merchant-bitmap-aggregator")
            .setStartingOffsets(OffsetsInitializer.committedOffsets())
            .setValueOnlyDeserializer(new MerchantEventDeserializer())
            .build();

        // Pipeline
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-MerchantEvents")
            .name("kafka-source")
            .uid("kafka-source-uid")

            // 按 image_pk 分组 (保证同图事件有序)
            .keyBy(MerchantEvent::getImagePk)

            // v1.2: 窗口从 5s 缩至 1s (写入可见性加速)
            // Flink 2.0: Time.seconds() deprecated → Duration.ofSeconds()
            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(windowSeconds)))

            // 窗口内聚合: 按 image_pk 合并 Bitmap
            .aggregate(new BitmapAggregateFunction())
            .name("bitmap-aggregate")
            .uid("bitmap-aggregate-uid")

            // Sink: PG Upsert + bitmap-filter PushUpdate
            .addSink(new BitmapPostgresqlSink(pgJdbcUrl, pgUser, pgPassword))
            .name("pg-sink")
            .uid("pg-sink-uid");

        LOG.info("Starting MerchantBitmapJob with {}s window", windowSeconds);
        env.execute("MerchantBitmapAggregation-v1.2");
    }
}
