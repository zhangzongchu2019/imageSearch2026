package com.szwego.imagesearch.bitmap.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.szwego.imagesearch.bitmap.degrade.HealthChecker;
import com.szwego.imagesearch.bitmap.store.RocksDBStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * CDC 事件消费: Debezium PG → Kafka → RocksDB
 *
 * <p>事件格式: Debezium change event (op: c/u/d)
 * <p>路由: is_evergreen → cf_evergreen / cf_rolling
 * <p>FIX-X: 处理失败 → 死信队列 bitmap-cdc-dlq
 *
 * <p>对齐系统设计 v1.2 §6.3
 */
@Component
public class CdcKafkaConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(CdcKafkaConsumer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DLQ_TOPIC = "bitmap-cdc-dlq";  // FIX-X
    private static final int MAX_DLQ_RETRIES = 3;               // FIX-X

    private final RocksDBStore store;
    private final MeterRegistry metrics;
    private final KafkaTemplate<String, String> kafkaTemplate;  // FIX-X
    private final HealthChecker healthChecker;
    private final String kafkaBrokers;
    private final String cdcTopic;
    private volatile boolean stopped = false;

    /** Spring-managed constructor (used with @KafkaListener). */
    public CdcKafkaConsumer(RocksDBStore store, MeterRegistry metrics,
                            KafkaTemplate<String, String> kafkaTemplate) {
        this.store = store;
        this.metrics = metrics;
        this.kafkaTemplate = kafkaTemplate;
        this.healthChecker = null;
        this.kafkaBrokers = null;
        this.cdcTopic = null;
    }

    /** Standalone constructor (used by BitmapFilterApplication main). */
    public CdcKafkaConsumer(String kafkaBrokers, String cdcTopic,
                            RocksDBStore store, HealthChecker healthChecker) {
        this.kafkaBrokers = kafkaBrokers;
        this.cdcTopic = cdcTopic;
        this.store = store;
        this.healthChecker = healthChecker;
        this.metrics = null;
        this.kafkaTemplate = null;
    }

    /**
     * Standalone polling loop (invoked from BitmapFilterApplication as a daemon thread).
     */
    public void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bitmap-filter-cdc");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(cdcTopic));
            while (!stopped) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record.value(), record.offset());
                }
            }
        } catch (Exception e) {
            if (!stopped) {
                LOG.error("CDC consumer loop error: {}", e.getMessage(), e);
            }
        }
        LOG.info("CdcKafkaConsumer stopped");
    }

    private void processRecord(String message, long offset) {
        try {
            JsonNode root = MAPPER.readTree(message);
            String op = root.path("op").asText();

            switch (op) {
                case "c", "u" -> handleCreateOrUpdate(root.path("after"));
                case "d" -> handleDelete(root.path("before"));
                default -> LOG.debug("Skipping CDC op: {}", op);
            }

            store.setLastCdcOffset(offset);
            if (healthChecker != null) {
                healthChecker.onCdcEvent(offset);
            }
        } catch (Exception e) {
            LOG.error("CDC event processing failed at offset {}: {}", offset, e.getMessage(), e);
        }
    }

    public void stop() {
        stopped = true;
        LOG.info("CdcKafkaConsumer stopping");
    }

    @KafkaListener(
        topics = "${cdc.topic:dbserver.public.image_merchant_bitmaps}",
        groupId = "${cdc.consumer-group:bitmap-filter-cdc}",
        concurrency = "2"
    )
    public void onCdcEvent(
            @Payload String message,
            @Header(KafkaHeaders.OFFSET) long offset) {
        try {
            JsonNode root = MAPPER.readTree(message);
            String op = root.path("op").asText();

            switch (op) {
                case "c", "u" -> handleCreateOrUpdate(root.path("after"));
                case "d" -> handleDelete(root.path("before"));
                default -> LOG.debug("Skipping CDC op: {}", op);
            }

            store.setLastCdcOffset(offset);
            if (metrics != null) metrics.counter("bitmap.cdc.events", "op", op).increment();

        } catch (Exception e) {
            LOG.error("CDC event processing failed at offset {}: {}", offset, e.getMessage(), e);
            if (metrics != null) metrics.counter("bitmap.cdc.errors").increment();

            // FIX-X: 发送到死信队列, 保证消息不丢失
            try {
                if (kafkaTemplate != null) {
                    kafkaTemplate.send(DLQ_TOPIC, String.valueOf(offset), message);
                    if (metrics != null) metrics.counter("bitmap.cdc.dlq.sent").increment();
                    LOG.warn("CDC event sent to DLQ: offset={}", offset);
                }
            } catch (Exception dlqErr) {
                LOG.error("DLQ send also failed at offset {}: {}", offset, dlqErr.getMessage());
                if (metrics != null) metrics.counter("bitmap.cdc.dlq.send_failed").increment();
            }
        }
    }

    private void handleCreateOrUpdate(JsonNode after) throws Exception {
        String imagePk = after.path("image_pk").asText().trim();
        byte[] key = hexDecode(imagePk);

        // Bitmap 字段 (pg_roaringbitmap 的 Base64 编码)
        String bitmapB64 = after.path("bitmap").asText();
        byte[] bitmapBytes = java.util.Base64.getDecoder().decode(bitmapB64);

        // 路由到对应 Column Family
        boolean isEvergreen = after.path("is_evergreen").asBoolean(false);
        String cf = isEvergreen ? "cf_evergreen" : "cf_rolling";
        store.put(cf, key, bitmapBytes);

        // 如果 is_evergreen 变更, 清理另一个 CF
        if (isEvergreen) {
            store.delete("cf_rolling", key);
        }
    }

    private void handleDelete(JsonNode before) throws Exception {
        String imagePk = before.path("image_pk").asText().trim();
        byte[] key = hexDecode(imagePk);
        store.delete("cf_rolling", key);
        store.delete("cf_evergreen", key);
    }

    /**
     * 32 hex chars → 16 bytes
     */
    private static byte[] hexDecode(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return bytes;
    }
}
