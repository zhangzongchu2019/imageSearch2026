package com.szwego.imagesearch.bitmap.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.szwego.imagesearch.bitmap.store.RocksDBStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * CDC 事件消费: Debezium PG → Kafka → RocksDB
 *
 * <p>事件格式: Debezium change event (op: c/u/d)
 * <p>路由: is_evergreen → cf_evergreen / cf_rolling
 *
 * <p>对齐系统设计 v1.2 §6.3
 */
@Component
public class CdcKafkaConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(CdcKafkaConsumer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RocksDBStore store;
    private final MeterRegistry metrics;

    public CdcKafkaConsumer(RocksDBStore store, MeterRegistry metrics) {
        this.store = store;
        this.metrics = metrics;
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
            metrics.counter("bitmap.cdc.events", "op", op).increment();

        } catch (Exception e) {
            LOG.error("CDC event processing failed at offset {}: {}", offset, e.getMessage(), e);
            metrics.counter("bitmap.cdc.errors").increment();
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
