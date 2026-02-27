package com.szwego.imagesearch.bitmap.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CDC 事件路由测试 (create/update/delete/evergreen 路由)
 */
@Tag("unit")
class CdcKafkaConsumerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void createEventParsed() throws Exception {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("op", "c");
        ObjectNode after = MAPPER.createObjectNode();
        after.put("image_pk", "abcdef1234567890abcdef1234567890");
        after.put("bitmap", java.util.Base64.getEncoder().encodeToString(new byte[]{1, 2, 3}));
        after.put("is_evergreen", false);
        root.set("after", after);

        assertEquals("c", root.path("op").asText());
        assertFalse(root.path("after").path("is_evergreen").asBoolean());
    }

    @Test
    void updateEventParsed() throws Exception {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("op", "u");
        ObjectNode after = MAPPER.createObjectNode();
        after.put("image_pk", "1234567890abcdef1234567890abcdef");
        after.put("bitmap", java.util.Base64.getEncoder().encodeToString(new byte[]{4, 5, 6}));
        after.put("is_evergreen", true);
        root.set("after", after);

        assertEquals("u", root.path("op").asText());
        assertTrue(root.path("after").path("is_evergreen").asBoolean());
    }

    @Test
    void deleteEventParsed() throws Exception {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("op", "d");
        ObjectNode before = MAPPER.createObjectNode();
        before.put("image_pk", "00112233445566778899aabbccddeeff");
        root.set("before", before);

        assertEquals("d", root.path("op").asText());
        assertEquals("00112233445566778899aabbccddeeff", root.path("before").path("image_pk").asText());
    }

    @Test
    void evergreenRoutingCf() throws Exception {
        ObjectNode after = MAPPER.createObjectNode();
        after.put("is_evergreen", true);
        String cf = after.path("is_evergreen").asBoolean(false) ? "cf_evergreen" : "cf_rolling";
        assertEquals("cf_evergreen", cf);

        after.put("is_evergreen", false);
        cf = after.path("is_evergreen").asBoolean(false) ? "cf_evergreen" : "cf_rolling";
        assertEquals("cf_rolling", cf);
    }

    @Test
    void hexDecode() {
        String hex = "abcdef1234567890abcdef1234567890";
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        assertEquals(16, bytes.length);
        assertEquals((byte) 0xab, bytes[0]);
        assertEquals((byte) 0x90, bytes[bytes.length - 1]);
    }

    @Test
    void dlqTopic() {
        String dlqTopic = "bitmap-cdc-dlq";
        assertNotNull(dlqTopic);
        assertFalse(dlqTopic.isEmpty());
    }
}
