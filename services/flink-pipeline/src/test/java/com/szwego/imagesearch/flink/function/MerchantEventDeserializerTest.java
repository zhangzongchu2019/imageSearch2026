package com.szwego.imagesearch.flink.function;

import com.szwego.imagesearch.flink.model.MerchantEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class MerchantEventDeserializerTest {

    private MerchantEventDeserializer deserializer;

    @BeforeEach
    void setUp() {
        deserializer = new MerchantEventDeserializer();
    }

    @Test
    void validJson() throws IOException {
        String json = """
            {"event_type": "ADD", "image_pk": "abcdef1234567890abcdef1234567890",
             "merchant_id": "m001", "source": "update-image",
             "timestamp": 1700000000000, "trace_id": "t001"}
            """;
        MerchantEvent event = deserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));
        assertNotNull(event);
        assertEquals("ADD", event.getEventType());
        assertEquals("abcdef1234567890abcdef1234567890", event.getImagePk());
        assertEquals("m001", event.getMerchantId());
        assertEquals("update-image", event.getSource());
        assertEquals(1700000000000L, event.getTimestamp());
    }

    @Test
    void invalidReturnsNull() throws IOException {
        byte[] invalid = "not json at all".getBytes(StandardCharsets.UTF_8);
        MerchantEvent event = deserializer.deserialize(invalid);
        assertNull(event);
    }

    @Test
    void nullHandled() throws IOException {
        byte[] empty = "{}".getBytes(StandardCharsets.UTF_8);
        MerchantEvent event = deserializer.deserialize(empty);
        assertNotNull(event);
        // All fields should be null/default
        assertNull(event.getEventType());
    }

    @Test
    void unknownFieldsIgnored() throws IOException {
        String json = """
            {"event_type": "ADD", "image_pk": "pk0001",
             "merchant_id": "m001", "extra_field": "should be ignored",
             "nested": {"foo": "bar"}}
            """;
        MerchantEvent event = deserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));
        assertNotNull(event);
        assertEquals("ADD", event.getEventType());
    }

    @Test
    void snakeCase() throws IOException {
        String json = """
            {"event_type": "REMOVE", "image_pk": "pk0002",
             "merchant_id": "m002", "trace_id": "trace123"}
            """;
        MerchantEvent event = deserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));
        assertNotNull(event);
        assertEquals("REMOVE", event.getEventType());
        assertEquals("trace123", event.getTraceId());
    }

    @Test
    void isEndOfStreamReturnsFalse() {
        assertFalse(deserializer.isEndOfStream(new MerchantEvent()));
    }

    @Test
    void producedTypeNotNull() {
        assertNotNull(deserializer.getProducedType());
    }
}
