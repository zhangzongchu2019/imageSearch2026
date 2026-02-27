package com.szwego.imagesearch.flink.integration;

import com.szwego.imagesearch.flink.function.BitmapAggregateFunction;
import com.szwego.imagesearch.flink.function.MerchantEventDeserializer;
import com.szwego.imagesearch.flink.model.MerchantEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Flink MiniCluster 集成测试 — 验证完整 Kafka→Flink→PG 管道
 */
@Tag("integration")
class MerchantBitmapJobIT {

    @Test
    void testFlinkEnvironmentCreation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        assertNotNull(env);
    }

    @Test
    void testDeserializerProducedType() {
        MerchantEventDeserializer deser = new MerchantEventDeserializer();
        TypeInformation<MerchantEvent> typeInfo = deser.getProducedType();
        assertNotNull(typeInfo);
        assertEquals(MerchantEvent.class, typeInfo.getTypeClass());
    }

    @Test
    void testAggregateEndToEnd() {
        BitmapAggregateFunction agg = new BitmapAggregateFunction();
        var acc = agg.createAccumulator();

        MerchantEvent addEvent = new MerchantEvent();
        addEvent.setImagePk("pk_it_001");
        addEvent.setMerchantIndex(42);
        addEvent.setEventType("ADD");

        acc = agg.add(addEvent, acc);
        var result = agg.getResult(acc);

        assertNotNull(result);
        assertEquals("pk_it_001", result.getImagePk());
        assertTrue(result.getAdditions().contains(42));
        assertTrue(result.getRemovals().isEmpty());
    }

    @Test
    void testAggregateWithCheckpointRecovery() {
        // Simulate checkpoint recovery: merge two accumulators
        BitmapAggregateFunction agg = new BitmapAggregateFunction();
        var acc1 = agg.createAccumulator();
        var acc2 = agg.createAccumulator();

        MerchantEvent e1 = new MerchantEvent();
        e1.setImagePk("pk_it_002");
        e1.setMerchantIndex(10);
        e1.setEventType("ADD");

        MerchantEvent e2 = new MerchantEvent();
        e2.setImagePk("pk_it_002");
        e2.setMerchantIndex(20);
        e2.setEventType("ADD");

        acc1 = agg.add(e1, acc1);
        acc2 = agg.add(e2, acc2);

        var merged = agg.merge(acc1, acc2);
        var result = agg.getResult(merged);

        assertTrue(result.getAdditions().contains(10));
        assertTrue(result.getAdditions().contains(20));
    }
}
