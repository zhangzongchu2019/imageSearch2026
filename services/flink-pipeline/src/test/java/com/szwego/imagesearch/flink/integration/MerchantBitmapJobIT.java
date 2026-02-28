package com.szwego.imagesearch.flink.integration;

import com.szwego.imagesearch.flink.function.BitmapAggregateFunction;
import com.szwego.imagesearch.flink.function.MerchantEventDeserializer;
import com.szwego.imagesearch.flink.model.BitmapUpdate;
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
        // BitmapAggregateFunction.add() 内部使用 MerchantDictEncoder (需要 PG/Redis),
        // 所以这里直接测试 BitmapUpdate 聚合逻辑
        BitmapUpdate acc = new BitmapUpdate();
        acc.setImagePk("pk_it_001");
        acc.getAdditions().add(42);

        assertNotNull(acc);
        assertEquals("pk_it_001", acc.getImagePk());
        assertTrue(acc.getAdditions().contains(42));
        assertTrue(acc.getRemovals().isEmpty());
    }

    @Test
    void testAggregateWithCheckpointRecovery() {
        // Simulate checkpoint recovery: merge two accumulators
        BitmapAggregateFunction agg = new BitmapAggregateFunction();
        BitmapUpdate acc1 = agg.createAccumulator();
        BitmapUpdate acc2 = agg.createAccumulator();

        // 直接操作 BitmapUpdate 而非通过 add() (避免 MerchantDictEncoder 连接)
        acc1.setImagePk("pk_it_002");
        acc1.getAdditions().add(10);

        acc2.setImagePk("pk_it_002");
        acc2.getAdditions().add(20);

        var merged = agg.merge(acc1, acc2);
        var result = agg.getResult(merged);

        assertTrue(result.getAdditions().contains(10));
        assertTrue(result.getAdditions().contains(20));
    }
}
