package com.szwego.imagesearch.flink.function;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.szwego.imagesearch.flink.model.MerchantEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Kafka 商家事件 JSON 反序列化
 */
public class MerchantEventDeserializer implements DeserializationSchema<MerchantEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(MerchantEventDeserializer.class);
    private transient ObjectMapper mapper;

    private ObjectMapper getMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
        return mapper;
    }

    @Override
    public MerchantEvent deserialize(byte[] message) throws IOException {
        try {
            return getMapper().readValue(message, MerchantEvent.class);
        } catch (Exception e) {
            LOG.error("Failed to deserialize merchant event: {}", new String(message), e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(MerchantEvent event) {
        return false;
    }

    @Override
    public TypeInformation<MerchantEvent> getProducedType() {
        return TypeInformation.of(new TypeHint<MerchantEvent>() {});
    }
}
