package com.szwego.imagesearch.flink.model;

import java.io.Serializable;

/**
 * Kafka 商家关联事件 (对齐系统设计 v1.2 §3.5.1)
 */
public class MerchantEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String eventType;     // "ADD" | "REMOVE"
    private String imagePk;       // hex(sha256(uri)[0:16])
    private String merchantId;    // 原始 string 商家 ID
    private String source;        // "update-image" | "bind-merchant"
    private long timestamp;
    private String traceId;

    public MerchantEvent() {}

    public MerchantEvent(String eventType, String imagePk, String merchantId,
                         String source, long timestamp, String traceId) {
        this.eventType = eventType;
        this.imagePk = imagePk;
        this.merchantId = merchantId;
        this.source = source;
        this.timestamp = timestamp;
        this.traceId = traceId;
    }

    // Getters & Setters
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public String getImagePk() { return imagePk; }
    public void setImagePk(String imagePk) { this.imagePk = imagePk; }
    public String getMerchantId() { return merchantId; }
    public void setMerchantId(String merchantId) { this.merchantId = merchantId; }
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }
}
