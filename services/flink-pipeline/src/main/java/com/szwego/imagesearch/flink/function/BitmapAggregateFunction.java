package com.szwego.imagesearch.flink.function;

import com.szwego.imagesearch.flink.model.BitmapUpdate;
import com.szwego.imagesearch.flink.model.MerchantEvent;
import com.szwego.imagesearch.flink.util.MerchantDictEncoder;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 窗口内按 image_pk 聚合商家关联事件到 Roaring Bitmap
 *
 * <p>ADD: rb_or (幂等追加)
 * <p>REMOVE: rb_andnot (v1.2+ 预留, 商家解绑)
 */
public class BitmapAggregateFunction
        implements AggregateFunction<MerchantEvent, BitmapUpdate, BitmapUpdate> {

    private static final Logger LOG = LoggerFactory.getLogger(BitmapAggregateFunction.class);
    private transient MerchantDictEncoder encoder;

    @Override
    public BitmapUpdate createAccumulator() {
        return new BitmapUpdate();
    }

    @Override
    public BitmapUpdate add(MerchantEvent event, BitmapUpdate acc) {
        if (encoder == null) {
            encoder = MerchantDictEncoder.getInstance();
        }

        // 设置 image_pk
        if (acc.getImagePk() == null) {
            acc.setImagePk(event.getImagePk());
        }

        // 字典编码: merchant_id (string) → bitmap_index (uint32)
        int bitmapIndex = encoder.encode(event.getMerchantId());

        switch (event.getEventType()) {
            case "ADD":
                acc.getAdditions().add(bitmapIndex);
                break;

            case "REMOVE":
                // v1.2+ 商家解绑预留
                acc.getRemovals().add(bitmapIndex);
                LOG.info("REMOVE event received for image_pk={}, merchant={}",
                    event.getImagePk(), event.getMerchantId());
                break;

            default:
                LOG.warn("Unknown event type: {}", event.getEventType());
        }

        return acc;
    }

    @Override
    public BitmapUpdate getResult(BitmapUpdate acc) {
        return acc;
    }

    @Override
    public BitmapUpdate merge(BitmapUpdate a, BitmapUpdate b) {
        a.getAdditions().or(b.getAdditions());
        a.getRemovals().or(b.getRemovals());
        return a;
    }
}
