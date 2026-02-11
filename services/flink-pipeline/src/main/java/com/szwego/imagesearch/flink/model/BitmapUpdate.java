package com.szwego.imagesearch.flink.model;

import org.roaringbitmap.RoaringBitmap;

import java.io.Serializable;

/**
 * Bitmap 聚合结果 — 窗口内合并后的增量
 */
public class BitmapUpdate implements Serializable {
    private static final long serialVersionUID = 1L;

    private String imagePk;
    private RoaringBitmap additions;
    private RoaringBitmap removals;

    public BitmapUpdate() {
        this.additions = new RoaringBitmap();
        this.removals = new RoaringBitmap();
    }

    public String getImagePk() { return imagePk; }
    public void setImagePk(String imagePk) { this.imagePk = imagePk; }
    public RoaringBitmap getAdditions() { return additions; }
    public void setAdditions(RoaringBitmap additions) { this.additions = additions; }
    public RoaringBitmap getRemovals() { return removals; }
    public void setRemovals(RoaringBitmap removals) { this.removals = removals; }
}
