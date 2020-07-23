package com.pusidun.flink.streaming.brandlike;

/**
 * 用户喜欢的品牌统计
 */
public class BrandLikeEntity {

    private String groupField;
    private String brand;
    private long count;

    public String getGroupField() {
        return groupField;
    }

    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return brand+"\t"+count;
    }
}
