package com.pusidun.flink.streaming.brandlike;

import java.io.Serializable;

/**
 * 用户浏览商品
 */
public class ScanProductEntity implements Serializable {
    private String uid;   //用户Id
    private int useType;  // 终端类型：0、pc端；1、移动端；2、小程序端
    private String brand; //品牌
    private int productId;//商品id
    private long eventTime;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public int getUseType() {
        return useType;
    }

    public void setUseType(int useType) {
        this.useType = useType;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public int getProductId() {
        return productId;
    }

    public void setProductId(int productId) {
        this.productId = productId;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }
}
