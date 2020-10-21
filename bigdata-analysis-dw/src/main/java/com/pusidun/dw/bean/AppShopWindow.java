package com.pusidun.dw.bean;

/**
 * 自定义弹窗
 */
public class AppShopWindow {
    private String action;
    private String buyKind; //购买种类
    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getBuyKind() {
        return buyKind;
    }

    public void setBuyKind(String buyKind) {
        this.buyKind = buyKind;
    }
}
