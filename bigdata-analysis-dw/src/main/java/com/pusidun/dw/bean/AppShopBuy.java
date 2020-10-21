package com.pusidun.dw.bean;

/**
 * 内购
 */
public class AppShopBuy {
    private String action;	//0购买钻石；1购买礼包；3移除广告
    private String buyKind;//购买的种类
    private Integer diamond;	//钻石的数量
    private String propList;	//道具的种类和数量
    private Integer energy;	//能量
    private String ballList;	//球皮肤

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

    public Integer getDiamond() {
        return diamond;
    }

    public void setDiamond(Integer diamond) {
        this.diamond = diamond;
    }

    public String getPropList() {
        return propList;
    }

    public void setPropList(String propList) {
        this.propList = propList;
    }

    public Integer getEnergy() {
        return energy;
    }

    public void setEnergy(Integer energy) {
        this.energy = energy;
    }

    public String getBallList() {
        return ballList;
    }

    public void setBallList(String ballList) {
        this.ballList = ballList;
    }
}
