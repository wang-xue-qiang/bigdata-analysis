package com.pusidun.dw.bean;

/**
 * 抽奖
 */
public class AppLuckDraw {

    private String  adRewardKind; //抽奖奖励 钻石，能量，皮肤，黄金瞄准，飞碟
    private Integer  adReward; // 奖品

    public String getAdRewardKind() {
        return adRewardKind;
    }

    public void setAdRewardKind(String adRewardKind) {
        this.adRewardKind = adRewardKind;
    }

    public Integer getAdReward() {
        return adReward;
    }

    public void setAdReward(Integer adReward) {
        this.adReward = adReward;
    }
}
