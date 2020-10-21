package com.pusidun.dw.bean;

/**
 * 签到
 */
public class AppSign {
    private String action; // 0直接领取；1看广告领取
    private Integer signDay; // 签到天数
    private String signReward; // 签到奖励
    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Integer getSignDay() {
        return signDay;
    }

    public void setSignDay(Integer signDay) {
        this.signDay = signDay;
    }

    public String getSignReward() {
        return signReward;
    }

    public void setSignReward(String signReward) {
        this.signReward = signReward;
    }
}
