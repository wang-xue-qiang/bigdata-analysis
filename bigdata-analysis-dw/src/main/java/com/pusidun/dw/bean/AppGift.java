package com.pusidun.dw.bean;

/**
 * 礼物盒子
 */
public class AppGift {
    private String action;	//0看广告；1购买；2免费
    private String prop;	//道具种类
    private Integer level;	//关卡
    private String condition;	//场景

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getProp() {
        return prop;
    }

    public void setProp(String prop) {
        this.prop = prop;
    }

    public Integer getLevel() {
        return level;
    }

    public void setLevel(Integer level) {
        this.level = level;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }
}
