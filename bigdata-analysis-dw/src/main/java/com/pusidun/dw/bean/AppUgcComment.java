package com.pusidun.dw.bean;

/**
 * UGC评价
 */
public class AppUgcComment {
    private String  action;	//0新关卡1热门
    private Integer ugcId;	//关卡唯一标识
    private Integer result;	//1好评；2差评；0没有评价

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Integer getUgcId() {
        return ugcId;
    }

    public void setUgcId(Integer ugcId) {
        this.ugcId = ugcId;
    }

    public Integer getResult() {
        return result;
    }

    public void setResult(Integer result) {
        this.result = result;
    }
}
