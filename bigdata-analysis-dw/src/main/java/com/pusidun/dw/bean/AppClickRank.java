package com.pusidun.dw.bean;

/**
 * 点击排行榜
 */
public class AppClickRank {
    private String action; //0全榜；1活动榜；2现实排行榜
    public String getAction() {
        return action;
    }
    public void setAction(String action) {
        this.action = action;
    }
}
