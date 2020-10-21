package com.pusidun.dw.bean;

/**
 * 广告
 */
public class AppAd {
    private String action;	//0插屏；1视频
    private Integer result;	//视频播放0失败；1成功
    private String condition;	//场景

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Integer getResult() {
        return result;
    }

    public void setResult(Integer result) {
        this.result = result;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }
}
