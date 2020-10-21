package com.pusidun.dw.bean;

/**
 * 活动
 */
public class AppActivity {
    private String action;	//新技能块；冰爽一夏；超大地图等
    private Integer level;	//关卡
    private Integer result;	//0失败；1成功

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Integer getLevel() {
        return level;
    }

    public void setLevel(Integer level) {
        this.level = level;
    }

    public Integer getResult() {
        return result;
    }

    public void setResult(Integer result) {
        this.result = result;
    }
}
