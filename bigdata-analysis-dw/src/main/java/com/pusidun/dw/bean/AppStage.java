package com.pusidun.dw.bean;

/**
 * 主线关卡
 */
public class AppStage {
    private Integer level; //当前关卡
    private Integer maxLevel;//最大关卡
    private Integer result;	//0失败 ；1成功；2退出

    public Integer getLevel() {
        return level;
    }

    public void setLevel(Integer level) {
        this.level = level;
    }

    public Integer getMaxLevel() {
        return maxLevel;
    }

    public void setMaxLevel(Integer maxLevel) {
        this.maxLevel = maxLevel;
    }

    public Integer getResult() {
        return result;
    }

    public void setResult(Integer result) {
        this.result = result;
    }
}
