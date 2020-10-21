package com.pusidun.dw.bean;

/**
 * 金币产出与消耗
 */
public class AppCoin {
    private String action;	//add产出；reduce消耗
    private String condition;	//场景
    private String ball;	//球皮肤
    private Integer num;	 //数量
    private Integer allNum;	//总数

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getBall() {
        return ball;
    }

    public void setBall(String ball) {
        this.ball = ball;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    public Integer getAllNum() {
        return allNum;
    }

    public void setAllNum(Integer allNum) {
        this.allNum = allNum;
    }
}
