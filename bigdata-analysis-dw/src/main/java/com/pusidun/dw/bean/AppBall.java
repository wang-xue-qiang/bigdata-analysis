package com.pusidun.dw.bean;

/**
 * 球皮肤
 */
public class AppBall {
    private String action;	//0钻石购买；1金币购买；3广告试用
    private Integer ball;	//球皮肤
    private Integer num;	    //消耗的数量
    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Integer getBall() {
        return ball;
    }

    public void setBall(Integer ball) {
        this.ball = ball;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }
}
