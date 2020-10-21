package com.pusidun.dw.bean;


/**
 * 后台进程
 */
public class AppBackground {
    private String action;
    private String active_source;  //	1=upgrade,2=download(下载),3=plugin_upgrade

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getActive_source() {
        return active_source;
    }

    public void setActive_source(String active_source) {
        this.active_source = active_source;
    }
}
