package com.pusidun.flink.streaming.uv;

/**
 * 定义实体
 */
public class UniqueVisitorEntity {

    private String uid;
    private Long  eventTime;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }
}
