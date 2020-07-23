package com.pusidun.flink.streaming.urltopn;

import com.pusidun.utils.DateUtils;

/**
 * nginx生产日志
 * 127.0.0.1 1595492263309 /k
 * 52.30.46.215 2020-07-15T05:48:42+00:00 /u3d/appsflyer/pushdata 127.0.0.1:8080 200 "http-kit/2.0"
 */
public class ApacheLogEvent {

    private String ip;
    private Long eventTime;
    private String url;
    private String host;
    private String httpStatus;
    private String httpUserAgent;


    public ApacheLogEvent(){}

/*    public ApacheLogEvent(String eventTime, String url) {
        this.eventTime = DateUtils.parseNginxTime(eventTime);
        this.url = url;
    }*/

    public ApacheLogEvent(Long eventTime, String url) {
        this.eventTime = eventTime;
        this.url = url;
    }
    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus(String httpStatus) {
        this.httpStatus = httpStatus;
    }

    public String getHttpUserAgent() {
        return httpUserAgent;
    }

    public void setHttpUserAgent(String httpUserAgent) {
        this.httpUserAgent = httpUserAgent;
    }
}
