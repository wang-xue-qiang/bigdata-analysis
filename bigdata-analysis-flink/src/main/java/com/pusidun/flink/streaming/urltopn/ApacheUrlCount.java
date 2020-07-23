package com.pusidun.flink.streaming.urltopn;

/**
 * 开窗聚合后结果
 */
public class ApacheUrlCount {

    private String url;
    private  Long windowEnd;
    private Long count;

    public ApacheUrlCount() { }

    public ApacheUrlCount(String url, Long windowEnd, Long count) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}

