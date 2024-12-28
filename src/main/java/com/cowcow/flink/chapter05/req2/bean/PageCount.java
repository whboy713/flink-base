package com.cowcow.flink.chapter05.req2.bean;

public class PageCount {

    private String url;
    private Long count;
    private Long windowEnd;

    public PageCount() {
    }

    public PageCount(String url, Long count, Long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowEnd = windowEnd;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "PageCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
