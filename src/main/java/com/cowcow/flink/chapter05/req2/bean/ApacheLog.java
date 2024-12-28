package com.cowcow.flink.chapter05.req2.bean;

public class ApacheLog {

    private String ip;
    private long eventTime;
    private String method;
    private String url;

    public ApacheLog(String ip, long eventTime, String method, String url) {
        this.ip = ip;
        this.eventTime = eventTime;
        this.method = method;
        this.url = url;
    }

    public ApacheLog() {
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "ApacheLog{" +
                "ip='" + ip + '\'' +
                ", eventTime=" + eventTime +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
