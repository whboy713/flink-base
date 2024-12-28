package com.cowcow.flink.chapter05.req2.bean;

public class AdsClickLog {
    private long userId;
    private long adsId;
    private String province;
    private String city;
    private Long timestamp;

    public AdsClickLog() {
    }

    public AdsClickLog(long userId, long adsId, String province, String city, Long timestamp) {
        this.userId = userId;
        this.adsId = adsId;
        this.province = province;
        this.city = city;
        this.timestamp = timestamp;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getAdsId() {
        return adsId;
    }

    public void setAdsId(long adsId) {
        this.adsId = adsId;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}