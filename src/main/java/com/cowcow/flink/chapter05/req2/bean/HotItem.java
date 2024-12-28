package com.cowcow.flink.chapter05.req2.bean;

public class HotItem {

    private Long itemId;
    private Long count;
    private Long windowEndTime;

    public HotItem() {
    }

    public HotItem(Long itemId, Long count, Long windowEndTime) {
        this.itemId = itemId;
        this.count = count;
        this.windowEndTime = windowEndTime;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getWindowEndTime() {
        return windowEndTime;
    }

    public void setWindowEndTime(Long windowEndTime) {
        this.windowEndTime = windowEndTime;
    }

    @Override
    public String toString() {
        return "HotItem{" +
                "itemId=" + itemId +
                ", count=" + count +
                ", windowEndTime=" + windowEndTime +
                '}';
    }
}
