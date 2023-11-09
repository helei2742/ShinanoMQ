package cn.com.shinano.ShinanoMQ.base.dto;

public enum AckStatus {
    SUCCESS(1),
    FAIL(-1),
    WAITE(0);

    int value;
    AckStatus(int i) {
        value = i;
    }

    public int getValue() {
        return value;
    }
}