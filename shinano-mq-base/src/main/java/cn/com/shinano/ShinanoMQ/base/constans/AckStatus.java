package cn.com.shinano.ShinanoMQ.base.constans;

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