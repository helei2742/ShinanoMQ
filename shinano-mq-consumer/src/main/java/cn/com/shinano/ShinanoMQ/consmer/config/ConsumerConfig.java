package cn.com.shinano.ShinanoMQ.consmer.config;

import java.io.File;

public class ConsumerConfig {
    /**
     * 消费者心跳间隔
     */
    public static final int IDLE_TIME_SECONDS = 30;

    /**
     * 提前拉消息的数据限制
     */
    public static final int PRE_PULL_MESSAGE_COUNT = 100;

    /**
     * 提前拉消息的间隔
     */
    public static final long PRE_PULL_MESSAGE_INTERVAL = 5000;

    /**
     * consumer发成功ack的批大小
     */
    public static final int CONSUME_ACK_BATCH_SIZE = 100;

    /**
     * 发送consume offset的间隔
     */
    public static final long PUSH_CONSUME_OFFSET_INTERVAL = 1000*10;

    /**
     * 本地提交的地址
     */
    public static final String LOCAL_COMMIT_LOCATION = System.getProperty("user.dir") + File.separator + "consumer" + File.separator + "consumelog.log";


    /**
     * 提交consume offset 的重试时间
     */
    public static final int[] COMMIT_REMOTE_RETRY_COUNT = {1, 5, 25, 60, 1800, 3600, 7200, 18000, 86400};
}
